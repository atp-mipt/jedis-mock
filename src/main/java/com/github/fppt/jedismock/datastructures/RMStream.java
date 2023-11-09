package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.linkedMap.LinkedMap;

import static java.lang.Long.compareUnsigned;
import static java.lang.Long.parseUnsignedLong;

public class RMStream implements RMDataStructure {
    private final LinkedMap<Slice, LinkedMap<Slice, Slice>> storedData;
    private long lastFirstId = 0;
    private long lastSecondId = 0;

    private static final String ZERO_ERROR = "ERR The ID specified in XADD must be greater than 0-0";
    private static final String TOP_ERROR = "ERR The ID specified in XADD is equal or smaller than the target stream top item";
    public static final String INVALID_ID_ERROR = "ERR Invalid stream ID specified as stream command argument";


    public RMStream() {
        storedData = new LinkedMap<>();
    }

    public LinkedMap<Slice, LinkedMap<Slice, Slice>> getStoredData() {
        return storedData;
    }

    public void updateLastId(Slice id) {
        String[] parsedKey = id.toString().split("-");
        lastFirstId = parseUnsignedLong(parsedKey[0]);
        lastSecondId = parseUnsignedLong(parsedKey[1]);
    }

    /**
     * TODO explain
     * @return
     * @throws WrongStreamKeyException
     */
    public static void checkParsedKey(String[] parsedKey) throws WrongStreamKeyException {
        if (parsedKey.length != 2) {
            throw new WrongStreamKeyException(INVALID_ID_ERROR);
        }

        try {
            parseUnsignedLong(parsedKey[0]);
            parseUnsignedLong(parsedKey[1]);
        } catch (NumberFormatException e) {
            throw new WrongStreamKeyException(INVALID_ID_ERROR);
        }
    }

    public static Slice checkKey(Slice key) throws WrongStreamKeyException {
        String[] parsedKey = key.toString().split("-");

        checkParsedKey(parsedKey);
        return key;
    }

    // xadd
    public Slice compareWithTopKey(Slice key) throws WrongStreamKeyException { //done
        String[] parsedKey = key.toString().split("-");

        if (storedData.size() == 0) {
            check(
                    parseUnsignedLong(parsedKey[0]),
                    parseUnsignedLong(parsedKey[1]),
                    0, 0,
                    ZERO_ERROR
            );
        } else {
            check(
                    parseUnsignedLong(parsedKey[0]),
                    parseUnsignedLong(parsedKey[1]),
                    lastFirstId, lastSecondId,
                    TOP_ERROR
            );
        }


        return key;
    }

    // xrange
    public static int compare(Slice left, Slice right) {
        String[] parsedLeft = left.toString().split("-");
        String[] parsedRight = right.toString().split("-");

        return compareUnsigned(parseUnsignedLong(parsedLeft[0]), parseUnsignedLong(parsedRight[0])) != 0
                ? compareUnsigned(parseUnsignedLong(parsedLeft[0]), parseUnsignedLong(parsedRight[0]))
                : compareUnsigned(parseUnsignedLong(parsedLeft[1]), parseUnsignedLong(parsedRight[1]));
    }

    private static void check(long keyFirstPart, long keySecondPart, long otherKeyFirstPart, long otherKeySecondPart, String message) throws WrongStreamKeyException {
        if (compareUnsigned(otherKeyFirstPart, keyFirstPart) > 0) {
            throw new WrongStreamKeyException(message);
        }

        if (compareUnsigned(otherKeyFirstPart, keyFirstPart) == 0 && compareUnsigned(otherKeySecondPart, keySecondPart) >= 0) {
            throw new WrongStreamKeyException(message);
        }
    }


    public Slice replaceAsterisk(Slice key) throws WrongStreamKeyException {
        if (key.toString().equals("*")) {
            long secondPart = lastSecondId + 1;
            long firstPart = lastSecondId == -1 ? lastFirstId + 1 : lastFirstId;

            return Slice.create(firstPart + "-" + secondPart);
        }

        String[] parsedKey = key.toString().split("-");

        if (parsedKey.length != 2) {
            return key; /* Wrong key format - will be caught in checkKey */
        }

        try {
            if (compareUnsigned(lastFirstId, parseUnsignedLong(parsedKey[0])) > 0) {
                throw new WrongStreamKeyException(TOP_ERROR);
            }

            if (parsedKey[1].equals("*")) {
                return Slice.create(parsedKey[0] + "-" + (
                        /* First parts are equal => incrementing last second part
                         * 0xFFFFFFFFFFFFFFFF -> 0 which produces an error when comparing to the top key as it has to be
                         * otherwise use 0 as the smallest unsigned long number
                         */
                        compareUnsigned(lastFirstId, parseUnsignedLong(parsedKey[0])) == 0
                                ? lastSecondId + 1
                                : 0
                ));
            }
        } catch (NumberFormatException e) {
            throw new WrongStreamKeyException(INVALID_ID_ERROR);
        }

        return key;
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMStream value is used in the wrong place");
    }

    @Override
    public String getTypeName() {
        return "stream";
    }
}
