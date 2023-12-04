package com.github.fppt.jedismock.datastructures.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;

import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.RANGES_END_ID_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.RANGES_START_ID_ERROR;
import static java.lang.Long.compareUnsigned;
import static java.lang.Long.parseUnsignedLong;
import static java.lang.Long.toUnsignedString;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.INVALID_ID_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.ZERO_ERROR;

public class StreamId implements Comparable<StreamId> {
    private final long firstPart;
    private final long secondPart;

    public long getFirstPart() {
        return firstPart;
    }

    public long getSecondPart() {
        return secondPart;
    }

    /* 0-0 ID for PRIVATE API usage */
    StreamId() {
        this(0, 0);
    }

    /* ID from long numbers for PRIVATE API usage */
    public StreamId(long firstPart, long secondPart) {
        this.firstPart = firstPart;
        this.secondPart = secondPart;
    }

    public StreamId(String key) throws WrongStreamKeyException {
        String[] parsed = key.split("-");

        switch (parsed.length) {
            case 2:
                try {
                    secondPart = parseUnsignedLong(parsed[1]);
                } catch (NumberFormatException e) {
                    throw new WrongStreamKeyException(INVALID_ID_ERROR);
                }

                try {
                    firstPart = parseUnsignedLong(parsed[0]);
                } catch (NumberFormatException e) {
                    throw new WrongStreamKeyException(INVALID_ID_ERROR);
                }

                break;
            case 1:
                secondPart = 0;

                try {
                    firstPart = parseUnsignedLong(parsed[0]);
                } catch (NumberFormatException e) {
                    throw new WrongStreamKeyException(INVALID_ID_ERROR);
                }

                break;
            default:
                throw new WrongStreamKeyException(INVALID_ID_ERROR);
        }
    }

    public StreamId(Slice slice) throws WrongStreamKeyException {
        this(slice.toString());
    }

    public StreamId compareToZero() throws WrongStreamKeyException {
        if (secondPart == 0 && firstPart == 0) {
            throw new WrongStreamKeyException(ZERO_ERROR);
        }

        return this;
    }

    public StreamId increment() throws WrongStreamKeyException {
        long second = secondPart + 1;
        long first = firstPart;

        if (compareUnsigned(second, 0) == 0) { // the previous one was 0xFFFFFFFFFFFFFFFF => update the first part
            if (compareUnsigned(first, -1) == 0) {
                throw new WrongStreamKeyException(RANGES_START_ID_ERROR);
            }

            ++first;
        }

        return new StreamId(first, second);
    }

    public StreamId decrement() throws WrongStreamKeyException {
        long second = secondPart - 1;
        long first = firstPart;

        if (compareUnsigned(second, -1) == 0) { // the previous one was 0x0000000000000000 => update the first part
            if (compareUnsigned(first, 0) == 0) {
                throw new WrongStreamKeyException(RANGES_END_ID_ERROR);
            }

            --first;
        }

        return new StreamId(first, second);
    }

    @Override
    public int compareTo(StreamId other) {
        return compareUnsigned(firstPart, other.firstPart) != 0
                ? compareUnsigned(firstPart, other.firstPart)
                : compareUnsigned(secondPart, other.secondPart);
    }

    public Slice toSlice() {
        return Slice.create(toString());
    }

    @Override
    public String toString() {
        return toUnsignedString(firstPart) + "-" + toUnsignedString(secondPart);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StreamId)) return false;

        StreamId streamId = (StreamId) o;

        if (getFirstPart() != streamId.getFirstPart()) return false;
        return getSecondPart() == streamId.getSecondPart();
    }

    @Override
    public int hashCode() {
        int result = (int) (getFirstPart() ^ (getFirstPart() >>> 32));
        result = 31 * result + (int) (getSecondPart() ^ (getSecondPart() >>> 32));
        return result;
    }
}
