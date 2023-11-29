package com.github.fppt.jedismock.datastructures.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;

import static java.lang.Long.compareUnsigned;
import static java.lang.Long.parseUnsignedLong;
import static java.lang.Long.toUnsignedString;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.INVALID_ID_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.ZERO_ERROR;

public class StreamId implements Comparable<StreamId> {
    private long firstPart = 0;
    private long secondPart = 0;

    public long getFirstPart() {
        return firstPart;
    }

    public long getSecondPart() {
        return secondPart;
    }

    /* 0-0 ID for PRIVATE API usage */
    StreamId() {}

    public StreamId(String key) throws WrongStreamKeyException {
        String[] parsed = key.split("-");

        switch (parsed.length) {
            case 2:
                try {
                    secondPart = parseUnsignedLong(parsed[1]);
                } catch (NumberFormatException e) {
                    throw new WrongStreamKeyException(INVALID_ID_ERROR);
                }
            case 1:
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

    public StreamId compareToZero() throws WrongStreamKeyException {
        if (secondPart == 0 && firstPart == 0) {
            throw new WrongStreamKeyException(ZERO_ERROR);
        }

        return this;
    }


    public StreamId(Slice slice) throws WrongStreamKeyException {
        this(slice.toString());
    }

    public void increment() throws WrongStreamKeyException {
        ++secondPart;

        if (compareUnsigned(secondPart, 0) == 0) { // the previous one was 0xFFFFFFFFFFFFFFFF => update the first part
            if (compareUnsigned(firstPart, -1) == 0) {
                /* TODO unsure of exception message */
                throw new WrongStreamKeyException("ERR invalid start ID for the interval");
            }

            ++firstPart;
        }
    }

    public void decrement() throws WrongStreamKeyException {
        --secondPart;

        if (compareUnsigned(secondPart, -1) == 0) { // the previous one was 0x0000000000000000 => update the first part
            if (compareUnsigned(firstPart, 0) == 0) {
                /* TODO unsure of exception message */
                throw new WrongStreamKeyException("ERR invalid end ID for the interval");
            }

            --firstPart;
        }
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
