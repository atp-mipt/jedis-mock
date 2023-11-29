package com.github.fppt.jedismock.datastructures.streams;

public class StreamErrors {
    public static final String ID_OVERFLOW_ERROR = "ERR The stream has exhausted the last possible ID, unable to add more items";
    public static final String TOP_ERROR = "ERR The ID specified in XADD is equal or smaller than the target stream top item";
    public static final String INVALID_ID_ERROR = "ERR Invalid stream ID specified as stream command argument";
    public static final String ZERO_ERROR = "ERR The ID specified in XADD must be greater than 0-0";
}
