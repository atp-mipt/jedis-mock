package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import static com.github.fppt.jedismock.Utils.convertToDouble;

@RedisCommand("hincrbyfloat")
class HIncrByFloat extends HIncrBy {
    HIncrByFloat(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice hsetValue(Slice key1, Slice key2, Slice value) {
        // BigDecimal numericValue = new BigDecimal(value.toString());
        // Slice foundValue = base().getSlice(key1, key2);
        // if (foundValue != null) {
        // numericValue = numericValue.add(new BigDecimal((new
        // String(foundValue.data()))));
        // }
        // String data =
        // String.valueOf(BigDecimal.valueOf(numericValue.intValue()).compareTo(numericValue)
        // == 0
        // ? numericValue.intValue()
        // : numericValue);

        // Slice res = Slice.create(data);
        // base().putSlice(key1, key2, res, -1L);

        // return Response.bulkString(res);
        // Slice key1 = params().get(0);
        // Slice key2 = params().get(1);
        // Slice value = params().get(2);

        double numericValue = convertToDouble(String.valueOf(value));
        Slice foundValue = base().getSlice(key1, key2);
        // System.out.println("WAS:");
        // System.out.println(foundValue);
        if (foundValue != null) {
            numericValue = convertToDouble(new String(foundValue.data())) + numericValue;
        }
        // System.out.println("NOW:");
        // System.out.println(numericValue + "");
        // System.out.println(String.valueOf(numericValue));
        // System.out.println("==========");
        // byte[] output = ByteBuffer.allocate(8).putDouble(numericValue).array();

        // base().putSlice(key1, key2, Slice.create(String.valueOf(output)), -1L);
        base().putSlice(key1, key2, Slice.create(String.valueOf(numericValue)), -1L);
        return Response.bulkString(Slice.create(String.valueOf(numericValue)));

    }

    @Override
    protected Slice response() {
        // Slice key1 = params().get(0);
        // Slice key2 = params().get(1);
        // Slice value = params().get(2);

        // double numericValue = convertToDouble(String.valueOf(value));
        // Slice foundValue = base().getSlice(key1, key2);
        // if (foundValue != null) {
        // numericValue = convertToDouble(new String(foundValue.data())) + numericValue;
        // }
        // base().putSlice(key1, key2, Slice.create(String.valueOf(numericValue)), -1L);
        // return Response.doubleValue(numericValue);
        Slice key1 = params().get(0);
        Slice key2 = params().get(1);
        Slice value = params().get(2);

        return hsetValue(key1, key2, value);
    }
}
