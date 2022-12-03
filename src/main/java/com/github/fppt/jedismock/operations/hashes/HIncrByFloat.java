package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import static com.github.fppt.jedismock.Utils.convertToDouble;

@RedisCommand("hincrbyfloat")
class HIncrByFloat extends HIncrBy {
    HIncrByFloat(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice hsetValue(Slice key1, Slice key2, Slice value) {
        double numericValue = convertToDouble(String.valueOf(value));
        Slice foundValue = base().getSlice(key1, key2);
        if (foundValue != null) {
            String foundValueStr = String.valueOf(foundValue);
            if (foundValueStr.startsWith(" ") || foundValueStr.endsWith(" ")){
                throw new IllegalArgumentException("ERROR: HINCRBYFLOAT argument is not a float value");
            }
            byte[] bts = foundValue.data();
            for (int i = 0; i < bts.length; ++i){
                if(bts[i] == 0){
                    throw new IllegalArgumentException("ERROR: HINCRBYFLOAT argument is not a float value");
                }
            }
            numericValue = convertToDouble(new String(foundValue.data())) + numericValue;
        }
        Slice res = Slice.create(
            String.format("%f", numericValue)
                  .replaceAll("[0]*$", "")
                  .replaceAll(",", ".")
                  .replaceAll("\\.$", ""));
        base().putSlice(key1, key2, res, -1L);
        return Response.bulkString(res);
    }

    @Override
    protected Slice response() {
        Slice key1 = params().get(0);
        Slice key2 = params().get(1);
        Slice value = params().get(2);

        return hsetValue(key1, key2, value);
    }
}
