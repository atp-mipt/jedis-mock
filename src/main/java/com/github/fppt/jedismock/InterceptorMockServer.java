package com.github.fppt.jedismock;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.server.MockExecutor;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.ServiceOptions;

import java.io.IOException;

public class InterceptorMockServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        //This binds mock redis server to a 39807 port
        RedisServer
                .newRedisServer(39807)
                .setOptions(ServiceOptions.withInterceptor((state, roName, params) -> {
                    if ("config".equalsIgnoreCase(roName)) {
                        //You can can imitate any reply from Redis
                        return Response.bulkString(Slice.create("1"));
                    } else if (
                        "debug".equalsIgnoreCase(roName) 
                        && params.get(0).toString().equalsIgnoreCase("object")
                    ) {
                        //Handling unsopported DEBUG OBJECT command

                        RMDataStructure cls = state.base().getHash(params.get(1));
                        return Response.bulkString(Slice.create(cls.getMeta()));

                    } else if (
                        "object".equalsIgnoreCase(roName) 
                        && params.get(0).toString().equalsIgnoreCase("encoding")
                    ) {
                        // Handling unsopported OBJECT ENCODING command

                        RMDataStructure cls = state.base().getHash(params.get(1));
                        return Response.bulkString(Slice.create(cls.getEncoding()));
                    } else {
                        //Delegate execution to JedisMock which will mock the real Redis behaviour (when it can)
                        return MockExecutor.proceed(state, roName, params);
                    }
                    //NB: you can also delegate to a 'real' Redis in TestContainers here
                }))
                .start();
    }
}
