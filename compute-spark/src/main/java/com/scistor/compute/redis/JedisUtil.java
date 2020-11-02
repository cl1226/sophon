package com.scistor.compute.redis;

import com.scistor.compute.model.remote.RedisConfig;

public class JedisUtil {

    public static JedisClient getJedisClient(RedisConfig redisConfig) {
        try {
            return new JedisClientSingle(redisConfig.getHost(), redisConfig.getPort(), redisConfig.getPassword(), redisConfig.getDatabase());
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

}
