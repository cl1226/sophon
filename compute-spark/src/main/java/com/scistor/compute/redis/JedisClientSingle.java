package com.scistor.compute.redis;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Set;

public class JedisClientSingle implements JedisClient {
    private String host = "";
    private String port = "";
    private String password = "";

    public JedisClientSingle(){
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxWaitMillis(100000);
        config.setMaxIdle(10);
        config.setMaxTotal(100);
        //JedisPool j = new JedisPool(config,"47.94.196.195",6378);
        if(StringUtils.isNoneBlank(password)){
            jedisPool = new JedisPool(config, host, Integer.valueOf(port),10000, password, 0);
        }else {
            //JedisPool(GenericObjectPoolConfig poolConfig, String host, int port, int timeout, String password, int database)
            jedisPool = new JedisPool(config, host, Integer.valueOf(port), 10000, (String)null, 0);
        }
    }

    public JedisClientSingle(JedisPoolConfig config){
        config.setMaxWaitMillis(100000);
        config.setMaxIdle(10);
        config.setMaxTotal(100);
        if(StringUtils.isNoneBlank(password)){
            jedisPool = new JedisPool(config,host,Integer.valueOf(port),10000,password);
        }else {
            jedisPool = new JedisPool(host,Integer.valueOf(port));
        }
    }

    public JedisClientSingle(String host, int port, String password){
        new JedisClientSingle(host, port, password, 0);
    }

    public JedisClientSingle(String host, int port, String password, int database){
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxWaitMillis(100000);
        config.setMaxIdle(10);
        config.setMaxTotal(100);
        if(StringUtils.isNoneBlank(password)){
            jedisPool = new JedisPool(config, host, Integer.valueOf(port),10000, password, database);
        }else {
            jedisPool = new JedisPool(config, host, Integer.valueOf(port), 10000, (String)null, database);
        }
    }

    private JedisPool jedisPool;

    public String set(String key, String value) {
        Jedis jedis = jedisPool.getResource();
        String result = jedis.set(key, value);
        jedis.close();
        return result;
    }

    public String get(String key) {
        Jedis jedis = jedisPool.getResource();
        String result = jedis.get(key);
        jedis.close();
        return result;
    }

    public Long hset(String key, String item, String value) {
        Jedis jedis = jedisPool.getResource();
        Long result = jedis.hset(key, item, value);
        jedis.close();
        return result;
    }

    public String hget(String key, String item) {
        Jedis jedis = jedisPool.getResource();
        String result = jedis.hget(key, item);
        jedis.close();
        return result;
    }

    public Long incr(String key) {
        Jedis jedis = jedisPool.getResource();
        Long result = jedis.incr(key);
        jedis.close();
        return result;
    }

    public Long decr(String key) {
        Jedis jedis = jedisPool.getResource();
        Long result = jedis.decr(key);
        jedis.close();
        return result;
    }

    public Long expire(String key, int second) {
        Jedis jedis = jedisPool.getResource();
        Long result = jedis.expire(key, second);
        jedis.close();
        return result;
    }

    public Long ttl(String key) {
        Jedis jedis = jedisPool.getResource();
        Long result = jedis.ttl(key);
        jedis.close();
        return result;
    }

    public Long hdel(String key, String item) {
        Jedis jedis = jedisPool.getResource();
        Long result = jedis.hdel(key, item);
        jedis.close();
        return result;
    }

    public Set<String> keys(String pattern){
        Jedis jedis = jedisPool.getResource();
        Set<String> result = jedis.keys(pattern);
        jedis.close();
        return result;
    }

    public Set<String> hkeys(String pattern){
        Jedis jedis = jedisPool.getResource();
        Set<String> result = jedis.hkeys(pattern);
        jedis.close();
        return result;
    }

    @Override
    public boolean exists(String key) {
        Jedis jedis = jedisPool.getResource();
        jedis.close();
        return jedis.exists(key);
    }

    @Override
    public boolean del(String key) {
        Jedis jedis = jedisPool.getResource();
        jedis.del(key);
        jedis.close();
        return false;
    }
}
