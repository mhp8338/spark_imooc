package com.imooc.bigdata.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Author: Michael PK
 * Redis工具类：建议大家使用单例模式进行封装
 */
public class RedisUtils {

    private static JedisPool jedisPool = null;

    private static final String HOST = "192.168.199.234";
    private static final int PORT = 6379;

    public static synchronized Jedis getJedis(){

        if(null == jedisPool) {
            GenericObjectPoolConfig config = new JedisPoolConfig();
            config.setMaxIdle(10);
            config.setMaxTotal(100);
            config.setMaxWaitMillis(1000);
            config.setTestOnBorrow(true);

            jedisPool = new JedisPool(config, HOST, PORT);
        }

        return jedisPool.getResource();
    }

}
