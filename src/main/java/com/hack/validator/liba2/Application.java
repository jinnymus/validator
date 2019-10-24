package com.hack.validator.liba2;

import redis.clients.jedis.Jedis;

public class Application {

    public static void main(String[] args) {
        Jedis jedis = new Jedis("localhost", 6379);
        jedis.connect();
    }
}
