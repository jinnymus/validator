package com.hack.validator.liba2;

import net.openhft.hashing.LongHashFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

public class Duplicator {

    // todo: redis
    // statistic : start, end, latency


    private static String redisHost;// = "localhost";
    private static Integer redisPort;// = 6379;

    private static String prometheusHost;
    private static Integer prometheusPort;

    //the jedis connection pool..
    private static JedisPool pool = null;
//    private static Counter messagesCounter = Counter.build().name("message_counts").help("Count of messages").create();
//    private static Counter duplicateCounter = Counter.build().name("duplicate_counts").help("Count of duplicate messages").create();
//    private static Gauge msgProcessingDuration = Gauge.build().name("msg_process_dur").help("Message processing duration").create();
//    private static Gauge duplicationCheckingDuration = Gauge.build().name("msg_dup_check_dur").help("Message duplication checking duration").create();
//    private static Gauge writingDuration = Gauge.build().name("msg_wr_dur").help("Message writing duration").create();
    //private static PushGateway gateway;

//    private void setEnviroment(Enviroment enviroment) {
//
//    }

    public Duplicator() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("property.properties")) {
            if (is == null) {
                throw new RuntimeException("Cannot find property file.");
            }
            Properties properties = new Properties();
            properties.load(is);

            redisHost = properties.getProperty("redis.host");
            redisPort = Integer.valueOf(properties.getProperty("redis.port"));

            //prometheusHost = properties.getProperty("prometheus.host");
            //prometheusPort = Integer.valueOf(properties.getProperty("prometheus.port"));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        //configure our pool connection
        pool = new JedisPool(redisHost, redisPort);
        //gateway = new PushGateway(prometheusHost + ":" + prometheusPort);
    }


    public boolean isDuplicated(String message) {
//        messagesCounter.inc();
//        Gauge.Timer msgProcessingTimer = msgProcessingDuration.startTimer();
        long startTime = System.nanoTime();
        long hash = getHash(message);
        long endTime = System.nanoTime();

        System.out.println("Time spend to hash calculating: " + getMicroSec(endTime - startTime));

        Jedis resource = pool.getResource();

//        Gauge.Timer duplicationCheckingTimer = duplicationCheckingDuration.startTimer();
        startTime = System.nanoTime();
        boolean bdIsEmpty = bdIsEmpty(hash, resource);
        endTime = System.nanoTime();
//        duplicationCheckingTimer.close();

        System.out.println("Check duplicate in db: " + getMicroSec(endTime - startTime));

        if (bdIsEmpty) {
//            Gauge.Timer wringTimer = writingDuration.startTimer();
            startTime = System.nanoTime();
            writeInDb(hash, resource);
            endTime = System.nanoTime();
//            wringTimer.close();
            System.out.println("Writing in db time: " + getMicroSec(endTime - startTime));

            resource.close();
//            msgProcessingTimer.close();
            return true;
        }

//        msgProcessingTimer.close();
//        duplicateCounter.inc();
        resource.close();
        return false;
    }

    private void writeInDb(long hash, Jedis resource) {
        resource.sadd(String.valueOf(hash), ".");
    }

    private boolean bdIsEmpty(long hash, Jedis resource) {
        Set<String> smembers = resource.smembers(String.valueOf(hash));
        return smembers.isEmpty();
    }

    private String getMicroSec(long diff) {
        return diff / 1000 + " microsecs";
    }

    private long getHash(String message) {
        return LongHashFunction.xx().hashChars(message);
    }
}
