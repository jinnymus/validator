package com.hack.validator.liba2;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import net.openhft.hashing.LongHashFunction;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;


public class Duplicator {

    private static String redisHost;// = "localhost";
    private static Integer redisPort;// = 6379;

    private static String prometheusHost;
    private static Integer prometheusPort;

    //the jedis connection pool..
    private static JedisPool pool;
    private static Counter messagesCounter;
    private static Counter duplicateCounter;
    private static Gauge msgProcessingDuration;
    private static Gauge duplicationCheckingDuration;
    private static Gauge writingDuration;
    private static PushGateway gateway;
    private static CollectorRegistry registry;

    public Duplicator() {

        registry = new CollectorRegistry();

        messagesCounter = Counter.build().name("hack_message_counts").help("Count of messages").create().register(registry);
        duplicateCounter = Counter.build().name("hack_duplicate_counts").help("Count of duplicate messages").create().register(registry);
        msgProcessingDuration = Gauge.build().name("hack_msg_process_dur").help("Message processing duration").register(registry);
        duplicationCheckingDuration = Gauge.build().name("hack_msg_dup_check_dur").help("Message duplication checking duration").register(registry);
        writingDuration = Gauge.build().name("hack_msg_wr_dur").help("Message writing duration").register(registry);

        try (InputStream is = getClass().getClassLoader().getResourceAsStream("property.properties")) {
            if (is == null) {
                throw new RuntimeException("Cannot find property file.");
            }
            Properties properties = new Properties();
            properties.load(is);

            redisHost = properties.getProperty("redis.host");
            redisPort = Integer.valueOf(properties.getProperty("redis.port"));

            prometheusHost = properties.getProperty("pushgateway.host");
            prometheusPort = Integer.valueOf(properties.getProperty("pushgateway.port"));

            gateway = new PushGateway( prometheusHost + ":" + prometheusPort);


        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        //configure our pool connection

        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(30);
        pool = new JedisPool(poolConfig, redisHost, redisPort, 3000);
    }


    public void push() {
        try {
            gateway.pushAdd(registry, "app");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void incmessagesCounter() {
        messagesCounter.inc();
    }

    public void incduplicateCounter() {
        duplicateCounter.inc();
    }

    public boolean isDuplicated(String message) {
        Gauge.Timer msgProcessingTimer = msgProcessingDuration.startTimer();
        long startTime = System.nanoTime();
        long hash = getHash(message);
        long endTime = System.nanoTime();

        System.out.println("Time spend to hash calculating: " + getMicroSec(endTime - startTime));

        Jedis resource = pool.getResource();

        Gauge.Timer duplicationCheckingTimer = duplicationCheckingDuration.startTimer();
        startTime = System.nanoTime();
        boolean bdIsEmpty = bdIsEmpty(hash, resource);
        endTime = System.nanoTime();
        duplicationCheckingTimer.close();

        System.out.println("Check duplicate in db: " + getMicroSec(endTime - startTime));

        if (bdIsEmpty) {
            Gauge.Timer wringTimer = writingDuration.startTimer();
            startTime = System.nanoTime();
            writeInDb(hash, resource);
            endTime = System.nanoTime();
            wringTimer.close();
            System.out.println("Writing in db time: " + getMicroSec(endTime - startTime));

            resource.close();
            msgProcessingTimer.close();
            return true;
        }

        msgProcessingTimer.close();
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