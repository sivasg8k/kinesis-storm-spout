package com.amazonaws.services.kinesis.stormspout.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class RedisClient {
	
	private static final String endpoint = "moviereviewsdb.sqjbuo.0001.use1.cache.amazonaws.com";
	private static final Logger LOG = LoggerFactory.getLogger(RedisClient.class);
	
	private static RedisClient redisClient = new RedisClient();
	
	private Jedis jedis;
	
	private RedisClient() {
		
		jedis = new Jedis(endpoint,6379,15000);
		jedis.connect();
		
	}
	
	public static RedisClient getInstance() {
		return redisClient;
	}
	
	public void updateWordCountToRedis(String word) {
		int count = 0;
		word = word.trim();
		if(null != jedis.get(word)) {
			LOG.info("count before parsing" + jedis.get(word));
			count = Integer.parseInt(jedis.get(word));
			count++;
		} else {
			count = 1;
		}
		LOG.info("word--->" + word + " count---->" + count);
		jedis.set(word, String.valueOf(count));
	}
	
	public static void main(String args[]) {
		RedisClient rd = new RedisClient();
	}

}
