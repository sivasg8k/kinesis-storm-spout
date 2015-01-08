package com.amazonaws.services.kinesis.stormspout.utils;

import redis.clients.jedis.Jedis;

public class RedisClient {
	
	private static final String endpoint = "moviereviewsdb.sqjbuo.0001.use1.cache.amazonaws.com";
	
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
		Integer count = null;
		if(null != jedis.get(word)) {
			count = Integer.parseInt(jedis.get(word));
			count++;
		} else {
			count = new Integer(1);
		}
		jedis.set(word.trim(), String.valueOf(count));
	}
	
	public static void main(String args[]) {
		RedisClient rd = new RedisClient();
	}

}
