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
		
		Integer count = null;
		
		if(null != word && !"".equalsIgnoreCase(word)) {
			word = word.trim();
			LOG.info("count before parsing the word " + word + " is " + jedis.get(word));
			if(null != jedis.get(word)) {
				try {
					count = new Integer(jedis.get(word));
				} catch(Exception nfe) { 
					//jedis.del(word);
					return;
				}
				
				count++;
			} else {
				count = new Integer(1);
			}
			LOG.info("word--->" + word + " count---->" + count);
			jedis.set(word, String.valueOf(count));
		}
	}
	
	public static void main(String args[]) {
		RedisClient rd = new RedisClient();
	}

}
