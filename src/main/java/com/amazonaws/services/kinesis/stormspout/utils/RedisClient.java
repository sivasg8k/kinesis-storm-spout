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
	
	public void updateWordCountToRedis(String set,String word) {
		
		Double count = null;
		
		if(null != word && !"".equalsIgnoreCase(word)) {
			word = word.trim();
			count = getScore(set, word);
			LOG.info("count before parsing the word " + word + " is " + count);
			if(null != count) {
				count++;
			} else {
				count = new Double(1);
			}
			LOG.info("word--->" + word + " count---->" + count);
			jedis.zadd(set,count.doubleValue(),word);
		}
	}
	
	private Double getScore(String setName,String key) {
		return jedis.zscore(setName, key);
	}
	
	public static void main(String args[]) {
		RedisClient rd = new RedisClient();
		System.out.println(rd.getScore("8kmiles", "Siva"));
	}

}
