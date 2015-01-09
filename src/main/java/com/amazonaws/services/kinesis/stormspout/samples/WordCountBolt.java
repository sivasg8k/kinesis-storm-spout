package com.amazonaws.services.kinesis.stormspout.samples;

import com.amazonaws.services.kinesis.stormspout.utils.RedisClient;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WordCountBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String set = "movieReviews";

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = input.getString(0);
		RedisClient.getInstance().updateWordCountToRedis(set,word);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
