package com.amazonaws.services.kinesis.stormspout.twitterstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TextFilterBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(TextFilterBolt.class);

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		LOG.debug("removing ugly characters");
        String text = input.getString(0);
        LOG.info("tweet text string------>" + text);
        String tweetData[] = text.split(":");
        Long tweetId = new Long(tweetData[0]);
        String tweetText = tweetData[1];
        tweetText = tweetText.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
        LOG.info("tweetId----->" + tweetId + "tweetText----->" + tweetText);
        collector.emit(new Values(tweetId,tweetText));
    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_id","tweet_text"));
	}

}
