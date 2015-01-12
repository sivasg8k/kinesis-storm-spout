package com.amazonaws.services.kinesis.stormspout.twitterstream;

import org.apache.log4j.Logger;

import com.amazonaws.services.kinesis.stormspout.utils.BigDataUtil;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StemmingBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;

	private static Logger LOGGER = Logger.getLogger(StemmingBolt.class);

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweet_id","tweet_text"));
    }

    public void execute(Tuple input, BasicOutputCollector collector)
    {
        LOGGER.debug("removing stop words");
        Long tweetId = input.getLong(input.fieldIndex("tweet_id"));
        String tweetText = input.getString(input.fieldIndex("tweet_text"));
        tweetText = BigDataUtil.getInstance().removeStopWords(tweetText);
        collector.emit(new Values(tweetId,tweetText));
    }

}
