package com.amazonaws.services.kinesis.stormspout.twitterstream;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SentimentScoringBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 42L;
    private static final Logger LOGGER = Logger.getLogger(SentimentScoringBolt.class);

    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	
        LOGGER.info("Scoring tweet");
        Long id = tuple.getLong(tuple.fieldIndex("tweet_id"));
        String text = tuple.getString(tuple.fieldIndex("tweet_text"));
        Float pos = tuple.getFloat(tuple.fieldIndex("pos_score"));
        Float neg = tuple.getFloat(tuple.fieldIndex("neg_score"));
        String score = pos > neg ? "positive" : "negative";
        LOGGER.info(String.format("tweet id->" + id + " tweetText->" + text + " positive score->" + pos + " negative score->" + neg + " effective sentiment->" + score));
        collector.emit(new Values(id, text, pos, neg, score));
        
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)  {
        declarer.declare(new Fields("tweet_id","tweet_text","pos_score","neg_score","score"));
    }

}
