package com.amazonaws.services.kinesis.stormspout.twitterstream;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.stormspout.wordcount.SampleKinesisRecordScheme;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TextFilterBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(TextFilterBolt.class);
	private transient CharsetDecoder decoder;
	
	@Override
    public void prepare(Map stormConf, TopologyContext context) {
        decoder = Charset.forName("UTF-8").newDecoder();
    }

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		LOG.debug("removing ugly characters");
		
		String sequenceNumber = (String)input.getValueByField(SampleKinesisRecordScheme.FIELD_SEQUENCE_NUMBER);
        byte[] payload = (byte[])input.getValueByField(SampleKinesisRecordScheme.FIELD_RECORD_DATA);
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        String data = null; 
        try {
            data = decoder.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            LOG.error("Exception when decoding record ", e);
        }
		
		String tweetData[] = data.split(":");
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
