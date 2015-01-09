/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.stormspout.samples;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Map;
import java.util.StringTokenizer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.stormspout.utils.BigDataUtil;


import com.amazonaws.services.kinesis.stormspout.utils.RedisClient;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;




public class WordSplitBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 177788290277634253L;
    private static final Logger LOG = LoggerFactory.getLogger(WordSplitBolt.class);
    private transient CharsetDecoder decoder;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        decoder = Charset.forName("UTF-8").newDecoder();
    }
    
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String partitionKey = (String)input.getValueByField(SampleKinesisRecordScheme.FIELD_PARTITION_KEY);
        String sequenceNumber = (String)input.getValueByField(SampleKinesisRecordScheme.FIELD_SEQUENCE_NUMBER);
        byte[] payload = (byte[])input.getValueByField(SampleKinesisRecordScheme.FIELD_RECORD_DATA);
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        String data = null; 
        try {
            data = decoder.decode(buffer).toString();
        } catch (CharacterCodingException e) {
            LOG.error("Exception when decoding record ", e);
        }
        /*LOG.info("SampleBolt got record: partitionKey=" + partitionKey + ", " + " sequenceNumber=" + sequenceNumber
                + ", data=" + data);*/
        data = data.replace("review/summary", "");
        data = data.replace("review/text", "");
        data =  Jsoup.parse(data).text();
        data = BigDataUtil.getInstance().removeStopWords(data);
        LOG.info("Stop words removed for record seq number " + sequenceNumber);
       // RedisClient.getInstance().updateWordCountToRedis(data);
        
    	StringTokenizer st = new StringTokenizer(data," ");
		while(st.hasMoreTokens()) {
			String word = st.nextToken();
			collector.emit(new Values(word));
		}
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("word"));
    }

}
