package com.amazonaws.services.kinesis.stormspout.twitterstream;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.stormspout.wordcount.CustomCredentialsProviderChain;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterStreamProducer {

	static Client hosebirdClient;
	static String streamName = "TwitterStreamingApp";

	/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
	static BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

	public static void main(String[] args) {
		AmazonKinesis kinesisClient = new AmazonKinesisClient(new CustomCredentialsProviderChain());
		setupHosebirdClient();
		hosebirdClient.connect();
		System.out.println("connected to twitter hose");
		while (!hosebirdClient.isDone()) {
			try {
				String tweetText = msgQueue.take();
				System.out.println("tweet ----->" + tweetText);
				// Add Data to a Stream
				PutRecordRequest putRecordRequest = new PutRecordRequest();
				putRecordRequest.setStreamName(streamName);
				putRecordRequest.setData(ByteBuffer.wrap(tweetText.getBytes()));
				putRecordRequest.setPartitionKey(String.format("partitionKey-%s", "tweets"));
				PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);

				System.out.println(String.format("Seq No: %s - %s", putRecordResult.getSequenceNumber(), tweetText));

			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
    }

	public static void setupHosebirdClient() {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
		List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("twitter", "api");
		endpoint.followings(followings);
		endpoint.trackTerms(terms);
		
		String consumerKey = System.getenv("CONSUMER_KEY");
		String consumerSecret = System.getenv("CONSUMER_SECRET");
		String accessToken = System.getenv("ACCESS_TOKEN");
		String accessTokenSecret = System.getenv("ACCESS_TOKEN_SECRET");

		Authentication hosebirdAuth = new OAuth1(consumerKey,consumerSecret,accessToken,accessTokenSecret);

        ClientBuilder builder = new ClientBuilder()
	        .name("Hosebird-Client-01")		// optional: mainly for the logs
	        .hosts(hosebirdHosts)
	        .authentication(hosebirdAuth)
	        .endpoint(endpoint)
	        .processor(new StringDelimitedProcessor(msgQueue));

		hosebirdClient = builder.build();
	}

}
