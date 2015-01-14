package com.amazonaws.services.kinesis.stormspout.twitterstream;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;


@DynamoDBTable(tableName="TWEET_SENTIMENTS")
public class TweetSentiment {
	
	private Long tweetId;
	private String tweetText;
	private Double posScore;
	private Double negScore;
	private String effectiveSentiment;
	
	@DynamoDBHashKey(attributeName="TWEET_ID")
	public Long getTweetId() {
		return tweetId;
	}
	
	
	public void setTweetId(Long tweetId) {
		this.tweetId = tweetId;
	}
	
	@DynamoDBAttribute(attributeName="TWEET_TEXT")
	public String getTweetText() {
		return tweetText;
	}
	
	
	public void setTweetText(String tweetText) {
		this.tweetText = tweetText;
	}
	
	@DynamoDBAttribute(attributeName="TWEET_POS")
	public Double getPosScore() {
		return posScore;
	}
	
	
	public void setPosScore(Double posScore) {
		this.posScore = posScore;
	}
	
	@DynamoDBAttribute(attributeName="TWEET_NEG")
	public Double getNegScore() {
		return negScore;
	}
	
	
	public void setNegScore(Double negScore) {
		this.negScore = negScore;
	}
	
	@DynamoDBAttribute(attributeName="EFF_SCORE")
	public String getEffectiveSentiment() {
		return effectiveSentiment;
	}
	
	
	public void setEffectiveSentiment(String effectiveSentiment) {
		this.effectiveSentiment = effectiveSentiment;
	}
	
	

}
