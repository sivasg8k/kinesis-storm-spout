package com.amazonaws.services.kinesis.stormspout.samples;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;


public class KinesisProducer {
	
	public static void main(String[] args) throws IOException {
		
		String streamName = "KinesisStreamingApp";
		
		
		AmazonKinesis kinesisClient = new AmazonKinesisClient(new CustomCredentialsProviderChain());
		String sequenceNumberOfPreviousRecord = "1234";
		
		String resourceName = "movieReviews.txt"; // could also be a constant
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		
		
		InputStream resourceStream = null;
		//while(true) {
		
			resourceStream = loader.getResourceAsStream(resourceName);
		    BufferedReader in = new BufferedReader(new InputStreamReader(resourceStream));
		    String line = null;
		    int lineNumber = 1;
		    while((line = in.readLine()) != null) {
		    	line = line.trim();
		    	if(!line.startsWith("review/summary") && !line.startsWith("review/text")) {
					  continue;
				  }
		    	PutRecordRequest putRecordRequest = new PutRecordRequest(); 
				putRecordRequest.setStreamName(streamName);
				putRecordRequest.setData(ByteBuffer.wrap(line.getBytes()));
				putRecordRequest.setPartitionKey(String.format( "partitionKey-%d", lineNumber%5 ));
				putRecordRequest.setSequenceNumberForOrdering(String.valueOf(sequenceNumberOfPreviousRecord));  
			    PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);  
			    sequenceNumberOfPreviousRecord = putRecordResult.getSequenceNumber(); 
			    System.out.println("record number " + lineNumber  + " inserted into kinesis");
			    System.out.println("sequence Number is ---->" + sequenceNumberOfPreviousRecord);
			    lineNumber++;
		    }
		    
		    resourceStream.close();
		    in.close();
		    
		    try {
		    	Thread.sleep(5000);
		    } catch(Exception ie) {
		    	
		    }
		//}
	    	
		
		/*
		for (int j = 0; j < 10000000; j++) {  
			
			PutRecordRequest putRecordRequest = new PutRecordRequest(); 
			putRecordRequest.setStreamName(streamName);
			putRecordRequest.setData(ByteBuffer.wrap(String.format("testData-%d",j).getBytes()));
			putRecordRequest.setPartitionKey(String.format( "partitionKey-%d", j%5 )); 
		    putRecordRequest.setSequenceNumberForOrdering(String.valueOf(sequenceNumberOfPreviousRecord));  
		    PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);  
		    sequenceNumberOfPreviousRecord = putRecordResult.getSequenceNumber(); 
		    System.out.println("record number " + j  + " inserted into kinesis");
		}*/


	}

}
