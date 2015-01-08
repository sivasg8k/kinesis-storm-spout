package com.amazonaws.services.kinesis.stormspout.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import com.amazonaws.auth.AWSCredentials;

public class BigDataUtil {
	
	private BigDataUtil() {
		
	}
	
	private static final BigDataUtil bigDataUtil = new BigDataUtil();
	private static Map<String,String> stopWords = new HashMap<String,String>();
	
	static {
		
		String resourceName = "stop_words_english.txt";
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		
		InputStream resourceStream = loader.getResourceAsStream(resourceName);
	    BufferedReader in = new BufferedReader(new InputStreamReader(resourceStream));
	    String line = null;
	    
	    try {
			while((line = in.readLine()) != null) {
				stopWords.put(line.trim(), "1");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static BigDataUtil getInstance() {
		return bigDataUtil;
	}
	
	
	public String removeStopWords(String input) {
		
		StringBuffer output = new StringBuffer();
		
		StringTokenizer st = new StringTokenizer(input," ");
		
		while(st.hasMoreTokens()) {
			String token = st.nextToken().trim();
			if(null == stopWords.get(token)) {
				token = token.replace("/", "");
				token = token.replace(":", "");
				token = token.replace(",", "");
				token = token.replace(".", "");
				output.append(token);
				output.append(" ");
			}
		}
		return output.toString();
	}
}
