package com.amazonaws.services.kinesis.stormspout.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import com.amazonaws.auth.AWSCredentials;

public class BigDataUtil {
	
	private BigDataUtil() {
		
	}
	
	private static final BigDataUtil bigDataUtil = new BigDataUtil();
	private static Map<String,String> stopWords = new HashMap<String,String>();
	private static Set<String> positiveWords = new TreeSet<>();
	private static Set<String> negativeWords = new TreeSet<>();
	
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
			
			resourceName = "positive_words.txt";
		    loader = Thread.currentThread().getContextClassLoader();
			
			resourceStream = loader.getResourceAsStream(resourceName);
		    in = new BufferedReader(new InputStreamReader(resourceStream));
		    
		    while((line = in.readLine()) != null) {
		    	positiveWords.add(line.trim());
			}
		    
		    resourceName = "negative_words.txt";
		    loader = Thread.currentThread().getContextClassLoader();
			
			resourceStream = loader.getResourceAsStream(resourceName);
		    in = new BufferedReader(new InputStreamReader(resourceStream));
		    
		    while((line = in.readLine()) != null) {
		    	negativeWords.add(line.trim());
			}
		    
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static BigDataUtil getInstance() {
		return bigDataUtil;
	}
	
	public Set<String> getPositiveWords() {
		return positiveWords;
	}
	
	public Set<String> getNegativeWords() {
		return negativeWords;
	}
	
	public static void main(String args[]) {
		System.out.println("siva%$\"^".replace("%$^",""));
	}
	
	
	public String removeStopWords(String input) {
		
		StringBuffer output = new StringBuffer();
		
		StringTokenizer st = new StringTokenizer(input," ");
		
		while(st.hasMoreTokens()) {
			String token = st.nextToken().trim();
			token = token.replace("\"","");
			token = token.replace("/","");
			token = token.replace(":","");
			token = token.replace(",","");
			if(null == stopWords.get(token.toLowerCase())) {
				output.append(token);
				output.append(" ");
			}
		}
		return output.toString();
	}
}
