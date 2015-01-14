package com.amazonaws.services.kinesis.stormspout.twitterstream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.stormspout.InitialPositionInStream;
import com.amazonaws.services.kinesis.stormspout.KinesisSpout;
import com.amazonaws.services.kinesis.stormspout.KinesisSpoutConfig;
import com.amazonaws.services.kinesis.stormspout.wordcount.ConfigKeys;
import com.amazonaws.services.kinesis.stormspout.wordcount.CustomCredentialsProviderChain;
import com.amazonaws.services.kinesis.stormspout.wordcount.SampleKinesisRecordScheme;
import com.amazonaws.services.kinesis.stormspout.wordcount.WordCountTopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class SentimentAnalysisTopology {

	private static final Logger LOGGER = Logger.getLogger(SentimentAnalysisTopology.class);
	private static String topologyName = "SentimentAnalysisTopology";
    private static String streamName = "TwitterStreamingApp";
    private static InitialPositionInStream initialPositionInStream = InitialPositionInStream.LATEST;
    private static int recordRetryLimit = 3;
    private static Regions region = Regions.US_EAST_1;
    private static String zookeeperEndpoint;
    private static String zookeeperPrefix;
    
    private static void printUsageAndExit() {
        System.out.println("Usage: " + WordCountTopology.class.getName() + " <propertiesFile> <LocalMode or RemoteMode>");
        System.exit(-1);
    }
    
    private static void configure(String propertiesFile) throws IOException {
        FileInputStream inputStream = new FileInputStream(propertiesFile);
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } finally {
            inputStream.close();
        }

        String topologyNameOverride = properties.getProperty(ConfigKeys.TOPOLOGY_NAME_KEY);
        if (topologyNameOverride != null) {
            topologyName = topologyNameOverride;
        }
        LOGGER.info("Using topology name " + topologyName);

        String streamNameOverride = properties.getProperty(ConfigKeys.STREAM_NAME_KEY);
        if (streamNameOverride != null) {
            streamName = streamNameOverride;
        }
        LOGGER.info("Using stream name " + streamName);

        String initialPositionOverride = properties.getProperty(ConfigKeys.INITIAL_POSITION_IN_STREAM_KEY);
        if (initialPositionOverride != null) {
             initialPositionInStream = InitialPositionInStream.valueOf(initialPositionOverride);
        }
        LOGGER.info("Using initial position " + initialPositionInStream.toString() + " (if a checkpoint is not found).");
        
        String recordRetryLimitOverride = properties.getProperty(ConfigKeys.RECORD_RETRY_LIMIT);
        if (recordRetryLimitOverride != null) {
            recordRetryLimit = Integer.parseInt(recordRetryLimitOverride.trim());
        }
        LOGGER.info("Using recordRetryLimit " + recordRetryLimit);

        String regionOverride = properties.getProperty(ConfigKeys.REGION_KEY);
        if (regionOverride != null) {
            region = Regions.fromName(regionOverride);
        }
        LOGGER.info("Using region " + region.getName());

        String zookeeperEndpointOverride = properties.getProperty(ConfigKeys.ZOOKEEPER_ENDPOINT_KEY);
        if (zookeeperEndpointOverride != null) {
            zookeeperEndpoint = zookeeperEndpointOverride;
        }
        LOGGER.info("Using zookeeper endpoint " + zookeeperEndpoint);

        String zookeeperPrefixOverride = properties.getProperty(ConfigKeys.ZOOKEEPER_PREFIX_KEY);
        if (zookeeperPrefixOverride != null) {            
            zookeeperPrefix = zookeeperPrefixOverride;
        }
        LOGGER.info("Using zookeeper prefix " + zookeeperPrefix);

    }

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		String propertiesFile = null;
        
        if (args.length != 1) {
            printUsageAndExit();
        } else {
            propertiesFile = args[0];
            
        }
        
        configure(propertiesFile);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sentiment-analysis", createConfig(true),createTopology());
		Thread.sleep(60000);
		cluster.shutdown();
		
	}

	private static StormTopology createTopology() {
		final KinesisSpoutConfig config =
                new KinesisSpoutConfig(streamName, zookeeperEndpoint).withZookeeperPrefix(zookeeperPrefix)
                        .withKinesisRecordScheme(new SampleKinesisRecordScheme())
                        .withInitialPositionInStream(initialPositionInStream)
                        .withRecordRetryLimit(recordRetryLimit)
                        .withRegion(region);

        final KinesisSpout spout = new KinesisSpout(config, new CustomCredentialsProviderChain(), new ClientConfiguration());
        TopologyBuilder topology = new TopologyBuilder();
        LOGGER.info("Using Kinesis stream: " + config.getStreamName());

        // Using number of shards as the parallelism hint for the spout.
        topology.setSpout("kinesis_spout", spout, 1).setDebug(false);

		topology.setBolt("text_filter", new TextFilterBolt(), 2).fieldsGrouping("kinesis_spout", new Fields(SampleKinesisRecordScheme.FIELD_PARTITION_KEY));
				//.shuffleGrouping("kinesis_spout");
		

		topology.setBolt("stemming", new StemmingBolt(), 2).shuffleGrouping(
				"text_filter");

		topology.setBolt("positive", new PositiveSentimentBolt(), 2)
				.shuffleGrouping("stemming");
		topology.setBolt("negative", new NegativeSentimentBolt(), 2)
				.shuffleGrouping("stemming");

		topology.setBolt("join", new JoinSentimentsBolt(), 2)
				.fieldsGrouping("positive", new Fields("tweet_id"))
				.fieldsGrouping("negative", new Fields("tweet_id"));

		topology.setBolt("score", new SentimentScoringBolt(), 2)
				.shuffleGrouping("join");

		return topology.createTopology();
	}

	private static Config createConfig(boolean local) {
		int workers = 1;
		Config conf = new Config();
		conf.setDebug(false);
		if (local)
			conf.setMaxTaskParallelism(workers);
		else
			conf.setNumWorkers(workers);
		conf.setFallBackOnJavaSerialization(true);
        
		return conf;
	}

}
