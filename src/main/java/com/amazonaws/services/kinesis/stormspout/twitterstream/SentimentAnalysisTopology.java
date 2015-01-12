package com.amazonaws.services.kinesis.stormspout.twitterstream;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.stormspout.InitialPositionInStream;
import com.amazonaws.services.kinesis.stormspout.KinesisSpout;
import com.amazonaws.services.kinesis.stormspout.KinesisSpoutConfig;
import com.amazonaws.services.kinesis.stormspout.wordcount.CustomCredentialsProviderChain;
import com.amazonaws.services.kinesis.stormspout.wordcount.SampleKinesisRecordScheme;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class SentimentAnalysisTopology {

	private static final Logger LOGGER = Logger.getLogger(SentimentAnalysisTopology.class);
	//private static String topologyName = "SentimentAnalysisTopology";
    private static String streamName = "TwitterStreamingApp";
    private static InitialPositionInStream initialPositionInStream = InitialPositionInStream.LATEST;
    private static int recordRetryLimit = 3;
    private static Regions region = Regions.US_EAST_1;
    private static String zookeeperEndpoint;
    private static String zookeeperPrefix;

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();

		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], createConfig(false),
					createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("sentiment-analysis", createConfig(true),
					createTopology());
			Thread.sleep(60000);
			cluster.shutdown();
		}
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
        topology.setSpout("kinesis_spout", spout, 2);

		topology.setBolt("text_filter", new TextFilterBolt(), 4)
				.shuffleGrouping("kinesis_spout");

		topology.setBolt("stemming", new StemmingBolt(), 4).shuffleGrouping(
				"text_filter");

		topology.setBolt("positive", new PositiveSentimentBolt(), 4)
				.shuffleGrouping("stemming");
		topology.setBolt("negative", new NegativeSentimentBolt(), 4)
				.shuffleGrouping("stemming");

		topology.setBolt("join", new JoinSentimentsBolt(), 4)
				.fieldsGrouping("positive", new Fields("tweet_id"))
				.fieldsGrouping("negative", new Fields("tweet_id"));

		topology.setBolt("score", new SentimentScoringBolt(), 4)
				.shuffleGrouping("join");

		return topology.createTopology();
	}

	private static Config createConfig(boolean local) {
		int workers = 4;
		Config conf = new Config();
		conf.setDebug(true);
		if (local)
			conf.setMaxTaskParallelism(workers);
		else
			conf.setNumWorkers(workers);
		return conf;
	}

}
