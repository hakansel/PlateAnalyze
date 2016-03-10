package com.hakansel.pa.topology;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.hakansel.pa.bolt.EventStoreBolt;
import com.hakansel.pa.bolt.PlateAnalyzeBolt;
import com.hakansel.pa.spout.PlateAnalyzeSpout;

/*
 * Assumptions:
 * 	Plate analyze topology has one spout and two bolt.
 * 	It assumes plate number stream via tcp port 3367.
 * 	It stores event detection to local cassandra using 'plateanalyze' as keyspace name and 'eventtable' as columnfamily 
 * 
 * */
public class PlateAnalyzeTopology
{
	private static final String			LOCAL_TOPOLOGY_NAME		= "PlateAnalyzeTopology";

	private static final Integer		TCP_STREAM_PORT			= 3367;

	private static final String			CASSANDRA_HOST			= "127.0.0.1";
	private static final String			CASSANDRA_KEYSPACE		= "plateanalyze";
	private static final String			CASSANDRA_COLUMNFAMILY	= "eventtable";

	/*
	 * these lists are provided for testing after defining the data.
	 * 
	 * */
	private static final List<String>	RED_LIST				= new ArrayList<String>();
	private static final List<String>	BLACK_LIST				= new ArrayList<String>();

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException,
			AuthorizationException, PlateTopologyExeption
	{
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("PlateAnalyzeSpout", new PlateAnalyzeSpout(TCP_STREAM_PORT));
		topologyBuilder.setBolt("PlateAnalyzeBolt", new PlateAnalyzeBolt(RED_LIST, BLACK_LIST)).shuffleGrouping(
				"PlateAnalyzeSpout");
		topologyBuilder.setBolt("EventStoreBolt",
				new EventStoreBolt(CASSANDRA_HOST, CASSANDRA_KEYSPACE, CASSANDRA_COLUMNFAMILY)).shuffleGrouping(
				"PlateAnalyzeBolt");

		Config config = new Config();
		config.setDebug(true);

		// Cluster mode
		if (args != null && args.length == 1)
		{
			String topologyName = args[0];

			config.setNumWorkers(6);
			config.setNumAckers(6);
			config.setMaxSpoutPending(100);
			config.setMessageTimeoutSecs(20);
			StormSubmitter.submitTopology(topologyName, config, topologyBuilder.createTopology());
		}
		// Local mode
		else if (args != null && args.length == 0)
		{
			final LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(LOCAL_TOPOLOGY_NAME, config, topologyBuilder.createTopology());

			Runtime.getRuntime().addShutdownHook(new Thread()
			{
				@Override
				public void run()
				{
					localCluster.killTopology(LOCAL_TOPOLOGY_NAME);
					localCluster.shutdown();
				}
			});
		}
		else
		{
			throw new PlateTopologyExeption(
					"Usage for local mode:"
							+ "\n\t If you want to use in local mode"
							+ "\n\t > java -jar </path/to/jar>"
							+ "\n"
							+ "Usage for cluster mode:"
							+ "\n\t If you want to use in cluster mode"
							+ "\n\t > storm jar </path/to/jar> com.hakansel.pa.topology.PlateAnalyzeTopology <your-topology-name>");
		}
	}
}
