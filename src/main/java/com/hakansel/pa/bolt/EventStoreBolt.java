package com.hakansel.pa.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/*
 * this bolt checks cassandra env and if needed create new keyspace and columnfamily.
 * if it gets 'true' value, store to cassandra.
 * 
 * */
public class EventStoreBolt extends BaseRichBolt
{
	private static final long	serialVersionUID	= 1L;

	private String				ip;
	private String				keyspace;
	private String				columnfamily;
	private Session				session;

	private Integer				event_no;

	public EventStoreBolt(String ip, String keyspace, String columnfamily)
	{
		this.ip = ip;
		this.keyspace = keyspace;
		this.columnfamily = columnfamily;
	}

	/*
	 * Connecting cassandra
	 * and
	 * Checking existing status of keyspace and columnfamily if not exist create new one.
	 * 
	 * */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.event_no = 1;

		Cluster cluster = Cluster.builder().addContactPoint(ip).build();
		this.session = cluster.connect();
		String checkKeyspaceQuery = "SELECT * FROM system.schema_keyspaces WHERE keyspace_name = '" + this.keyspace
				+ "';";
		ResultSet results = this.session.execute(checkKeyspaceQuery);
		if (results.all().size() == 0)
		{
			this.session.execute("CREATE KEYSPACE " + this.keyspace + " WITH replication "
					+ "= {'class':'SimpleStrategy', 'replication_factor':3};");
		}

		this.session = cluster.connect(this.keyspace);

		String checkColumnFamilyExist = "select columnfamily_name from system.schema_columnfamilies where keyspace_name = '"
				+ this.keyspace + "' and columnfamily_name = '" + this.columnfamily + "';";
		ResultSet execute = this.session.execute(checkColumnFamilyExist);
		if (execute.all().size() == 0)
		{
			this.session.execute("CREATE TABLE " + this.columnfamily + "(event_time bigint PRIMARY KEY,"
					+ "event_no int);");
		}
	}

	/*
	 * Store event with id(event_id) and event store time(event_no) within unix timestamp to cassandra
	 * 
	 * */
	@Override
	public void execute(Tuple input)
	{
		Boolean isEventDetected = (Boolean)input.getValueByField("Event");
		if (isEventDetected == true)
		{
			String storeEventQuery = "INSERT INTO " + this.columnfamily + " (event_time, event_no) VALUES("
					+ System.currentTimeMillis() + "," + this.event_no + ");";
			this.session.execute(storeEventQuery);
			this.event_no++;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		// HHATODO Auto-generated method stub

	}

}
