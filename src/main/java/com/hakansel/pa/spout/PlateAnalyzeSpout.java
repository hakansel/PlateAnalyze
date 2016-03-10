package com.hakansel.pa.spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
/*
 * spout listens tcp port for the data as string.
 * 
 * */
public class PlateAnalyzeSpout extends BaseRichSpout
{
	private static final long		serialVersionUID	= 1L;

	private SpoutOutputCollector	spoutOutputCollector;
	private Integer					port;
	private Socket					clientSocket;
	private ServerSocket			serverSocket;

	public PlateAnalyzeSpout(Integer port)
	{
		this.port = port;
	}

	@Override
	public void nextTuple()
	{
		try
		{
			this.clientSocket = this.serverSocket.accept();
			BufferedReader inputStream = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
			String readLine = inputStream.readLine();
			this.spoutOutputCollector.emit(new Values(readLine));
		}
		catch (IOException e)
		{
			// HHATODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector)
	{
		this.spoutOutputCollector = spoutOutputCollector;
		try
		{
			this.serverSocket = new ServerSocket(this.port);
		}
		catch (IOException e)
		{
			// HHATODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
	{
		outputFieldsDeclarer.declare(new Fields("Plate"));
	}

	@Override
	public void close()
	{
		try
		{
			this.serverSocket.close();
			this.clientSocket.close();
		}
		catch (IOException e)
		{
			// HHATODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
