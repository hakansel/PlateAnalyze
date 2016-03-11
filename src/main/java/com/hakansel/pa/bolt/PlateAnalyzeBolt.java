package com.hakansel.pa.bolt;

import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * This bolt tracks the 'Plate' to detect the sequence in red list and black list within 5 min interval.
 * if it detects it, emit 'true' to other bolt
 *
 * */
public class PlateAnalyzeBolt extends BaseRichBolt
{
	private static final long	serialVersionUID	= 1L;

	//	private static final Long	LOG_INTERVAL		= (long)(5 * 60 * 1000);	/*5 min interval*/

	private Integer				blackCounter;
	private Integer				redCounter;
	private List<String>		redList;
	private List<String>		blackList;
	private long				lastLogTime;

	private Long				logInterval;

	private OutputCollector		outputCollector;

	public PlateAnalyzeBolt(List<String> red, List<String> black)
	{
		this.redList = red;
		this.blackList = black;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.outputCollector = collector;

		this.blackCounter = 0;
		this.redCounter = 0;
		this.lastLogTime = System.currentTimeMillis();
	}

	/*
	 * capture at every sequence(2 red list and 3 black list) in 5 min interval
	 * */
	@Override
	public void execute(Tuple tuple)
	{
		String plate = (String)tuple.getValueByField("Plate");

		if (this.redList.contains(plate))
		{
			this.redCounter++;
		}
		if (this.blackList.contains(plate))
		{
			this.blackCounter++;
		}
		long now = System.currentTimeMillis();
		long logPeriod = now - lastLogTime;

		if ((this.redCounter == 2) && (this.blackCounter == 3) && (logPeriod <= this.logInterval))
		{
			this.outputCollector.emit(new Values(true));
			this.redCounter = 0;
			this.blackCounter = 0;
		}
		if (logPeriod > this.logInterval)
		{
			this.lastLogTime = now;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
	{
		outputFieldsDeclarer.declare(new Fields("Event"));
	}

	public void setLogInterval(Long logInterval)
	{
		this.logInterval = logInterval;
	}

}
