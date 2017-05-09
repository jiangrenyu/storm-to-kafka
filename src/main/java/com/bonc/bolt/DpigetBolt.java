package com.bonc.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import com.bonc.utils.DateUtils;
import com.bonc.utils.PrintHelper;
import com.google.common.collect.Maps;

public class DpigetBolt implements IRichBolt {
    
	private static Logger LOG = Logger.getLogger(DpigetBolt.class);
	
	Map<String, Integer> countMap = Maps.newConcurrentMap();
	List<String> list = new ArrayList<String>();
	OutputCollector outputollector;
	Integer tickTime;
	static long start;
	static long end;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputollector = collector;

	}

	public DpigetBolt(Integer tickTime) {
		this.tickTime = tickTime;
	}

	public void execute(Tuple input) {
		if (TupleUtils.isTick(input)) {
			end = System.currentTimeMillis();
			
			System.out.println("定时开始时间：" + DateUtils.getTime(end));
			
			//System.out.println("定时周期：" + (end - start) / 1000);
			for (Entry<String, Integer> entry : countMap.entrySet()) {
				System.out.println("数据："+entry.getKey()+"数量："+entry.getValue());
				String outStr = entry.getKey() + "|" + entry.getValue();
				System.out.println("数据+数量的拼接："+outStr);
				PrintHelper.print("发送消息："+outStr);
				this.outputollector.emit(input, new Values(outStr));
			}

			countMap.clear();
			return;
		} else {
			// 开始时间
			start = System.currentTimeMillis();

			System.out.println("正常处理开始时间：" + DateUtils.getTime(start));

			String dpiData = input.getString(0);
			System.out.println("从上游接收到消息：" + dpiData);
			String[] splitData = dpiData.split("\\|");
			if(splitData!=null){
				String CountData = splitData[0] + splitData[2];
				
//				String commOrWarn = splitData[0];
//				String ymd = splitData[36].substring(0, 8);
//				String hour = splitData[36].substring(8, 10);
//				
//				String sendData = commOrWarn+"|"+ymd+"|"+hour;
				System.out.println("切分后的数据：" + CountData);

				if (!countMap.containsKey(CountData)) {
					countMap.put(CountData, 1);
				} else {
					Integer count = countMap.get(CountData)+1;
					countMap.put(CountData, count);
				}
				this.outputollector.ack(input);
			}else{
				System.out.println("不合法数据："+"长度是："+splitData.length);
				LOG.info(dpiData+"异常");
			}
			
		}
	}

	public DpigetBolt() {
		super();
		// TODO Auto-generated constructor stub
	}

	public void cleanup() {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("dpiData"));

	}

	public Map<String, Object> getComponentConfiguration() {
		Config config = new Config();
		config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickTime);
		return config;
	}

}
