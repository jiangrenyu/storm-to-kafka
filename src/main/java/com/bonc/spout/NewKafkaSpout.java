package com.bonc.spout;

import java.util.Map;

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.shade.org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

public class NewKafkaSpout extends KafkaSpout {
    public NewKafkaSpout(SpoutConfig spoutConf) {
        super(spoutConf);
    }
    @Override
    public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
		System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, "SpoutClient");
        super.open(conf, context, collector);
    }
}
