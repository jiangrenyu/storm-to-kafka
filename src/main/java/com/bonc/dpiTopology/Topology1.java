package com.bonc.dpiTopology;

import java.util.List;
import java.util.Properties;
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.bonc.bolt.DpigetBolt;
import com.bonc.utils.PropertiesConfig;



public class Topology1 {

	private static Logger LOG = Logger.getLogger(Topology1.class);
	// 拓扑名称
	private static String topology_name = null;
	// spout名称
	private static String spout_name = "kafkaSpout";
	// 统计bolt
	private static String countBolt_Name = "countBolt";
	// kafkabolt
	private static String kafkaBolt_Name = "kafkaBolt";

	// kafkaspout的并行度,默认是1
	private static int kafkaSpoutParallelism = 1;
	// countBolt的并行度，默认是1
	private static int countBoltParallelism = 1;
	// kafkabolt的并行度，默认是1
	private static int kafkaBoltParallelism = 1;

	// 配置文件路径
	private static String configPath = "";
	// 处理topic名称
	private static String consumer_topic = "";
	// 写入topic名称
	private static String producer_topic = "";

	// 存放处理进度的zookeeper节点
	private static String brokerZkStr = "";
	// zookeeper路径
	// private static String brokerZkPath = "";
	// 将offset汇报给哪个集群,记录spout的读取进度
	private static String offsetSerevers = "";
	// 写回到的broker节点信息
	private static String brokerLists = "";
	// 端口号
	private static Integer offsetZkPort = 0;
	// 汇报offset信息的路径
	private static String offsetZkRoot = "";
	// 存储该spout id的消费offset信息,譬如以topoName来命名
	private static String offsetZkId = "";
	// 间隔时间
	private static int tickTime = 0;
	// 是否从头开始都kafka的消息
	private static boolean ignoreZkOffsets = false;
	// 配置文件对象
	private static PropertiesConfig propertiesConfig = null;

	public Topology1() {

	}

	public static void main(String args[]) {
		String usage = "Usage:Kafka-Storm-KakfaTopology [-topologyName <topologyName>] -spoutParallelism <spoutParallelism> -countBoltParallelism <countBoltParallelism> -kafkaBoltParallelism <kafkaBoltParallelism> -configPath <configPath> -input_topic <topicName> -output_topic <topicName>";
		if (args.length < 12) {
			System.err.println("启动命令缺少参数");
			System.out.println(usage);
			System.exit(-1);
		}

		for (int i = 0; i < args.length; i++) {
			try {
				switch (args[i]) {
				case "-topologyName":
					topology_name = args[i + 1];
					break;
				case "-spoutParallelism":
					try {
						kafkaSpoutParallelism = Integer.parseInt(args[i + 1]);
					} catch (NumberFormatException e) {
						System.err.println("spoutParallelism必须是数字");
						System.exit(-1);
					}
					break;
				case "-countBoltParallelism":
					try {
						countBoltParallelism = Integer.parseInt(args[i + 1]);
					} catch (NumberFormatException e) {
						System.err.println("countBoltParallelism必须是数字");
						System.exit(-1);
					}
					break;
				case "-kafkaBoltParallelism":
					try {
						kafkaBoltParallelism = Integer.parseInt(args[i + 1]);
					} catch (NumberFormatException e) {
						System.err.println("kafkaBoltParallelism必须是数字");
						System.exit(-1);
					}
					break;
				case "-configPath":
					configPath = args[i + 1];
					break;
				case "-input_topic":
					consumer_topic = args[i + 1];
					break;
				case "-output_topic":
					producer_topic = args[i + 1];
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("缺少启动参数:" + usage);
			}
		}

		try {
			propertiesConfig = new PropertiesConfig(configPath);
		} catch (Exception e) {
			System.err.println("读取配置文件失败");
			System.exit(-1);
		}

		try {
			brokerZkStr = propertiesConfig.getValue("zkhost");
		} catch (Exception e) {
			System.err.println("获取消息kafka节点信息失败");
			System.exit(-1);
		}

		try {
			offsetSerevers = propertiesConfig.getValue("zookeeper.connect");
		} catch (Exception e) {
			System.err.println("获取zookeeeper节点信息失败");
			System.exit(-1);
		}

		try {
			offsetZkPort = Integer.valueOf(propertiesConfig.getValue("zkPort"));
			if (offsetZkPort <= 0) {
				System.err.println("zookeeper端口号不能小于0");
				System.exit(-1);
			}
		} catch (Exception e) {
			System.err.println("获取zookeeper端口号失败");
			System.exit(-1);
		}

		try {
			offsetZkRoot = propertiesConfig.getValue("zookeeper.root");
		} catch (Exception e) {
			System.err.println("获取zookeeper进度信息保存路径失败");
			System.exit(-1);
		}

		try {
			brokerLists = propertiesConfig.getValue("metadata.broker.lists");
		} catch (Exception e) {
			System.err.println("获取写回kafka消息broker节点信息失败");
			System.exit(-1);
		}

		try {
			tickTime = Integer.parseInt(propertiesConfig.getValue("tickTime"));
			if (tickTime < 0) {
				System.err.println("定时触发时间不能小于0");
				System.exit(-1);
			}
		} catch (Exception e) {
			System.err.println("获取定时触发时间失败");
			System.exit(-1);
		}

		try {
			ignoreZkOffsets = Boolean.parseBoolean(propertiesConfig.getValue("forceFromStart"));
		} catch (Exception e) {
			System.err.println("读取forceFromStart失败");
			System.exit(-1);
		}

		// 从zookeeper获取kafka的分区信息
		ZkHosts zkHosts = new ZkHosts(brokerZkStr);

		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, consumer_topic, offsetZkRoot, "spout-id");// 保存zk的信息

		List<String> zkServers = new ArrayList<String>();
		for (String broker : offsetSerevers.split(",")) {
			zkServers.add(broker);
		}

		spoutConfig.zkServers = zkServers;
		spoutConfig.zkPort = offsetZkPort;
		spoutConfig.ignoreZkOffsets = ignoreZkOffsets;

		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		Config config = new Config();
		Properties map = new Properties();
		// 配置Kafka broker地址，写回到这台broker
		map.put("bootstrap.servers", brokerLists);
		// map.put("metadata.broker.list", "192.168.37.160:9092");
		// 设置消息的序列化类
		map.put("serializer.class", "kafka.serializer.StringEncoder");
		map.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		map.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		config.put("kafka.broker.properties", map);
		// 配置KafkaBolt生成的topic

		KafkaSpout kafakSpout = new KafkaSpout(spoutConfig);
		KafkaBolt<String, String> kafkaBolt = new KafkaBolt<String, String>();
		kafkaBolt.withProducerProperties(map).withTopicSelector(new DefaultTopicSelector(producer_topic))
				.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("dpiData1", "dpiData"));
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(spout_name, kafakSpout, kafkaSpoutParallelism);

		builder.setBolt(countBolt_Name, new DpigetBolt(tickTime), countBoltParallelism).shuffleGrouping(spout_name);
		builder.setBolt(kafkaBolt_Name, kafkaBolt, kafkaBoltParallelism).fieldsGrouping(countBolt_Name,
				new Fields("dpiData"));

		if (topology_name != null) {
			config.setDebug(false);

			try {
				StormSubmitter.submitTopology(topology_name, config, builder.createTopology());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", config, builder.createTopology());

		}

	}
}
