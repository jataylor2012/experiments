package experiment.testing;

import static org.junit.Assert.*;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.utils.Utils;
import experiment.utils.EmbeddedZookeeper;
import experiment.utils.kafka.EmbeddedKafka;
import experiment.utils.kafka.SimpleKafkaDataIn;
import experiment.utils.storm.SingletonCounter;
import experiment.utils.storm.TopologyStarter;
import experiment.utils.storm.VerboseCollectorBolt;

public class TestDataFlow {

	private Logger logger = Logger.getLogger(getClass());
	private EmbeddedZookeeper zookeeper;
	private EmbeddedKafka kafka;
	private final String TOPIC = "data";
	private final String TOPOLOGY_LABEL = "testing";
	private final String BROKER_ID = "1";
	private final int KAFKA_PORT = 9999;
	private final int ZOOKEEPER_PORT = 21819;
	private final int MAX_CONNECTIONS = 5000;
	private final int TICK_TIME = 2000;
	private final int GENERATE_COUNT = 1000;
	private SimpleKafkaDataIn in;
	private TopologyStarter starter;

	@Before
	public void setUp() throws Exception {
		//Start a local zookeeper for Kafka and storing offsets.
		zookeeper = new EmbeddedZookeeper(ZOOKEEPER_PORT, MAX_CONNECTIONS, TICK_TIME);
		zookeeper.start();
		
		//Start a local Kafka for putting dummy data on.
		kafka = new EmbeddedKafka(KAFKA_PORT, zookeeper.getUrl(), BROKER_ID);
		kafka.start();
		in = new SimpleKafkaDataIn("localhost:"+KAFKA_PORT, TOPIC);
		
		//Start a local Storm topology that reads from Kafka.
		starter = new TopologyStarter(false, 2, TOPOLOGY_LABEL);
		starter.getBuilder().setSpout("getFromKafka", getKafkaSpout());
		VerboseCollectorBolt bolt = new VerboseCollectorBolt(GENERATE_COUNT);
		starter.getBuilder().setBolt("sysoutData", bolt).shuffleGrouping("getFromKafka");
		starter.startLocal();
	}
	
	private KafkaSpout getKafkaSpout() {
		ZkHosts hosts = new ZkHosts(zookeeper.getUrl());
		SpoutConfig config = new SpoutConfig(hosts, TOPIC, "/kafkaspout", BROKER_ID);
		return new KafkaSpout(config);
	}

	@After
	public void tearDown() throws Exception {
		kafka.stop();
		zookeeper.stop();
		starter.shutdown();
	}

	@Test
	public void testKafkaStart() {
		logger.info("Waiting for Kafka, Zookeeper & Storm to fully start...");
		Utils.sleep(5000);
		assertTrue("Kafka has not started.", kafka.isReady());
		logger.info("-------------------------------->Kafka has started");
		for(int i=0; i < GENERATE_COUNT; i++) {
			in.submit("Key " + i, "This is message " + i + " that was generated.");
		}
		in.shutdown();
		logger.info("-------------------------------->Messages sent, sleeping");
		while(SingletonCounter.INSTANCE.get()!=GENERATE_COUNT) {
			Utils.sleep(1000);
		}
		assertTrue("Messages missing from Kafka count.", in.getCount()==GENERATE_COUNT);
		assertTrue("Message count is " + SingletonCounter.INSTANCE.get(), SingletonCounter.INSTANCE.get()==GENERATE_COUNT);
	}

}
