package experiment.testing;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import experiment.utils.EmbeddedZookeeper;
import experiment.utils.kafka.EmbeddedKafka;

public class TestEmbeddedKafka {

	private EmbeddedZookeeper zookeeper;
	private EmbeddedKafka kafka;

	@Before 
	public void setUp() throws Exception {
		zookeeper = new EmbeddedZookeeper(21819, 5000, 2000);
		zookeeper.start();
		kafka = new EmbeddedKafka(9999, zookeeper.getUrl(), "1");
		kafka.start();
	}

	@After
	public void tearDown() throws Exception {
		kafka.stop();
		zookeeper.stop();
	}

	@Test @Ignore
	public void testKafkaStart() {
		assertTrue("Kafka has not started.", kafka.isReady());
	}

}
