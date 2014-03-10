package experiment.testing;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import experiment.utils.EmbeddedZookeeper;

public class TestEmbeddedZookeeper {

	private EmbeddedZookeeper zookeeper;

	@Before
	public void setUp() throws Exception {
		zookeeper = new EmbeddedZookeeper(21819, 5000, 2000);
		zookeeper.start();
	}

	@After
	public void tearDown() throws Exception {
		zookeeper.stop();
	}

	@Test @Ignore
	public void testZookeeperStarts() {
		assertTrue("The zookeeper has not started.", zookeeper.isReady());
	}

}
