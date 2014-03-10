package experiment.testing;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

public class TestTrident {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test @Ignore
	public void test() {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
	               new Values("the cow jumped over the moon"),
	               new Values("the man went to the store and bought some candy"),
	               new Values("four score and seven years ago"),
	               new Values("how many apples can you eat"));
		spout.setCycle(true);
		
		TridentTopology topology = new TridentTopology();        
		TridentState wordCounts =
		     topology.newStream("spout1", spout)
		       .each(new Fields("sentence"), new Split(), new Fields("word"))
		       .groupBy(new Fields("word"))
		       .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))                
		       .parallelismHint(6);
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, topology.build());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();
	}

}
