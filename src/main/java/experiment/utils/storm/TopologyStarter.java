package experiment.utils.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TopologyStarter {

	private TopologyBuilder builder;
	private Config conf;
	private String label;
	private LocalCluster cluster;

	public TopologyStarter(boolean isDebug, int numberOfWorkers, String label) {
		this.label = label;
		builder = new TopologyBuilder();
		conf = new Config();
		conf.setDebug(isDebug);
		conf.setNumWorkers(numberOfWorkers);
	}
	
	public void startLocal() {
		cluster = new LocalCluster();
		cluster.submitTopology(label, conf, builder.createTopology());
	}

	public TopologyBuilder getBuilder() {
		return builder;
	}
	
	public void shutdown() {
		if(cluster!=null) {
			cluster.killTopology(label);
			cluster.shutdown();
		}
	}
}
