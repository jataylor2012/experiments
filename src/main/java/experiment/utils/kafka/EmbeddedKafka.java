package experiment.utils.kafka;

import java.io.File;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

import org.apache.log4j.Logger;

public class EmbeddedKafka implements Time{
	
	private Logger logger = Logger.getLogger(getClass());
	private final String EMBEDDED = "./embedded";
	private final String KAFKADIR = "/kafka";
	private int port;
	private String zookeeperUrl;
	private String brokerId;
	private File dataDirectory;
	private boolean isReady = false;
	private KafkaServer server;
	
	public EmbeddedKafka(int port, String zookeeperUrl, String brokerId) {
		this.port = port;
		this.zookeeperUrl = zookeeperUrl;
		this.brokerId = brokerId;
		dataDirectory = new File(EMBEDDED, KAFKADIR);
		if(!dataDirectory.exists()) {
			dataDirectory.mkdir();
		}
	}
	
	public void setDataDirectory(File dir) {
		this.dataDirectory = dir;
	}
	
	public void start() {
		try {
			Properties properties = new Properties();
			properties.put("port", String.valueOf(port));
			properties.put("zookeeper.connect", zookeeperUrl);
			properties.put("broker.id", brokerId);
			properties.put("log.dir", dataDirectory.getAbsolutePath());
			KafkaConfig config = new KafkaConfig(properties);
			server = new KafkaServer(config, this);
			server.startup();
			isReady = true;
		} catch(Exception e) {
			isReady = false;
			logger.error("An error occurred when initialising kafka.",e);
		}
	}
	
	public void stop() {
		if(server!=null) {
			server.shutdown();
			server.awaitShutdown();
		}
	}
	
	public boolean isReady() {
		return isReady;
	}

	@Override
	public long milliseconds() {
		return 0;
	}

	@Override
	public long nanoseconds() {
		return 0;
	}

	@Override
	public void sleep(long arg0) {
		try {
			Thread.sleep(arg0);
		} catch (InterruptedException e) {
			logger.error("An error occurred when calling the embedded sleep.",e);
		}
	}

}
