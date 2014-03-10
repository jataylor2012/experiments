package experiment.utils;

import java.io.File;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;

public class EmbeddedZookeeper {

	private Logger logger = Logger.getLogger(getClass());
	private final String EMBEDDED = "./embedded";
	private final String ZOODIR = "/zookeeper";
	private int port;
	private int numberOfConnections;
	private int tickTime;
	private File dataDirectory;
	private ZooKeeperServer server;
	private boolean isReady = false;

	public EmbeddedZookeeper(int port, int numberOfConnections, int tickTime) {
		this.port = port;
		this.numberOfConnections = numberOfConnections;
		this.tickTime = tickTime;
		dataDirectory = new File(EMBEDDED, ZOODIR);
		if(!dataDirectory.exists()) {
			dataDirectory.mkdir();
		}
	}

	public void setDataDirectory(File dir) {
		this.dataDirectory = dir;
	}

	public void start() {
		try {
			logger.info("Starting embeded zookeeper...");
			server = new ZooKeeperServer(dataDirectory, dataDirectory, tickTime);
			NIOServerCnxn.Factory standaloneServerFactory = new NIOServerCnxn.Factory(new InetSocketAddress(port), numberOfConnections);
			standaloneServerFactory.startup(server);
			logger.info("Completed starting embeded zookeeper.");
			isReady = true;
		} catch (Exception e) {
			isReady = false;
			logger.error("An error occurred when initialising the zookeeper.",e);
		}
	}

	public boolean isReady() {
		return isReady&&server.isRunning();
	}

	public void stop() {
		if(server!=null) {
			try {
				server.shutdown();
			} catch (Exception e) {
				logger.error("An error occurred when stopping the embeded zookeeper.",e);
			}
		}
	}
	
	public String getUrl() {
		return "localhost:"+port;
	}
}
