package experiment.utils.kafka;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleKafkaDataIn {

	private Logger logger = Logger.getLogger(getClass());
	private AtomicInteger counter = new AtomicInteger();
	private Producer<String, String> producer;
	private String topic;

	public SimpleKafkaDataIn(String brokerUrls, String topic) {
		this.topic = topic;
		Properties props = new Properties();
		props.put("metadata.broker.list", brokerUrls);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		producer = new Producer<String, String>(new ProducerConfig(props));
	}
	
	public void submit(String key, String message) {
		logger.info("Sending " + key);
		KeyedMessage<String,String> kv = new KeyedMessage<String, String>(topic, key, message);
		producer.send(kv);
		counter.incrementAndGet();
	}
	
	public int getCount() {
		return counter.get();
	}
	
	public void shutdown() {
		if(producer!=null) {
			producer.close();
		}
	}
}
