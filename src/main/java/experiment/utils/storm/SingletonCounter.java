package experiment.utils.storm;

import java.util.concurrent.atomic.AtomicInteger;

public enum SingletonCounter {

	INSTANCE;
	
	private AtomicInteger counter = new AtomicInteger();
	
	public void increment() {
		counter.incrementAndGet();
	}
	
	public int get() {
		return counter.get();
	}
}
