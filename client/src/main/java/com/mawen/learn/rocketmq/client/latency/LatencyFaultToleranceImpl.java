package com.mawen.learn.rocketmq.client.latency;

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.client.common.ThreadLocalIndex;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/25
 */
@RequiredArgsConstructor
public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {

	private static final Logger log = LoggerFactory.getLogger(LatencyFaultToleranceImpl.class);

	private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<>(16);
	private int detectTimeout = 200;
	private int detectInterval = 2000;
	private final ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

	private volatile boolean startDetectorEnable = false;
	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("LatencyFaultToleranceScheduledThread"));

	private final Resolver resolver;
	private final ServiceDetector serviceDetector;


	@Override
	public void updateFaultItem(String name, long currentLatency, long notAvailableDuration, boolean reachable) {
		FaultItem old = this.faultItemTable.get(name);
		if (old == null) {
			FaultItem faultItem = new FaultItem(name);
			faultItem.setCurrentLatency(currentLatency);
			faultItem.updateNotAvailableDuration(notAvailableDuration);
			faultItem.setReachableFlag(reachable);
			old = this.faultItemTable.putIfAbsent(name, faultItem);
		}

		if (old != null) {
			old.setCurrentLatency(currentLatency);
			old.updateNotAvailableDuration(notAvailableDuration);
			old.setReachableFlag(reachable);
		}

		if (!reachable) {
			log.info("{} is unreachable, it will be used util it's reachable");
		}
	}

	@Override
	public boolean isAvailable(String name) {
		FaultItem faultItem = this.faultItemTable.get(name);
		if (faultItem != null) {
			return faultItem.isAvailable();
		}
		return true;
	}

	@Override
	public boolean isReachable(String name) {
		FaultItem faultItem = this.faultItemTable.get(name);
		if (faultItem != null) {
			return faultItem.isReachableFlag();
		}
		return true;
	}

	@Override
	public void remove(String name) {
		this.faultItemTable.remove(name);
	}

	@Override
	public String pickOneAtLeast() {
		Enumeration<FaultItem> elements = this.faultItemTable.elements();
		List<FaultItem> tmpList = new LinkedList<>();

		while (elements.hasMoreElements()) {
			FaultItem faultItem = elements.nextElement();
			tmpList.add(faultItem);
		}

		if (!tmpList.isEmpty()) {
			Collections.shuffle(tmpList);
			for (FaultItem faultItem : tmpList) {
				if (faultItem.reachableFlag) {
					return faultItem.name;
				}
			}
		}

		return null;
	}

	@Override
	public void startDetector() {
		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				if (startDetectorEnable) {
					detectByOneRound();
				}
			}
			catch (Exception e) {
				log.warn("Unexpected exception raised while detecting service reachability", e);
			}
		}, 3, 3, TimeUnit.SECONDS);
	}

	@Override
	public void shutdown() {
		this.scheduledExecutorService.shutdown();
	}

	@Override
	public void detectByOneRound() {
		for (Map.Entry<String, FaultItem> entry : this.faultItemTable.entrySet()) {
			FaultItem brokerItem = entry.getValue();
			if (System.currentTimeMillis() - brokerItem.checkStamp >= 0) {
				brokerItem.checkStamp = System.currentTimeMillis() + this.detectInterval;
				String brokerAddr = resolver.resolve(brokerItem.getName());
				if (brokerAddr == null) {
					faultItemTable.remove(entry.getKey());
					continue;
				}
				if (serviceDetector == null) {
					continue;
				}
				boolean serviceOK = serviceDetector.detect(brokerAddr, detectTimeout);
				if (serviceOK && !brokerItem.reachableFlag) {
					log.info("{} is reachable now, then it can be used.", brokerItem.name);
					brokerItem.reachableFlag = true;
				}
			}
		}
	}

	@Override
	public void setDetectTimeout(int detectTimeout) {
		this.detectTimeout = detectTimeout;
	}

	@Override
	public void setDetectInterval(int detectInterval) {
		this.detectInterval = detectInterval;
	}

	@Override
	public void setStartDetectorEnable(boolean startDetectorEnable) {
		this.startDetectorEnable = startDetectorEnable;
	}

	@Override
	public boolean isStartDetectorEnable() {
		return startDetectorEnable;
	}

	@Setter
	@Getter
	@RequiredArgsConstructor
	public class FaultItem implements Comparable<FaultItem> {
		private final String name;
		private volatile long currentLatency;
		private volatile long startTimestamp;
		private volatile long checkStamp;
		private volatile boolean reachableFlag;

		public void updateNotAvailableDuration(long notAvailableDuration) {
			if (notAvailableDuration > 0 && System.currentTimeMillis() + notAvailableDuration > this.startTimestamp) {
				this.startTimestamp = System.currentTimeMillis() + notAvailableDuration;
				log.info("{} will be isolated for {}ms.", name, notAvailableDuration);
			}
		}

		@Override
		public int compareTo(LatencyFaultToleranceImpl.FaultItem other) {
			if (this.isAvailable() != other.isAvailable()) {
				if (this.isAvailable()) {
					return -1;
				}

				if (other.isAvailable()) {
					return 1;
				}
			}

			if (this.currentLatency < other.currentLatency) {
				return -1;
			} else if (this.currentLatency > other.currentLatency) {
				return 1;
			}

			if (this.startTimestamp < other.startTimestamp) {
				return -1;
			} else if (this.startTimestamp > other.startTimestamp) {
				return 1;
			}
			return 0;
		}

		public boolean isAvailable() {
			return System.currentTimeMillis() >= startTimestamp;
		}

		@Override
		public int hashCode() {
			int result = getName() != null ? getName().hashCode() : 0;
			result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
			result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
			return result;
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof FaultItem)) {
				return false;
			}

			final FaultItem faultItem = (FaultItem) o;

			if (getCurrentLatency() != faultItem.getCurrentLatency()) {
				return false;
			}
			if (getStartTimestamp() != faultItem.getStartTimestamp()) {
				return false;
			}
			return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;
		}

		@Override
		public String toString() {
			return "FaultItem{" +
					"name='" + name + '\'' +
					", currentLatency=" + currentLatency +
					", startTimestamp=" + startTimestamp +
					", reachableFlag=" + reachableFlag +
					'}';
		}
	}
}
