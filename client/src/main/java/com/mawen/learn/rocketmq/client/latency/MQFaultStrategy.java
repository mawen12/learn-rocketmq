package com.mawen.learn.rocketmq.client.latency;

import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.impl.producer.TopicPublishInfo;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/25
 */
@Setter
@Getter
public class MQFaultStrategy {

	private LatencyFaultTolerance<String> latencyFaultTolerance;
	private volatile boolean sendLatencyFaultEnable;
	private volatile boolean startDetectorEnable;
	private long[] latencyMax = {50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L};
	private long[] notAvailableDuration = {0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L};

	private ThreadLocal<BrokerFilter> threadBrokerFilter = new ThreadLocal<BrokerFilter>() {
		@Override
		protected BrokerFilter initialValue() {
			return new BrokerFilter();
		}
	};

	private TopicPublishInfo.QueueFilter reachableFilter = new TopicPublishInfo.QueueFilter() {
		@Override
		public boolean filter(MessageQueue mq) {
			return latencyFaultTolerance.isReachable(mq.getBrokerName());
		}
	};

	private TopicPublishInfo.QueueFilter availableFilter = new TopicPublishInfo.QueueFilter() {
		@Override
		public boolean filter(MessageQueue mq) {
			return latencyFaultTolerance.isAvailable(mq.getBrokerName());
		}
	};

	public MQFaultStrategy(ClientConfig cc, Resolver resolver, ServiceDetector serviceDetector) {
		this.latencyFaultTolerance = new LatencyFaultToleranceImpl(resolver, serviceDetector);
		this.latencyFaultTolerance.setDetectInterval(cc.getDetectInterval());
		this.latencyFaultTolerance.setDetectTimeout(cc.getDetectTimeout());

		this.setStartDetectorEnable(cc.isStartDetectorEnable());
		this.setSendLatencyFaultEnable(cc.isSendLatencyEnable());
	}

	public MQFaultStrategy(ClientConfig cc, LatencyFaultTolerance<String> tolerance) {
		this.latencyFaultTolerance = tolerance;
		this.latencyFaultTolerance.setDetectInterval(cc.getDetectInterval());
		this.latencyFaultTolerance.setDetectTimeout(cc.getDetectTimeout());

		this.setStartDetectorEnable(cc.isStartDetectorEnable());
		this.setSendLatencyFaultEnable(cc.isSendLatencyEnable());
	}

	public void startDetector() {
		this.latencyFaultTolerance.startDetector();
	}

	public void shutdown() {
		this.latencyFaultTolerance.shutdown();
	}

	public MessageQueue selectOneMessageQueue(final TopicPublishInfo info, final String lastBrokerName, final boolean resetIndex) {
		BrokerFilter brokerFilter = threadBrokerFilter.get();
		brokerFilter.setLastBrokerName(lastBrokerName);

		if (this.sendLatencyFaultEnable) {
			if (resetIndex) {
				info.resetIndex();
			}

			MessageQueue mq = info.selectOneMessageQueue(availableFilter, brokerFilter);
			if (mq != null) {
				return mq;
			}

			mq = info.selectOneMessageQueue(reachableFilter, brokerFilter);
			if (mq != null) {
				return mq;
			}

			return info.selectOneMessageQueue();
		}

		MessageQueue mq = info.selectOneMessageQueue(brokerFilter);
		if (mq != null) {
			return mq;
		}

		return info.selectOneMessageQueue();
	}

	public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation, final boolean reachable) {
		if (this.sendLatencyFaultEnable) {
			long duration = computeNotAvailableDuration(isolation ? 10000 : currentLatency);
			this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration, reachable);
		}
	}

	private long computeNotAvailableDuration(final long currentLatency) {
		for (int i = latencyMax.length - 1; i >= 0; i--) {
			if (currentLatency >= latencyMax[i]) {
				return this.notAvailableDuration[i];
			}
		}
		return 0;
	}

	@Setter
	public static class BrokerFilter implements TopicPublishInfo.QueueFilter {
		private String lastBrokerName;

		@Override
		public boolean filter(MessageQueue mq) {
			if (lastBrokerName != null) {
				return !mq.getBrokerName().equals(lastBrokerName);
			}
			return true;
		}
	}
}
