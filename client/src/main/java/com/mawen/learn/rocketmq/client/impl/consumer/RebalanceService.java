package com.mawen.learn.rocketmq.client.impl.consumer;

import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.ServiceThread;
import lombok.RequiredArgsConstructor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
@RequiredArgsConstructor
public class RebalanceService extends ServiceThread {

	private static final Logger log = LoggerFactory.getLogger(RebalanceService.class);

	private static long waitInterval = Long.parseLong(System.getProperty("rocketmq.client.rebalance.waitInterval", "20000"));
	private static long minInterval = Long.parseLong(System.getProperty("rocketmq.client.rebalance.minInterval", "1000"));

	private final MQClientInstance mqClientFactory;
	private long lastRebalanceTimestamp = System.currentTimeMillis();

	@Override
	public String getServiceName() {
		return RebalanceService.class.getSimpleName();
	}

	@Override
	public void run() {
		log.info("{} service start", this.getServiceName());

		long realWaitInterval = waitInterval;
		while (!this.isStopped()) {
			this.waitForRunning(realWaitInterval);

			long interval = System.currentTimeMillis() - lastRebalanceTimestamp;
			if (interval < minInterval) {
				realWaitInterval = minInterval - interval;
			}
			else {
				boolean balanced = this.mqClientFactory.doRebalance();
				realWaitInterval = balanced ? waitInterval : minInterval;
				lastRebalanceTimestamp = System.currentTimeMillis();
			}
		}

		log.info("{} service end", this.getServiceName());
	}
}
