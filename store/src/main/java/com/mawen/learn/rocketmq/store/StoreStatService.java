package com.mawen.learn.rocketmq.store;

import com.mawen.learn.rocketmq.common.ServiceThread;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
@Getter
public class StoreStatService extends ServiceThread {

	private static final Logger log = LoggerFactory.getLogger(StoreStatService.class);

	private static final int FREQUENCY_OF_SAMPLING = 1000;


	@Override
	public String getServiceName() {
		return "";
	}

	@Override
	public void run() {

	}

	@AllArgsConstructor
	static class CallSnapshot {
		public final long timestamp;
		public final long callTimesTotal;

		public static double getTPs(final CallSnapshot begin, final CallSnapshot end) {
			long total = end.callTimesTotal - begin.callTimesTotal;
			Long time = end.timestamp - begin.timestamp;

			double tps = total / time.doubleValue();

			return tps * 1000;
		}
	}
}
