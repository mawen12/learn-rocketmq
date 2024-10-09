package com.mawen.learn.rocketmq.common.statistics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class StatisticsItemFormatter {

	public String format(StatisticsItem item) {
		final String separator = "|";
		StringBuilder sb = new StringBuilder();
		sb.append(item.getStatKind()).append(separator);
		sb.append(item.getStatObject()).append(separator);
		for (AtomicLong acc : item.getItemAccumulates()) {
			sb.append(acc.get()).append(separator);
		}
		sb.append(item.getInvokeTimes());
		return sb.toString();
	}
}
