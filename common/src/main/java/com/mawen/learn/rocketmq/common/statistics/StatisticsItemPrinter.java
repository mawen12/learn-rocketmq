package com.mawen.learn.rocketmq.common.statistics;

import org.apache.rocketmq.logging.org.slf4j.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public class StatisticsItemPrinter {

	private Logger log;

	private StatisticsItemFormatter formatter;

	public StatisticsItemPrinter(StatisticsItemFormatter formatter, Logger log) {
		this.formatter = formatter;
		this.log = log;
	}

	public void log(Logger log) {
		this.log = log;
	}

	public void formatter(StatisticsItemFormatter formatter) {
		this.formatter = formatter;
	}

	public void print(String prefix, StatisticsItem item, String... suffixs) {
		StringBuilder sb = new StringBuilder();
		for (String str : suffixs) {
			sb.append(str);
		}

		if (log != null) {
			log.info("{}{}{}", prefix, formatter.format(item), sb.toString());
		}
	}
}
