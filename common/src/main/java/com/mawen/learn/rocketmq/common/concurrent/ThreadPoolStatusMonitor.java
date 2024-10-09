package com.mawen.learn.rocketmq.common.concurrent;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public interface ThreadPoolStatusMonitor {

	String describe();

	double value(ThreadPoolExecutor executor);

	boolean needPrintJstack(ThreadPoolExecutor executor, double value);
}
