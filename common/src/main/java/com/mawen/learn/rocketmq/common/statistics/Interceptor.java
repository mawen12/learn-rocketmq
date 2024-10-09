package com.mawen.learn.rocketmq.common.statistics;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/9
 */
public interface Interceptor {

	void inc(long... deltas);

	void reset();
}
