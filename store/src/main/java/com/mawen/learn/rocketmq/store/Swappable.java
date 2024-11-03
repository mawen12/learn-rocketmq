package com.mawen.learn.rocketmq.store;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public interface Swappable {

	void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs);

	void cleanSwappedMap(long forceCleanSwapIntervalMs);
}
