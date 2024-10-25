package com.mawen.learn.rocketmq.client.latency;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/25
 */
public interface LatencyFaultTolerance<T> {

	void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration, final boolean reachable);

	boolean isAvailable(final T name);

	boolean isReachable(final T name);

	void remove(final T name);

	T pickOneAtLeast();

	void startDetector();

	void shutdown();

	void detectByOneRound();

	void setDetectTimeout(final int detectTimeout);

	void setDetectInterval(final int detectInterval);

	void setStartDetectorEnable(final boolean startDetectorEnable);

	boolean isStartDetectorEnable();
}
