package com.mawen.learn.rocketmq.store;

import java.util.Map;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
public interface MessageArrivingListener {

	void arriving(String topic, int queueId, long logicOffset, long tagsCode,
	              long msgStoreTime, byte[] filterBitMap, Map<String, String> properties);
}
