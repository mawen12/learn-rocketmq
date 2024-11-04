package com.mawen.learn.rocketmq.store;

import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
@Getter
@Setter
public class PutMessageContext {
	private String topicQueueTableKey;

	private long[] phyPos;

	private int batchSize;

	public PutMessageContext(String topicQueueTableKey) {
		this.topicQueueTableKey = topicQueueTableKey;
	}
}
