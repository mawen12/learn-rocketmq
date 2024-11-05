package com.mawen.learn.rocketmq.store;

import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
@Getter
@Setter
@ToString
public class DispatchRequest {

	private final String topic;
	private final int queueId;
	private final long commitLogOffset;
	private int msgSize;
	private final long tagsCode;
	private final long storeTimestamp;
	private final long consumeQueueOffset;
	private final String keys;
	private final boolean success;
	private final String uniqKey;

	private final int sysFlag;
	private final long preparedTransactionOffset;
	private final Map<String, String> propertiesMap;
	private byte[] bitMap;

	private int bufferSize = -1;

	private long msgBaseOffset = -1;

	private short batchSize = 1;

	private long nextReputFromOffset = -1;

	private String offsetId;

	public DispatchRequest(int size) {
		this(size, false);
	}

	public DispatchRequest(int size, boolean success) {
		this.topic = "";
		this.queueId = 0;
		this.commitLogOffset = 0;
		this.msgSize = size;
		this.tagsCode = 0;
		this.storeTimestamp = 0;
		this.consumeQueueOffset = 0;
		this.keys = "";
		this.uniqKey = null;
		this.sysFlag = 0;
		this.preparedTransactionOffset = 0;
		this.success = success;
		this.propertiesMap = null;
	}

	public DispatchRequest(String topic, int queueId, long consumeQueueOffset, long commitLogOffset, int size, long tagsCode) {
		this.topic = topic;
		this.queueId = queueId;
		this.commitLogOffset = commitLogOffset;
		this.msgSize = size;
		this.tagsCode = tagsCode;
		this.storeTimestamp = 0;
		this.consumeQueueOffset = consumeQueueOffset;
		this.keys = "";
		this.uniqKey = null;
		this.sysFlag = 0;
		this.preparedTransactionOffset = 0;
		this.success = false;
		this.propertiesMap = null;
	}

	public DispatchRequest(String topic, int queueId, long commitLogOffset, long tagsCode, long storeTimestamp,
			long consumeQueueOffset, String keys, String uniqKey, int sysFlag, long preparedTransactionOffset, Map<String, String> propertiesMap) {
		this.topic = topic;
		this.queueId = queueId;
		this.commitLogOffset = commitLogOffset;
		this.tagsCode = tagsCode;
		this.storeTimestamp = storeTimestamp;
		this.consumeQueueOffset = consumeQueueOffset;
		this.keys = keys;
		this.success = false;

		this.uniqKey = uniqKey;
		this.sysFlag = sysFlag;
		this.preparedTransactionOffset = preparedTransactionOffset;
		this.propertiesMap = propertiesMap;
	}


}
