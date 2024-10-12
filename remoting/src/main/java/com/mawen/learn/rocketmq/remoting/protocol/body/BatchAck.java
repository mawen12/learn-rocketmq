package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.io.Serializable;
import java.util.BitSet;

import com.alibaba.fastjson2.annotation.JSONField;
import com.mawen.learn.rocketmq.remoting.protocol.BitSetSerializerDeserializer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class BatchAck implements Serializable {

	@JSONField(name = "c", alternateNames = {"consumerGroup"})
	private String consumerGroup;

	@JSONField(name = "t", alternateNames = {"topic"})
	private String topic;

	@JSONField(name = "r", alternateNames = {"retry"})
	private String retry;

	@JSONField(name = "so", alternateNames = {"startOffset"})
	private long startOffset;

	@JSONField(name = "q", alternateNames = {"queueId"})
	private int queueId;

	@JSONField(name = "rq", alternateNames = {"reviveQueueId"})
	private int reviveQueueId;

	@JSONField(name = "pt", alternateNames = {"popTime"})
	private long popTime;

	@JSONField(name = "it", alternateNames = {"invisibleTime"})
	private long invisibleTime;

	@JSONField(name = "b", alternateNames = {"bitSet"}, serializeUsing = BitSetSerializerDeserializer.class, deserializeUsing = BitSetSerializerDeserializer.class)
	private BitSet bitSet;

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getRetry() {
		return retry;
	}

	public void setRetry(String retry) {
		this.retry = retry;
	}

	public long getStartOffset() {
		return startOffset;
	}

	public void setStartOffset(long startOffset) {
		this.startOffset = startOffset;
	}

	public int getQueueId() {
		return queueId;
	}

	public void setQueueId(int queueId) {
		this.queueId = queueId;
	}

	public int getReviveQueueId() {
		return reviveQueueId;
	}

	public void setReviveQueueId(int reviveQueueId) {
		this.reviveQueueId = reviveQueueId;
	}

	public long getPopTime() {
		return popTime;
	}

	public void setPopTime(long popTime) {
		this.popTime = popTime;
	}

	public long getInvisibleTime() {
		return invisibleTime;
	}

	public void setInvisibleTime(long invisibleTime) {
		this.invisibleTime = invisibleTime;
	}

	public BitSet getBitSet() {
		return bitSet;
	}

	public void setBitSet(BitSet bitSet) {
		this.bitSet = bitSet;
	}

	@Override
	public String toString() {
		return "BatchAck{" +
				"consumerGroup='" + consumerGroup + '\'' +
				", topic='" + topic + '\'' +
				", retry='" + retry + '\'' +
				", startOffset=" + startOffset +
				", queueId=" + queueId +
				", reviveQueueId=" + reviveQueueId +
				", popTime=" + popTime +
				", invisibleTime=" + invisibleTime +
				", bitSet=" + bitSet +
				'}';
	}
}
