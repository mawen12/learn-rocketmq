package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.TopicQueueRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
@RocketMQAction(value = RequestCode.SEND_MESSAGE, action = Action.PUB)
public class SendMessageRequestHeader extends TopicQueueRequestHeader {

	@CFNotNull
	private String producerGroup;

	@CFNotNull
	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNotNull
	private String defaultTopic;

	@CFNotNull
	private Integer defaultTopicQueueNums;

	@CFNotNull
	private Integer queueId;

	@CFNotNull
	private Integer sysFlag;

	@CFNotNull
	private Long bornTimestamp;

	@CFNotNull
	private Integer flag;

	@CFNullable
	private String properties;

	@CFNullable
	private Integer reconsumeTimes;

	@CFNullable
	private Boolean unitMode;

	@CFNullable
	private Boolean batch;

	private Integer maxReconsumeTimes;

	public static SendMessageRequestHeader parseRequestHeader(RemotingCommand request) throws RemotingCommandException {
		SendMessageRequestHeaderV2 v2 = null;
		SendMessageRequestHeader v1 = null;
		switch (request.getCode()) {
			case RequestCode.SEND_BATCH_MESSAGE:
			case RequestCode.SEND_MESSAGE_V2:
				v2 = request.decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
			case RequestCode.SEND_MESSAGE:
				if (v2 == null) {
					v1 = request.decodeCommandCustomHeader(SendMessageRequestHeader.class);
				}
				else {
					v1 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV1(v2);
				}
			default:
				break;
		}
		return v1;
	}

	@Override
	public Integer getQueueId() {
		return queueId;
	}

	@Override
	public void setQueueId(Integer queueId) {
		this.queueId = queueId;
	}

	@Override
	public String getTopic() {
		return topic;
	}

	@Override
	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getProducerGroup() {
		return producerGroup;
	}

	public void setProducerGroup(String producerGroup) {
		this.producerGroup = producerGroup;
	}

	public String getDefaultTopic() {
		return defaultTopic;
	}

	public void setDefaultTopic(String defaultTopic) {
		this.defaultTopic = defaultTopic;
	}

	public Integer getDefaultTopicQueueNums() {
		return defaultTopicQueueNums;
	}

	public void setDefaultTopicQueueNums(Integer defaultTopicQueueNums) {
		this.defaultTopicQueueNums = defaultTopicQueueNums;
	}

	public Integer getSysFlag() {
		return sysFlag;
	}

	public void setSysFlag(Integer sysFlag) {
		this.sysFlag = sysFlag;
	}

	public Long getBornTimestamp() {
		return bornTimestamp;
	}

	public void setBornTimestamp(Long bornTimestamp) {
		this.bornTimestamp = bornTimestamp;
	}

	public Integer getFlag() {
		return flag;
	}

	public void setFlag(Integer flag) {
		this.flag = flag;
	}

	public String getProperties() {
		return properties;
	}

	public void setProperties(String properties) {
		this.properties = properties;
	}

	public Integer getReconsumeTimes() {
		if (reconsumeTimes == null) {
			return 0;
		}
		return reconsumeTimes;
	}

	public void setReconsumeTimes(Integer reconsumeTimes) {
		this.reconsumeTimes = reconsumeTimes;
	}

	public boolean isUnitMode() {
		if (unitMode == null) {
			return false;
		}
		return unitMode;
	}

	public void setUnitMode(Boolean unitMode) {
		this.unitMode = unitMode;
	}

	public boolean isBatch() {
		if (batch == null) {
			return false;
		}
		return batch;
	}

	public void setBatch(Boolean batch) {
		this.batch = batch;
	}

	public Integer getMaxReconsumeTimes() {
		return maxReconsumeTimes;
	}

	public void setMaxReconsumeTimes(Integer maxReconsumeTimes) {
		this.maxReconsumeTimes = maxReconsumeTimes;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("producerGroup", producerGroup)
				.add("topic", topic)
				.add("defaultTopic", defaultTopic)
				.add("defaultTopicQueueNums", defaultTopicQueueNums)
				.add("queueId", queueId)
				.add("sysFlag", sysFlag)
				.add("bornTimestamp", bornTimestamp)
				.add("flag", flag)
				.add("properties", properties)
				.add("reconsumeTimes", reconsumeTimes)
				.add("unitMode", unitMode)
				.add("batch", batch)
				.add("maxReconsumeTimes", maxReconsumeTimes)
				.toString();
	}
}
