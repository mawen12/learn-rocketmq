package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.TopicQueueRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, action = Action.SUB)
public class ReplyMessageRequestHeader extends TopicQueueRequestHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
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

	@CFNotNull
	private String properties;

	@CFNotNull
	private Integer reconsumeTimes;

	@CFNotNull
	private boolean unitMode = false;

	@CFNotNull
	private String bornHost;

	@CFNotNull
	private String storeHost;

	@CFNotNull
	private long storeTimestamp;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getProducerGroup() {
		return producerGroup;
	}

	public void setProducerGroup(String producerGroup) {
		this.producerGroup = producerGroup;
	}

	@Override
	public String getTopic() {
		return topic;
	}

	@Override
	public void setTopic(String topic) {
		this.topic = topic;
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

	@Override
	public Integer getQueueId() {
		return queueId;
	}

	@Override
	public void setQueueId(Integer queueId) {
		this.queueId = queueId;
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
		return reconsumeTimes;
	}

	public void setReconsumeTimes(Integer reconsumeTimes) {
		this.reconsumeTimes = reconsumeTimes;
	}

	public boolean isUnitMode() {
		return unitMode;
	}

	public void setUnitMode(boolean unitMode) {
		this.unitMode = unitMode;
	}

	public String getBornHost() {
		return bornHost;
	}

	public void setBornHost(String bornHost) {
		this.bornHost = bornHost;
	}

	public String getStoreHost() {
		return storeHost;
	}

	public void setStoreHost(String storeHost) {
		this.storeHost = storeHost;
	}

	public long getStoreTimestamp() {
		return storeTimestamp;
	}

	public void setStoreTimestamp(long storeTimestamp) {
		this.storeTimestamp = storeTimestamp;
	}
}
