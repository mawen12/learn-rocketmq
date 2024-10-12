package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.TopicFilterType;
import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.TopicRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.UPDATE_AND_CREATE_TOPIC, action = Action.CREATE)
public class CreateTopicRequestHeader extends TopicRequestHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNotNull
	private String defaultTopic;

	@CFNotNull
	private Integer readQueueNums;

	@CFNotNull
	private Integer writeQueueNums;

	@CFNotNull
	private Integer perm;

	@CFNotNull
	private String topicFilterType;

	private Integer topicSysFlag;

	@CFNotNull
	private Boolean order = false;

	private String attributes;

	@CFNullable
	private Boolean force = false;

	@Override
	public void checkFields() throws RemotingCommandException {
		try {
			TopicFilterType.valueOf(this.topicFilterType);
		}
		catch (Exception e) {
			throw new RemotingCommandException("topicFilterType = [" + topicFilterType + "] value invalid", e);
		}
	}

	public TopicFilterType getTopicFilterTypeEnum() {
		return TopicFilterType.valueOf(this.topicFilterType);
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

	public Integer getReadQueueNums() {
		return readQueueNums;
	}

	public void setReadQueueNums(Integer readQueueNums) {
		this.readQueueNums = readQueueNums;
	}

	public Integer getWriteQueueNums() {
		return writeQueueNums;
	}

	public void setWriteQueueNums(Integer writeQueueNums) {
		this.writeQueueNums = writeQueueNums;
	}

	public Integer getPerm() {
		return perm;
	}

	public void setPerm(Integer perm) {
		this.perm = perm;
	}

	public String getTopicFilterType() {
		return topicFilterType;
	}

	public void setTopicFilterType(String topicFilterType) {
		this.topicFilterType = topicFilterType;
	}

	public Integer getTopicSysFlag() {
		return topicSysFlag;
	}

	public void setTopicSysFlag(Integer topicSysFlag) {
		this.topicSysFlag = topicSysFlag;
	}

	public Boolean getOrder() {
		return order;
	}

	public void setOrder(Boolean order) {
		this.order = order;
	}

	public String getAttributes() {
		return attributes;
	}

	public void setAttributes(String attributes) {
		this.attributes = attributes;
	}

	public Boolean getForce() {
		return force;
	}

	public void setForce(Boolean force) {
		this.force = force;
	}

	@Override
	public String toString() {
		return "CreateTopicRequestHeader{" +
				"topic='" + topic + '\'' +
				", defaultTopic='" + defaultTopic + '\'' +
				", readQueueNums=" + readQueueNums +
				", writeQueueNums=" + writeQueueNums +
				", perm=" + perm +
				", topicFilterType='" + topicFilterType + '\'' +
				", topicSysFlag=" + topicSysFlag +
				", order=" + order +
				", attributes='" + attributes + '\'' +
				", force=" + force +
				"} " + super.toString();
	}
}
