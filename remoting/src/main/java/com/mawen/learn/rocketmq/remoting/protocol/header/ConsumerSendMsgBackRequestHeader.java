package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.RpcRequestHeader;
import com.mawen.learn.rocketmq.remoting.rpc.TopicRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.CONSUMER_SEND_MSG_BACK, action = Action.SUB)
public class ConsumerSendMsgBackRequestHeader extends RpcRequestHeader {

	@CFNotNull
	private Long offset;

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String group;

	@CFNotNull
	private Integer delayLevel;

	private String originMsgId;

	@RocketMQResource(ResourceType.TOPIC)
	private String originTopic;

	@CFNullable
	private boolean unitMode = false;

	private Integer maxReconsumeTimes;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public Long getOffset() {
		return offset;
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public Integer getDelayLevel() {
		return delayLevel;
	}

	public void setDelayLevel(Integer delayLevel) {
		this.delayLevel = delayLevel;
	}

	public String getOriginMsgId() {
		return originMsgId;
	}

	public void setOriginMsgId(String originMsgId) {
		this.originMsgId = originMsgId;
	}

	public String getOriginTopic() {
		return originTopic;
	}

	public void setOriginTopic(String originTopic) {
		this.originTopic = originTopic;
	}

	public boolean isUnitMode() {
		return unitMode;
	}

	public void setUnitMode(boolean unitMode) {
		this.unitMode = unitMode;
	}

	public Integer getMaxReconsumeTimes() {
		return maxReconsumeTimes;
	}

	public void setMaxReconsumeTimes(Integer maxReconsumeTimes) {
		this.maxReconsumeTimes = maxReconsumeTimes;
	}

	@Override
	public String toString() {
		return "ConsumerSendMsgBackRequestHeader{" +
				"offset=" + offset +
				", group='" + group + '\'' +
				", delayLevel=" + delayLevel +
				", originMsgId='" + originMsgId + '\'' +
				", originTopic='" + originTopic + '\'' +
				", unitMode=" + unitMode +
				", maxReconsumeTimes=" + maxReconsumeTimes +
				"} " + super.toString();
	}
}
