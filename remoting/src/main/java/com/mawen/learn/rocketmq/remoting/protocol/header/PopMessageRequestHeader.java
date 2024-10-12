package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.TopicQueueRequestHeader;
import org.checkerframework.checker.units.qual.C;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.POP_MESSAGE, action = Action.SUB)
public class PopMessageRequestHeader extends TopicQueueRequestHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String consumerGroup;

	@CFNotNull
	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNotNull
	private int queueId;

	@CFNotNull
	private int maxMsgNums;

	@CFNotNull
	private long invisibleTime;

	@CFNotNull
	private long pollTime;

	@CFNotNull
	private long bronTime;

	@CFNotNull
	private int initMode;

	private String expType;

	private String exp;

	private Boolean order = Boolean.FALSE;

	private String attemptId;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
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
	public Integer getQueueId() {
		if (queueId < 0) {
			return -1;
		}
		return queueId;
	}

	public void setQueueId(Integer queueId) {
		this.queueId = queueId;
	}

	public int getMaxMsgNums() {
		return maxMsgNums;
	}

	public void setMaxMsgNums(int maxMsgNums) {
		this.maxMsgNums = maxMsgNums;
	}

	public boolean isTimeoutTooMuch() {
		return System.currentTimeMillis() - bronTime - pollTime > 500;
	}

	public long getInvisibleTime() {
		return invisibleTime;
	}

	public void setInvisibleTime(long invisibleTime) {
		this.invisibleTime = invisibleTime;
	}

	public long getPollTime() {
		return pollTime;
	}

	public void setPollTime(long pollTime) {
		this.pollTime = pollTime;
	}

	public long getBronTime() {
		return bronTime;
	}

	public void setBronTime(long bronTime) {
		this.bronTime = bronTime;
	}

	public int getInitMode() {
		return initMode;
	}

	public void setInitMode(int initMode) {
		this.initMode = initMode;
	}

	public String getExpType() {
		return expType;
	}

	public void setExpType(String expType) {
		this.expType = expType;
	}

	public String getExp() {
		return exp;
	}

	public void setExp(String exp) {
		this.exp = exp;
	}

	public Boolean getOrder() {
		return order;
	}

	public boolean isOrder() {
		return this.order != null && this.order.booleanValue();
	}

	public void setOrder(Boolean order) {
		this.order = order;
	}

	public String getAttemptId() {
		return attemptId;
	}

	public void setAttemptId(String attemptId) {
		this.attemptId = attemptId;
	}

	@Override
	public String toString() {
		return "PopMessageRequestHeader{" +
				"consumerGroup='" + consumerGroup + '\'' +
				", topic='" + topic + '\'' +
				", queueId=" + queueId +
				", maxMsgNums=" + maxMsgNums +
				", invisibleTime=" + invisibleTime +
				", pollTime=" + pollTime +
				", bronTime=" + bronTime +
				", initMode=" + initMode +
				", expType='" + expType + '\'' +
				", exp='" + exp + '\'' +
				", order=" + order +
				", attemptId='" + attemptId + '\'' +
				"} " + super.toString();
	}
}
