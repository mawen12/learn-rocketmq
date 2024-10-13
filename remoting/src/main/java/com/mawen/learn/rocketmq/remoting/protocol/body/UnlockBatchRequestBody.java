package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.HashSet;
import java.util.Set;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class UnlockBatchRequestBody extends RemotingSerializable {

	private String consumerGroup;

	private String clientId;

	private boolean onlyThisBroker = false;

	private Set<MessageQueue> mqSet = new HashSet<>();

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public boolean isOnlyThisBroker() {
		return onlyThisBroker;
	}

	public void setOnlyThisBroker(boolean onlyThisBroker) {
		this.onlyThisBroker = onlyThisBroker;
	}

	public Set<MessageQueue> getMqSet() {
		return mqSet;
	}

	public void setMqSet(Set<MessageQueue> mqSet) {
		this.mqSet = mqSet;
	}

	@Override
	public String toString() {
		return "UnlockBatchRequestBody{" +
		       "consumerGroup='" + consumerGroup + '\'' +
		       ", clientId='" + clientId + '\'' +
		       ", onlyThisBroker=" + onlyThisBroker +
		       ", mqSet=" + mqSet +
		       "} " + super.toString();
	}
}
