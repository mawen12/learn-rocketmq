package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.consumer.ConsumerFromWhere;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class ConsumerConnection extends RemotingSerializable {

	private Set<Connection> connectionSet = new HashSet<>();

	private ConcurrentMap<String, SubscriptionData> subscriptionTable = new ConcurrentHashMap<>();

	private ConsumeType consumeType;

	private MessageModel messageModel;

	private ConsumerFromWhere consumerFromWhere;

	public int computeMinVersion() {
		int minVersion = Integer.MAX_VALUE;
		for (Connection c : this.connectionSet) {
			if (c.getVersion() < minVersion) {
				minVersion = c.getVersion();
			}
		}
		return minVersion;
	}

	public Set<Connection> getConnectionSet() {
		return connectionSet;
	}

	public void setConnectionSet(Set<Connection> connectionSet) {
		this.connectionSet = connectionSet;
	}

	public ConcurrentMap<String,SubscriptionData> getSubscriptionTable() {
		return subscriptionTable;
	}

	public void setSubscriptionTable(ConcurrentMap<String,SubscriptionData> subscriptionTable) {
		this.subscriptionTable = subscriptionTable;
	}

	public ConsumeType getConsumeType() {
		return consumeType;
	}

	public void setConsumeType(ConsumeType consumeType) {
		this.consumeType = consumeType;
	}

	public MessageModel getMessageModel() {
		return messageModel;
	}

	public void setMessageModel(MessageModel messageModel) {
		this.messageModel = messageModel;
	}

	public ConsumerFromWhere getConsumerFromWhere() {
		return consumerFromWhere;
	}

	public void setConsumerFromWhere(ConsumerFromWhere consumerFromWhere) {
		this.consumerFromWhere = consumerFromWhere;
	}
}
