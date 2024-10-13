package com.mawen.learn.rocketmq.remoting.protocol.body;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class CheckClientRequestBody extends RemotingSerializable {

	private String clientId;

	private String group;

	private SubscriptionData subscriptionData;

	private String namespace;

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public SubscriptionData getSubscriptionData() {
		return subscriptionData;
	}

	public void setSubscriptionData(SubscriptionData subscriptionData) {
		this.subscriptionData = subscriptionData;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
}
