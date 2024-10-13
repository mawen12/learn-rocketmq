package com.mawen.learn.rocketmq.remoting.protocol.body;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class QuerySubscriptionResponseBody extends RemotingSerializable {

	private SubscriptionData subscriptionData;

	private String group;

	private String topic;

	public SubscriptionData getSubscriptionData() {
		return subscriptionData;
	}

	public void setSubscriptionData(SubscriptionData subscriptionData) {
		this.subscriptionData = subscriptionData;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
}
