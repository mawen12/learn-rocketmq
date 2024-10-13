package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class TopicList extends RemotingSerializable {

	private Set<String> topicList = ConcurrentHashMap.newKeySet();

	private String brokerAddr;

	public Set<String> getTopicList() {
		return topicList;
	}

	public void setTopicList(Set<String> topicList) {
		this.topicList = topicList;
	}

	public String getBrokerAddr() {
		return brokerAddr;
	}

	public void setBrokerAddr(String brokerAddr) {
		this.brokerAddr = brokerAddr;
	}
}
