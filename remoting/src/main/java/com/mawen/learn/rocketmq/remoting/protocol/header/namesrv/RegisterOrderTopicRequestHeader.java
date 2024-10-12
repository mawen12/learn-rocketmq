package com.mawen.learn.rocketmq.remoting.protocol.header.namesrv;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class RegisterOrderTopicRequestHeader implements CommandCustomHeader {

	@CFNotNull
	private String topic;

	@CFNotNull
	private String orderTopicString;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getOrderTopicString() {
		return orderTopicString;
	}

	public void setOrderTopicString(String orderTopicString) {
		this.orderTopicString = orderTopicString;
	}
}
