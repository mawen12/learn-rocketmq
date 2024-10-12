package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.rpc.TopicRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class InitConsumerOffsetRequestHeader extends TopicRequestHeader {

	private String topic;

	private int initMode;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	@Override
	public String getTopic() {
		return topic;
	}

	@Override
	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getInitMode() {
		return initMode;
	}

	public void setInitMode(int initMode) {
		this.initMode = initMode;
	}
}
