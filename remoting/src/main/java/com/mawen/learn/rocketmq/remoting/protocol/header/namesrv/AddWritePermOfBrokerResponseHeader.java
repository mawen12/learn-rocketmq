package com.mawen.learn.rocketmq.remoting.protocol.header.namesrv;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class AddWritePermOfBrokerResponseHeader implements CommandCustomHeader {

	@CFNotNull
	private Integer addTopicCount;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public Integer getAddTopicCount() {
		return addTopicCount;
	}

	public void setAddTopicCount(Integer addTopicCount) {
		this.addTopicCount = addTopicCount;
	}
}
