package com.mawen.learn.rocketmq.remoting.protocol.header.namesrv;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class WipeWritePermOfBrokerResponseHeader implements CommandCustomHeader {

	@CFNotNull
	private Integer wipeTopicCount;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public Integer getWipeTopicCount() {
		return wipeTopicCount;
	}

	public void setWipeTopicCount(Integer wipeTopicCount) {
		this.wipeTopicCount = wipeTopicCount;
	}
}
