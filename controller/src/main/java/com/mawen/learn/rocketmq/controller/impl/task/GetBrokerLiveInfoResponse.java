package com.mawen.learn.rocketmq.controller.impl.task;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
public class GetBrokerLiveInfoResponse implements CommandCustomHeader {

	public GetBrokerLiveInfoResponse() {}

	@Override
	public void checkFields() throws RemotingCommandException {
		// NOP
	}

	@Override
	public String toString() {
		return "GetBrokerLiveInfoResponse{}";
	}
}
