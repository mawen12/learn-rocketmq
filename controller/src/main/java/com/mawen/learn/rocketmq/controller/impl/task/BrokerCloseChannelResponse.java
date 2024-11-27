package com.mawen.learn.rocketmq.controller.impl.task;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
@ToString
public class BrokerCloseChannelResponse implements CommandCustomHeader {

	@Override
	public void checkFields() throws RemotingCommandException {

	}
}
