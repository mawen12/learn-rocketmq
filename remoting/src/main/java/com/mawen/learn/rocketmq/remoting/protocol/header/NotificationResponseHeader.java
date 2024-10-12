package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class NotificationResponseHeader implements CommandCustomHeader {

	@CFNotNull
	private boolean hasMsg = false;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public boolean isHasMsg() {
		return hasMsg;
	}

	public void setHasMsg(boolean hasMsg) {
		this.hasMsg = hasMsg;
	}
}
