package com.mawen.learn.rocketmq.remoting.protocol.header.namesrv;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class GetKVConfigResponseHeader implements CommandCustomHeader {

	@CFNullable
	private String value;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
