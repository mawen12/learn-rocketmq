package com.mawen.learn.rocketmq.remoting.protocol.header.namesrv;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class RegisterBrokerResponseHeader implements CommandCustomHeader {

	@CFNullable
	private String haServerAddr;

	@CFNullable
	private String masterAddr;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getHaServerAddr() {
		return haServerAddr;
	}

	public void setHaServerAddr(String haServerAddr) {
		this.haServerAddr = haServerAddr;
	}

	public String getMasterAddr() {
		return masterAddr;
	}

	public void setMasterAddr(String masterAddr) {
		this.masterAddr = masterAddr;
	}
}
