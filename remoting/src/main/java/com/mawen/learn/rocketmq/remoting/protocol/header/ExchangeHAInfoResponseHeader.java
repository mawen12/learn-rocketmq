package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNullable;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class ExchangeHAInfoResponseHeader implements CommandCustomHeader {

	@CFNullable
	private String masterHaAddress;

	@CFNullable
	private Long masterFlushOffset;

	@CFNullable
	private String masterAddress;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getMasterHaAddress() {
		return masterHaAddress;
	}

	public void setMasterHaAddress(String masterHaAddress) {
		this.masterHaAddress = masterHaAddress;
	}

	public Long getMasterFlushOffset() {
		return masterFlushOffset;
	}

	public void setMasterFlushOffset(Long masterFlushOffset) {
		this.masterFlushOffset = masterFlushOffset;
	}

	public String getMasterAddress() {
		return masterAddress;
	}

	public void setMasterAddress(String masterAddress) {
		this.masterAddress = masterAddress;
	}
}

