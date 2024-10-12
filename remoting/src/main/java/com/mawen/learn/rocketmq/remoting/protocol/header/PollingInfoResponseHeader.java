package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class PollingInfoResponseHeader implements CommandCustomHeader {

	@CFNotNull
	private int pollingNum;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public int getPollingNum() {
		return pollingNum;
	}

	public void setPollingNum(int pollingNum) {
		this.pollingNum = pollingNum;
	}
}
