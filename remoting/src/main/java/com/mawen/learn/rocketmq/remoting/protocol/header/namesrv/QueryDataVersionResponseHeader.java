package com.mawen.learn.rocketmq.remoting.protocol.header.namesrv;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class QueryDataVersionResponseHeader implements CommandCustomHeader {

	@CFNotNull
	private Boolean changed;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public Boolean getChanged() {
		return changed;
	}

	public void setChanged(Boolean changed) {
		this.changed = changed;
	}

	@Override
	public String toString() {
		return "QueryDataVersionResponseHeader{" +
				"changed=" + changed +
				'}';
	}
}
