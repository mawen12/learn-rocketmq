package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class QueryMessageResponseHeader implements CommandCustomHeader {

	@CFNotNull
	private Long indexLastUpdateTimestamp;

	@CFNotNull
	private Long indexLastUpdatePhyoffset;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public Long getIndexLastUpdateTimestamp() {
		return indexLastUpdateTimestamp;
	}

	public void setIndexLastUpdateTimestamp(Long indexLastUpdateTimestamp) {
		this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
	}

	public Long getIndexLastUpdatePhyoffset() {
		return indexLastUpdatePhyoffset;
	}

	public void setIndexLastUpdatePhyoffset(Long indexLastUpdatePhyoffset) {
		this.indexLastUpdatePhyoffset = indexLastUpdatePhyoffset;
	}
}
