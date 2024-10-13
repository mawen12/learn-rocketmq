package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.List;

import com.mawen.learn.rocketmq.common.message.MessageQueueForC;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class ResetOffsetBodyForC extends RemotingSerializable {

	private List<MessageQueueForC> offsetTable;

	public List<MessageQueueForC> getOffsetTable() {
		return offsetTable;
	}

	public void setOffsetTable(List<MessageQueueForC> offsetTable) {
		this.offsetTable = offsetTable;
	}
}
