package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.HashMap;
import java.util.Map;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class ResetOffsetBody extends RemotingSerializable {

	private Map<MessageQueue, Map> offsetTable;

	public ResetOffsetBody() {
		this.offsetTable = new HashMap<>();
	}

	public Map<MessageQueue, Map> getOffsetTable() {
		return offsetTable;
	}

	public void setOffsetTable(Map<MessageQueue, Map> offsetTable) {
		this.offsetTable = offsetTable;
	}
}
