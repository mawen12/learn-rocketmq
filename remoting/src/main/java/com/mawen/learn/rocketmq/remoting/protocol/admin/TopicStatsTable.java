package com.mawen.learn.rocketmq.remoting.protocol.admin;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class TopicStatsTable extends RemotingSerializable {

	private ConcurrentMap<MessageQueue, TopicOffset> offsetTable = new ConcurrentHashMap<>();

	public ConcurrentMap<MessageQueue, TopicOffset> getOffsetTable() {
		return offsetTable;
	}

	public void setOffsetTable(ConcurrentMap<MessageQueue, TopicOffset> offsetTable) {
		this.offsetTable = offsetTable;
	}
}
