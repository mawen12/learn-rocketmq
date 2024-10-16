package com.mawen.learn.rocketmq.client;

import java.util.List;

import com.mawen.learn.rocketmq.common.message.MessageExt;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class QueryResult {

	private final long indexLastUdpateTimestamp;

	private final List<MessageExt> messageList;

	public QueryResult(long indexLastUdpateTimestamp, List<MessageExt> messageList) {
		this.indexLastUdpateTimestamp = indexLastUdpateTimestamp;
		this.messageList = messageList;
	}

	public long getIndexLastUdpateTimestamp() {
		return indexLastUdpateTimestamp;
	}

	public List<MessageExt> getMessageList() {
		return messageList;
	}

	@Override
	public String toString() {
		return "QueryResult{" +
				"indexLastUdpateTimestamp=" + indexLastUdpateTimestamp +
				", messageList=" + messageList +
				'}';
	}
}
