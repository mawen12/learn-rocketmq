package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class GetConsumerStatusBody extends RemotingSerializable {

	private Map<MessageQueue, Long> messageQueueTable = new HashMap<>();

	private Map<String, Map<MessageQueue, Long>> consumerTable = new HashMap<>();

	public Map<MessageQueue, Long> getMessageQueueTable() {
		return messageQueueTable;
	}

	public void setMessageQueueTable(Map<MessageQueue, Long> messageQueueTable) {
		this.messageQueueTable = messageQueueTable;
	}

	public Map<String, Map<MessageQueue, Long>> getConsumerTable() {
		return consumerTable;
	}

	public void setConsumerTable(Map<String, Map<MessageQueue, Long>> consumerTable) {
		this.consumerTable = consumerTable;
	}
}
