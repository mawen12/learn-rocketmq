package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class TopicConfigSerializeWrapper extends RemotingSerializable {

	private ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();

	private DataVersion dataVersion = new DataVersion();

	public ConcurrentMap<String, TopicConfig> getTopicConfigTable() {
		return topicConfigTable;
	}

	public void setTopicConfigTable(ConcurrentMap<String, TopicConfig> topicConfigTable) {
		this.topicConfigTable = topicConfigTable;
	}

	public DataVersion getDataVersion() {
		return dataVersion;
	}

	public void setDataVersion(DataVersion dataVersion) {
		this.dataVersion = dataVersion;
	}
}
