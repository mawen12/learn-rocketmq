package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.Map;

import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class TopicQueueMappingSerializeWrapper extends RemotingSerializable {

	private Map<String, TopicQueueMappingDetail> topicQueueMappingDetailMap;

	private DataVersion dataVersion = new DataVersion();

	public Map<String,TopicQueueMappingDetail> getTopicQueueMappingDetailMap() {
		return topicQueueMappingDetailMap;
	}

	public void setTopicQueueMappingDetailMap(Map<String,TopicQueueMappingDetail> topicQueueMappingDetailMap) {
		this.topicQueueMappingDetailMap = topicQueueMappingDetailMap;
	}

	public DataVersion getDataVersion() {
		return dataVersion;
	}

	public void setDataVersion(DataVersion dataVersion) {
		this.dataVersion = dataVersion;
	}
}
