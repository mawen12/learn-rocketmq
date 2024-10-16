package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;
import com.mawen.learn.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import com.mawen.learn.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class TopicConfigAndMappingSerializeWrapper extends TopicConfigSerializeWrapper{

	private Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = new ConcurrentHashMap<>();

	private Map<String, TopicQueueMappingDetail> topicQueueMappingDetailMap = new ConcurrentHashMap<>();

	private DataVersion mappingDataVersion = new DataVersion();

	public static TopicConfigAndMappingSerializeWrapper from(TopicConfigAndMappingSerializeWrapper wrapper) {
		if (wrapper instanceof TopicConfigAndMappingSerializeWrapper) {
			return wrapper;
		}

		TopicConfigAndMappingSerializeWrapper mappingSerializeWrapper = new TopicConfigAndMappingSerializeWrapper();
		mappingSerializeWrapper.setMappingDataVersion(wrapper.getMappingDataVersion());
		mappingSerializeWrapper.setTopicConfigTable(wrapper.getTopicConfigTable());
		return mappingSerializeWrapper;
	}

	public Map<String,TopicQueueMappingInfo> getTopicQueueMappingInfoMap() {
		return topicQueueMappingInfoMap;
	}

	public void setTopicQueueMappingInfoMap(Map<String,TopicQueueMappingInfo> topicQueueMappingInfoMap) {
		this.topicQueueMappingInfoMap = topicQueueMappingInfoMap;
	}

	public Map<String,TopicQueueMappingDetail> getTopicQueueMappingDetailMap() {
		return topicQueueMappingDetailMap;
	}

	public void setTopicQueueMappingDetailMap(Map<String,TopicQueueMappingDetail> topicQueueMappingDetailMap) {
		this.topicQueueMappingDetailMap = topicQueueMappingDetailMap;
	}

	public DataVersion getMappingDataVersion() {
		return mappingDataVersion;
	}

	public void setMappingDataVersion(DataVersion mappingDataVersion) {
		this.mappingDataVersion = mappingDataVersion;
	}
}
