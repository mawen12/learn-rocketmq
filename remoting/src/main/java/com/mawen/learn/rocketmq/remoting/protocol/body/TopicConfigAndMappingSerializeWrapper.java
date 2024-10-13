package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class TopicConfigAndMappingSerializeWrapper extends TopicConfigSerailizeWrapper{

	private ConcurrentMap<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = new ConcurrentHashMap<>();

	private ConcurrentMap<String, TopicQueueMappingDetail> topicQueueMappingDetailMap = new ConcurrentHashMap<>();

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

	public ConcurrentMap<String,TopicQueueMappingInfo> getTopicQueueMappingInfoMap() {
		return topicQueueMappingInfoMap;
	}

	public void setTopicQueueMappingInfoMap(ConcurrentMap<String,TopicQueueMappingInfo> topicQueueMappingInfoMap) {
		this.topicQueueMappingInfoMap = topicQueueMappingInfoMap;
	}

	public ConcurrentMap<String,TopicQueueMappingDetail> getTopicQueueMappingDetailMap() {
		return topicQueueMappingDetailMap;
	}

	public void setTopicQueueMappingDetailMap(ConcurrentMap<String,TopicQueueMappingDetail> topicQueueMappingDetailMap) {
		this.topicQueueMappingDetailMap = topicQueueMappingDetailMap;
	}

	public DataVersion getMappingDataVersion() {
		return mappingDataVersion;
	}

	public void setMappingDataVersion(DataVersion mappingDataVersion) {
		this.mappingDataVersion = mappingDataVersion;
	}
}
