package com.mawen.learn.rocketmq.remoting.protocol.statictopic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class TopicRemappingDetailWrapper extends RemotingSerializable {

	public static final String TYPE_CREATE_OR_UPDATE = "CREATE_OR_UPDATE";

	public static final String TYPE_REMAPPING = "REMAPPING";

	public static final String SUFFIX_BEFORE = ".before";

	public static final String SUFFIX_AFTER = ".after";


	private String topic;

	private String type;

	private long epoch;

	private Map<String, TopicConfigAndQueueMapping> brokerConfigMap = new HashMap<>();

	private Set<String> brokerInMapIn = new HashSet<>();

	private Set<String> brokerToMapOut = new HashSet<>();

	public TopicRemappingDetailWrapper() {
	}

	public TopicRemappingDetailWrapper(String topic, String type, long epoch, Map<String, TopicConfigAndQueueMapping> brokerConfigMap, Set<String> brokerInMapIn, Set<String> brokerToMapOut) {
		this.topic = topic;
		this.type = type;
		this.epoch = epoch;
		this.brokerConfigMap = brokerConfigMap;
		this.brokerInMapIn = brokerInMapIn;
		this.brokerToMapOut = brokerToMapOut;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public long getEpoch() {
		return epoch;
	}

	public void setEpoch(long epoch) {
		this.epoch = epoch;
	}

	public Map<String, TopicConfigAndQueueMapping> getBrokerConfigMap() {
		return brokerConfigMap;
	}

	public void setBrokerConfigMap(Map<String, TopicConfigAndQueueMapping> brokerConfigMap) {
		this.brokerConfigMap = brokerConfigMap;
	}

	public Set<String> getBrokerInMapIn() {
		return brokerInMapIn;
	}

	public void setBrokerInMapIn(Set<String> brokerInMapIn) {
		this.brokerInMapIn = brokerInMapIn;
	}

	public Set<String> getBrokerToMapOut() {
		return brokerToMapOut;
	}

	public void setBrokerToMapOut(Set<String> brokerToMapOut) {
		this.brokerToMapOut = brokerToMapOut;
	}
}
