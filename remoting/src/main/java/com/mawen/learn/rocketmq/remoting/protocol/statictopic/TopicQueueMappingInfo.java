package com.mawen.learn.rocketmq.remoting.protocol.statictopic;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class TopicQueueMappingInfo extends RemotingSerializable {

	public static final int LEVEL_0 = 0;

	String topic;

	String scope = MixAll.METADATA_SCOPE_GLOBAL;

	int totalQueues;

	String bname;

	long epoch;

	boolean dirty;

	protected ConcurrentMap<Integer, Integer> currIdMap = new ConcurrentHashMap<>();

	public TopicQueueMappingInfo() {
	}

	public TopicQueueMappingInfo(String topic, int totalQueues, String bname, long epoch) {
		this.topic = topic;
		this.totalQueues = totalQueues;
		this.bname = bname;
		this.epoch = epoch;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getScope() {
		return scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}

	public int getTotalQueues() {
		return totalQueues;
	}

	public void setTotalQueues(int totalQueues) {
		this.totalQueues = totalQueues;
	}

	public String getBname() {
		return bname;
	}

	public void setBname(String bname) {
		this.bname = bname;
	}

	public long getEpoch() {
		return epoch;
	}

	public void setEpoch(long epoch) {
		this.epoch = epoch;
	}

	public boolean isDirty() {
		return dirty;
	}

	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}

	public ConcurrentMap<Integer, Integer> getCurrIdMap() {
		return currIdMap;
	}

	public void setCurrIdMap(ConcurrentMap<Integer, Integer> currIdMap) {
		this.currIdMap = currIdMap;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof TopicQueueMappingInfo)) return false;

		TopicQueueMappingInfo info = (TopicQueueMappingInfo) o;

		if (totalQueues != info.totalQueues) return false;
		if (epoch != info.epoch) return false;
		if (dirty != info.dirty) return false;
		if (topic != null ? !topic.equals(info.topic) : info.topic != null) return false;
		if (scope != null ? !scope.equals(info.scope) : info.scope != null) return false;
		if (bname != null ? !bname.equals(info.bname) : info.bname != null) return false;
		return currIdMap != null ? currIdMap.equals(info.currIdMap) : info.currIdMap == null;
	}

	@Override
	public int hashCode() {
		int result = topic != null ? topic.hashCode() : 0;
		result = 31 * result + (scope != null ? scope.hashCode() : 0);
		result = 31 * result + totalQueues;
		result = 31 * result + (bname != null ? bname.hashCode() : 0);
		result = 31 * result + (int) (epoch ^ (epoch >>> 32));
		result = 31 * result + (dirty ? 1 : 0);
		result = 31 * result + (currIdMap != null ? currIdMap.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "TopicQueueMappingInfo{" +
				"topic='" + topic + '\'' +
				", scope='" + scope + '\'' +
				", totalQueues=" + totalQueues +
				", bname='" + bname + '\'' +
				", epoch=" + epoch +
				", dirty=" + dirty +
				", currIdMap=" + currIdMap +
				'}';
	}
}
