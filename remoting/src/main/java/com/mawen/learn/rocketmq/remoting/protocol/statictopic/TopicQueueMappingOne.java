package com.mawen.learn.rocketmq.remoting.protocol.statictopic;

import java.util.List;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class TopicQueueMappingOne extends RemotingSerializable {

	String topic;

	String bname;

	Integer globalId;

	List<LogicQueueMappingItem> items;

	TopicQueueMappingDetail mappingDetail;

	public TopicQueueMappingOne(TopicQueueMappingDetail mappingDetail, String topic, String bname, Integer globalId , List<LogicQueueMappingItem> items) {
		this.mappingDetail = mappingDetail;
		this.items = items;
		this.globalId = globalId;
		this.bname = bname;
		this.topic = topic;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getBname() {
		return bname;
	}

	public void setBname(String bname) {
		this.bname = bname;
	}

	public Integer getGlobalId() {
		return globalId;
	}

	public void setGlobalId(Integer globalId) {
		this.globalId = globalId;
	}

	public List<LogicQueueMappingItem> getItems() {
		return items;
	}

	public void setItems(List<LogicQueueMappingItem> items) {
		this.items = items;
	}

	public TopicQueueMappingDetail getMappingDetail() {
		return mappingDetail;
	}

	public void setMappingDetail(TopicQueueMappingDetail mappingDetail) {
		this.mappingDetail = mappingDetail;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof TopicQueueMappingOne))
			return false;

		TopicQueueMappingOne that = (TopicQueueMappingOne) o;

		if (topic != null ? !topic.equals(that.topic) : that.topic != null)
			return false;
		if (bname != null ? !bname.equals(that.bname) : that.bname != null)
			return false;
		if (globalId != null ? !globalId.equals(that.globalId) : that.globalId != null)
			return false;
		if (items != null ? !items.equals(that.items) : that.items != null)
			return false;
		return mappingDetail != null ? mappingDetail.equals(that.mappingDetail) : that.mappingDetail == null;
	}

	@Override
	public int hashCode() {
		int result = topic != null ? topic.hashCode() : 0;
		result = 31 * result + (bname != null ? bname.hashCode() : 0);
		result = 31 * result + (globalId != null ? globalId.hashCode() : 0);
		result = 31 * result + (items != null ? items.hashCode() : 0);
		result = 31 * result + (mappingDetail != null ? mappingDetail.hashCode() : 0);
		return result;
	}
}
