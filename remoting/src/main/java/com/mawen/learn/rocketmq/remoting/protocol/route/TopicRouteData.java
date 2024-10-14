package com.mawen.learn.rocketmq.remoting.protocol.route;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class TopicRouteData extends RemotingSerializable {

	private String orderTopicConf;

	private List<QueueData> queueDatas;

	private List<BrokerData> brokerDatas;

	private Map<String, List<String>> filterServerTable;

	private Map<String, TopicQueueMappingInfo> topicQueueMappingByBroker;

	public TopicRouteData() {
		this.queueDatas = new ArrayList<>();
		this.brokerDatas = new ArrayList<>();
		this.filterServerTable = new HashMap<>();
	}

	public TopicRouteData(TopicRouteData topicRouteData) {
		this.queueDatas = new ArrayList<>();
		this.brokerDatas = new ArrayList<>();
		this.filterServerTable = new HashMap<>();
		this.orderTopicConf = topicRouteData.orderTopicConf;

		if (topicRouteData.queueDatas != null) {
			this.queueDatas.addAll(topicRouteData.queueDatas);
		}

		if (topicRouteData.brokerDatas != null) {
			this.brokerDatas.addAll(topicRouteData.brokerDatas);
		}

		if (topicRouteData.filterServerTable != null) {
			this.filterServerTable.putAll(topicRouteData.filterServerTable);
		}

		if (topicRouteData.topicQueueMappingByBroker != null) {
			this.topicQueueMappingByBroker = new HashMap<>(topicRouteData.topicQueueMappingByBroker);
		}
	}

	public TopicRouteData cloneTopicRouteData() {
		TopicRouteData topicRouteData = new TopicRouteData();
		topicRouteData.setQueueDatas(new ArrayList<>());
		topicRouteData.setBrokerDatas(new ArrayList<>());
		topicRouteData.setFilterServerTable(new HashMap<>());
		topicRouteData.setOrderTopicConf(this.orderTopicConf);

		topicRouteData.getQueueDatas().addAll(this.queueDatas);
		topicRouteData.getBrokerDatas().addAll(this.brokerDatas);
		topicRouteData.getFilterServerTable().putAll(this.filterServerTable);
		if (this.topicQueueMappingByBroker != null) {
			Map<String, TopicQueueMappingInfo> cloneMap = new HashMap<>(this.topicQueueMappingByBroker);
			topicRouteData.setTopicQueueMappingByBroker(cloneMap);
		}
		return topicRouteData;
	}

	public TopicRouteData deepCloneTopicRouteData() {
		TopicRouteData topicRouteData = new TopicRouteData();
		topicRouteData.setOrderTopicConf(this.orderTopicConf);

		for (QueueData queueData : this.queueDatas) {
			topicRouteData.getQueueDatas().add(new QueueData(queueData));
		}

		for (BrokerData brokerData : this.brokerDatas) {
			topicRouteData.getBrokerDatas().add(new BrokerData(brokerData));
		}

		for (Map.Entry<String, List<String>> entry : this.filterServerTable.entrySet()) {
			topicRouteData.getFilterServerTable().put(entry.getKey(), new ArrayList<>(entry.getValue()));
		}

		if (this.topicQueueMappingByBroker != null) {
			Map<String, TopicQueueMappingInfo> cloneMap = new HashMap<>(this.topicQueueMappingByBroker.size());
			for (Map.Entry<String, TopicQueueMappingInfo> entry : this.getTopicQueueMappingByBroker().entrySet()) {
				TopicQueueMappingInfo topicQueueMappingInfo = new TopicQueueMappingInfo(entry.getValue().getTopic(), entry.getValue().getTotalQueues(), entry.getValue().getBname(), entry.getValue().getEpoch());
				topicQueueMappingInfo.setDirty(entry.getValue().isDirty());
				topicQueueMappingInfo.setScope(entry.getValue().getScope());
				ConcurrentMap<Integer, Integer> concurrentMap = new ConcurrentHashMap<>(entry.getValue().getCurrIdMap());
				topicQueueMappingInfo.setCurrIdMap(concurrentMap);
				cloneMap.put(entry.getKey(), topicQueueMappingInfo);
			}
			topicRouteData.setTopicQueueMappingByBroker(cloneMap);
		}
		return topicRouteData;
	}

	public boolean topicRouteDataChange(TopicRouteData oldData) {
		if (oldData == null) {
			return true;
		}

		TopicRouteData old = new TopicRouteData(oldData);
		TopicRouteData now = new TopicRouteData(this);
		Collections.sort(old.getQueueDatas());
		Collections.sort(old.getBrokerDatas());
		Collections.sort(now.getQueueDatas());
		Collections.sort(now.getBrokerDatas());
		return !old.equals(now);
	}

	public String getOrderTopicConf() {
		return orderTopicConf;
	}

	public void setOrderTopicConf(String orderTopicConf) {
		this.orderTopicConf = orderTopicConf;
	}

	public List<QueueData> getQueueDatas() {
		return queueDatas;
	}

	public void setQueueDatas(List<QueueData> queueDatas) {
		this.queueDatas = queueDatas;
	}

	public List<BrokerData> getBrokerDatas() {
		return brokerDatas;
	}

	public void setBrokerDatas(List<BrokerData> brokerDatas) {
		this.brokerDatas = brokerDatas;
	}

	public Map<String, List<String>> getFilterServerTable() {
		return filterServerTable;
	}

	public void setFilterServerTable(Map<String, List<String>> filterServerTable) {
		this.filterServerTable = filterServerTable;
	}

	public Map<String, TopicQueueMappingInfo> getTopicQueueMappingByBroker() {
		return topicQueueMappingByBroker;
	}

	public void setTopicQueueMappingByBroker(Map<String,TopicQueueMappingInfo> topicQueueMappingByBroker) {
		this.topicQueueMappingByBroker = topicQueueMappingByBroker;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((brokerDatas == null) ? 0 : brokerDatas.hashCode());
		result = prime * result + ((orderTopicConf == null) ? 0 : orderTopicConf.hashCode());
		result = prime * result + ((queueDatas == null) ? 0 : queueDatas.hashCode());
		result = prime * result + ((filterServerTable == null) ? 0 : filterServerTable.hashCode());
		result = prime * result + ((topicQueueMappingByBroker == null) ? 0 : topicQueueMappingByBroker.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TopicRouteData other = (TopicRouteData) obj;
		if (brokerDatas == null) {
			if (other.brokerDatas != null)
				return false;
		} else if (!brokerDatas.equals(other.brokerDatas))
			return false;
		if (orderTopicConf == null) {
			if (other.orderTopicConf != null)
				return false;
		} else if (!orderTopicConf.equals(other.orderTopicConf))
			return false;
		if (queueDatas == null) {
			if (other.queueDatas != null)
				return false;
		} else if (!queueDatas.equals(other.queueDatas))
			return false;
		if (filterServerTable == null) {
			if (other.filterServerTable != null)
				return false;
		} else if (!filterServerTable.equals(other.filterServerTable))
			return false;
		if (topicQueueMappingByBroker == null) {
			if (other.topicQueueMappingByBroker != null)
				return false;
		} else if (!topicQueueMappingByBroker.equals(other.topicQueueMappingByBroker))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TopicRouteData [orderTopicConf=" + orderTopicConf + ", queueDatas=" + queueDatas
				+ ", brokerDatas=" + brokerDatas + ", filterServerTable=" + filterServerTable + ", topicQueueMappingInfoTable=" + topicQueueMappingByBroker + "]";
	}
}
