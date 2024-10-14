package com.mawen.learn.rocketmq.remoting.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.body.ClusterInfo;
import com.mawen.learn.rocketmq.remoting.protocol.route.BrokerData;
import com.mawen.learn.rocketmq.remoting.protocol.route.TopicRouteData;
import com.mawen.learn.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import com.mawen.learn.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class ClientMetadata {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

	private final ConcurrentMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();

	private final ConcurrentMap<String, ConcurrentMap<MessageQueue, String>> topicEndPointsTable = new ConcurrentHashMap<>();

	private final ConcurrentMap<String, Map<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();

	private final ConcurrentMap<String, Map<String, Integer>> brokerVersionTable = new ConcurrentHashMap<>();

	public static ConcurrentMap<MessageQueue, String> topicRouteData2EndpointForStaticTopic(String topic, TopicRouteData route) {
		if (route.getTopicQueueMappingByBroker() == null || route.getTopicQueueMappingByBroker().isEmpty()) {
			return new ConcurrentHashMap<>();
		}

		ConcurrentMap<MessageQueue, String> mqEndPointSOfBroker = new ConcurrentHashMap<>();
		Map<String, Map<String, TopicQueueMappingInfo>> mappingInfosByScope = new HashMap<>();
		for (Map.Entry<String, TopicQueueMappingInfo> entry : route.getTopicQueueMappingByBroker().entrySet()) {
			TopicQueueMappingInfo info = entry.getValue();
			String scope = info.getScope();

			if (scope != null) {
				if (!mappingInfosByScope.containsKey(scope)) {
					mappingInfosByScope.put(scope, new HashMap<>());
				}
				mappingInfosByScope.get(scope).put(entry.getKey(), entry.getValue());
			}
		}

		for (Map.Entry<String, Map<String, TopicQueueMappingInfo>> mapEntry : mappingInfosByScope.entrySet()) {
			String scope = mapEntry.getKey();
			Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = mapEntry.getValue();
			ConcurrentMap<MessageQueue, TopicQueueMappingInfo> mqEndPoints = new ConcurrentHashMap<>();
			List<Map.Entry<String, TopicQueueMappingInfo>> mappingInfos = new ArrayList<>(topicQueueMappingInfoMap.entrySet());
			mappingInfos.sort((o1, o2) -> (int) (o2.getValue().getEpoch() - o1.getValue().getEpoch()));
			int maxTotalNums = 0;
			long maxTotalNumOfEpoch = -1;

			for (Map.Entry<String, TopicQueueMappingInfo> entry : mappingInfos) {
				TopicQueueMappingInfo info = entry.getValue();
				if (info.getEpoch() >= maxTotalNumOfEpoch && info.getTotalQueues() > maxTotalNums) {
					maxTotalNums = info.getTotalQueues();
				}

				for (Map.Entry<Integer, Integer> idEntry : entry.getValue().getCurrIdMap().entrySet()) {
					Integer globalId = idEntry.getKey();
					MessageQueue mq = new MessageQueue(topic, TopicQueueMappingUtils.getMockBrokerName(info.getScope()), globalId);
					TopicQueueMappingInfo oldInfo = mqEndPoints.get(mq);
					if (oldInfo == null || oldInfo.getEpoch() <= info.getEpoch()) {
						mqEndPoints.put(mq, info);
					}
				}
			}

			for (int i = 0; i < maxTotalNums; i++) {
				MessageQueue mq = new MessageQueue(topic, TopicQueueMappingUtils.getMockBrokerName(scope), i);
				if (!mqEndPoints.containsKey(mq)) {
					mqEndPointSOfBroker.put(mq, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST);
				}
				else {
					mqEndPointSOfBroker.put(mq, mqEndPoints.get(mq).getBname());
				}
			}
		}
		return mqEndPointSOfBroker;
	}

	public void freshTopicRoute(String topic, TopicRouteData topicRouteData) {
		if (topic == null || topicRouteData == null) {
			return;
		}

		TopicRouteData old = this.topicRouteTable.get(topic);
		if (!topicRouteData.topicRouteDataChange(old)) {
			return;
		}

		for (BrokerData bd : topicRouteData.getBrokerDatas()) {
			this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
		}

		ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointForStaticTopic(topic, topicRouteData);
		if (mqEndPoints != null && !mqEndPoints.isEmpty()) {
			topicEndPointsTable.put(topic, mqEndPoints);
		}
	}

	public String getBrokerNameFromMessageQueue(MessageQueue mq) {
		if (topicEndPointsTable.get(mq.getTopic()) != null && !topicEndPointsTable.get(mq.getTopic()).isEmpty()) {
			return topicEndPointsTable.get(mq.getTopic()).get(mq);
		}
		return mq.getBrokerName();
	}

	public void refreshClusterInfo(ClusterInfo clusterInfo) {
		if (clusterInfo == null || clusterInfo.getBrokerAddrTable() == null) {
			return;
		}

		for (Map.Entry<String, BrokerData> entry : clusterInfo.getBrokerAddrTable().entrySet()) {
			brokerAddrTable.put(entry.getKey(), entry.getValue().getBrokerAddrs());
		}
	}

	public String findMasterBrokerAddr(String brokerName) {
		if (!brokerAddrTable.containsKey(brokerName)) {
			return null;
		}

		return brokerAddrTable.get(brokerName).get(MixAll.MASTER_ID);
	}

	public ConcurrentMap<String, TopicRouteData> getTopicRouteTable() {
		return topicRouteTable;
	}

	public ConcurrentMap<String, ConcurrentMap<MessageQueue, String>> getTopicEndPointsTable() {
		return topicEndPointsTable;
	}

	public ConcurrentMap<String, Map<Long, String>> getBrokerAddrTable() {
		return brokerAddrTable;
	}

	public ConcurrentMap<String, Map<String, Integer>> getBrokerVersionTable() {
		return brokerVersionTable;
	}
}
