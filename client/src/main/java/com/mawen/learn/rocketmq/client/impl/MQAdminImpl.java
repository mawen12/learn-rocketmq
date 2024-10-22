package com.mawen.learn.rocketmq.client.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.mawen.learn.rocketmq.client.Validators;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.route.BrokerData;
import com.mawen.learn.rocketmq.remoting.protocol.route.TopicRouteData;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/22
 */
@RequiredArgsConstructor
public class MQAdminImpl {

	private static final Logger log = LoggerFactory.getLogger(MQClientAPIImpl.class);

	private final MQClientInstance mqClientFactory;

	@Setter
	@Getter
	private long timeoutMillis = 6000;

	public void createTopic(String key, String newTopic, int queueNum) {
		createTopic(key, newTopic, queueNum, 0, null);
	}

	public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag, Map<String, String> attributes) throws MQClientException {
		try {
			Validators.checkTopic(newTopic);
			Validators.isSystemTopic(newTopic);

			TopicRouteData topicRouteData = this.mqClientFactory.getMQClientAPIImpl().getTopicRouteinfoFromNameServer(key, timeoutMillis);
			List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
			if (brokerDataList != null && !brokerDataList.isEmpty()) {
				Collections.sort(brokerDataList);

				boolean createOKAtLeastOnce = false;
				MQClientException exception = null;

				StringBuilder sb = new StringBuilder();

				for (BrokerData brokerData : brokerDataList) {
					String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
					if (masterAddr != null) {
						TopicConfig topicConfig = new TopicConfig(newTopic);
						topicConfig.setReadQueueNums(queueNum);
						topicConfig.setWriteQueueNums(queueNum);
						topicConfig.setTopicSysFlag(topicSysFlag);
						topicConfig.setAttributes(attributes);

						boolean createOK = false;
						for (int i = 0; i < 5; i++) {
							try {
								this.mqClientFactory.getMQClientAPIImpl().createTopic(masterAddr, key, topicConfig, timeoutMillis);
								createOK = true;
								createOKAtLeastOnce = true;
								break;
							}
							catch (Exception e) {
								if (4 == i) {
									exception = new MQClientException("create topic to broker exception", e);
								}
							}
						}

						if (createOK) {
							sb.append(brokerData.getBrokerName());
							sb.append(":");
							sb.append(queueNum);
							sb.append(";");
						}
					}
				}

				if (exception != null && !createOKAtLeastOnce) {
					throw exception;
				}
			}
			else {
				throw new MQClientException("Not found broker, maybe key is wrong", null);
			}
		}
		catch (Exception e) {
			throw new MQClientException("create new topic failed", e);
		}
	}

	public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
		try {

		}
		catch (Exception e) {
			throw new MQClientException("Can not find Message Queue for this topic" + topic, e);
		}

		throw new MQClientException("Unknow why, ");
	}
}
