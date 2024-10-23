package com.mawen.learn.rocketmq.client.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.mawen.learn.rocketmq.client.QueryResult;
import com.mawen.learn.rocketmq.client.Validators;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.client.impl.producer.TopicPublishInfo;
import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.help.FAQUrl;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageDecoder;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageId;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;
import com.mawen.learn.rocketmq.remoting.InvokeCallback;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.netty.ResponseFuture;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryMessageResponseHeader;
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
			TopicRouteData topicRouteData = this.mqClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, timeoutMillis);
			if (topicRouteData != null) {
				TopicPublishInfo topicPublishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);
				if (topicPublishInfo != null && topicPublishInfo.ok()) {
					return parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
				}
			}
		}
		catch (Exception e) {
			throw new MQClientException("Can not find Message Queue for this topic" + topic, e);
		}

		throw new MQClientException("Unknow why, Can not find Message Queue for this topic, " + topic, null);
	}

	public List<MessageQueue> parsePublishMessageQueues(List<MessageQueue> messageQueueList) {
		final String namespace = this.mqClientFactory.getClientConfig().getNamespace();

		return messageQueueList.stream()
				.map(queue -> {
					String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), namespace);
					return new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId());
				})
				.collect(Collectors.toList());
	}

	public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
		try {
			TopicRouteData topicRouteData = this.mqClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, timeoutMillis);
			if (topicRouteData != null) {
				Set<MessageQueue> mqSet = MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
				if (!mqSet.isEmpty()) {
					return mqSet;
				}
				else {
					throw new MQClientException("Can not find Message Queue for this topic, " + topic + " Namesrv return empty", null);
				}
			}
		}
		catch (Exception e) {
			throw new MQClientException("Can not find Message Queue for this topic, " + topic + FAQUrl.suggestTodo(FAQUrl.MQLIST_NOT_EXIST), e);
		}

		throw new MQClientException("Unknown why, Can not find Message Queue for this topic, " + topic, null);
	}

	public long searchOffset(MessageQueue mq, long timestamp) {
		return searchOffset(mq, timestamp, BoundaryType.LOWER);
	}

	public long searchOffset(MessageQueue mq, long timestamp, BoundaryType boundaryType) throws MQClientException {
		String brokerAddr = getBrokerAddrFromMessageQueue(mq);

		if (brokerAddr != null) {
			try {
				return this.mqClientFactory.getMQClientAPIImpl().searchOffset(brokerAddr, mq, timestamp, boundaryType, timeoutMillis);
			}
			catch (Exception e) {
				throw new MQClientException("Invoker Broker[" + brokerAddr + "] exception", e);
			}
		}
		throw new MQClientException("The Broker [" + mq.getBrokerName() + "] not exist", null);
	}

	public long maxOffset(MessageQueue mq) throws MQClientException {
		String brokerAddr = getBrokerAddrFromMessageQueue(mq);

		if (brokerAddr != null) {
			try {
				return this.mqClientFactory.getMQClientAPIImpl().getMaxOffset(brokerAddr, mq, timeoutMillis);
			}
			catch (Exception e) {
				throw new MQClientException("Invoke broker [" + brokerAddr + "] exception", e);
			}
		}
		throw new MQClientException("The broker [" + mq.getBrokerName() + "] not exist", null);
	}

	public long minOffset(MessageQueue mq) throws MQClientException {
		String brokerAddr = getBrokerAddrFromMessageQueue(mq);

		if (brokerAddr != null) {
			try {
				return this.mqClientFactory.getMQClientAPIImpl().getMinOffset(brokerAddr, mq, timeoutMillis);
			}
			catch (Exception e) {
				throw new MQClientException("Invoke broker [" + brokerAddr + "] exception", e);
			}
		}
		throw new MQClientException("The broker [" + mq.getBrokerName() + "] not exist", null);
	}

	public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
		String brokerAddr = getBrokerAddrFromMessageQueue(mq);

		if (brokerAddr != null) {
			try {
				return this.mqClientFactory.getMQClientAPIImpl().getEarliestMsgStoreTime(brokerAddr, mq, timeoutMillis);
			}
			catch (Exception e) {
				throw new MQClientException("Invoke broker [" + brokerAddr + "] exception", e);
			}
		}

		throw new MQClientException("The broker [" + mq.getBrokerName() + "] not exist", null);
	}

	private String getBrokerAddrFromMessageQueue(MessageQueue mq) {
		String brokerAddr = this.mqClientFactory.findBrokerAddressInPublish(this.mqClientFactory.getBrokerNameFromMessageQueue(mq));
		if (brokerAddr == null) {
			this.mqClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
			brokerAddr = this.mqClientFactory.findBrokerAddresInPublish(this.mqClientFactory.getBrokerNameFromMessageQueue(mq));
		}

		return brokerAddr;
	}

	public MessageExt viewMessage(String topic, String msgId) throws MQClientException {
		MessageId messageId;
		try {
			messageId = MessageDecoder.decodeMessageId(msgId);
		}
		catch (Exception e) {
			throw new MQClientException(ResponseCode.NO_MESSAGE, "query message by id finished, but not message");
		}

		return this.mqClientFactory.getMQClientAPIImpl().viewMessage(NetworkUtil.socketAddress2String(messageId.getAddress()), topic, messageId.getOffset(), timeoutMillis);
	}

	public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws InterruptedException, MQClientException {
		return queryMessage(topic, key, maxNum, begin, end, true);
	}

	public QueryResult queryMessageByUniqKey(String topic, String uniqKey, int maxNum, long begin, long end) throws InterruptedException, MQClientException {
		return queryMessage(topic, uniqKey, maxNum, begin, end, false);
	}

	public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws InterruptedException, MQClientException {
		return queryMessageByUniqKey(topic, uniqKey, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3), Long.MAX_VALUE);
	}

	public MessageExt queryMessageByUniqKey(String clusterName, String topic, String uniqKey) throws InterruptedException, MQClientException {
		return queryMessageByUniqKey(clusterName, topic, uniqKey, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3), Long.MAX_VALUE);
	}

	public MessageExt queryMessageByUniqKey(String topic, String uniqKey, long begin, long end) throws InterruptedException, MQClientException {
		return queryMessageByUniqKey(null, topic, uniqKey, begin, end);
	}

	public MessageExt queryMessageByUniqKey(String clusterName, String topic, String uniqKey, long begin, long end) throws InterruptedException, MQClientException {
		QueryResult qr = this.queryMessage(clusterName, topic, uniqKey, 32, begin, end, true);

		return Optional.ofNullable(qr)
				.map(QueryResult::getMessageList)
				.map(list -> list.get(0))
				.orElse(null);
	}

	protected QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end, boolean isUniqKey) throws InterruptedException, MQClientException {
		return queryMessage(null, topic, key, maxNum, begin, end, isUniqKey);
	}

	protected QueryResult queryMessage(String clusterName, String topic, String key, int maxNum, long begin, long end, boolean isUniqKey) throws MQClientException, InterruptedException {
		TopicRouteData topicRouteData = this.mqClientFactory.getAnExistTopicRouteData(topic);
		if (topicRouteData == null) {
			this.mqClientFactory.upateTopicRouteInfoFromNameServer(topic);
			topicRouteData = this.mqClientFactory.getAnExistTopicRouteData(topic);
		}

		if (topicRouteData != null) {
			List<String> brokerAddrs = new ArrayList<>();

			if (clusterName != null && !clusterName.isEmpty()) {
				for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
					if (clusterName.equals(brokerData.getCluster())) {
						brokerAddrs.add(brokerData.selectBrokerAddr());
					}
				}
			}

			if (!brokerAddrs.isEmpty()) {
				final CountDownLatch countDownLatch = new CountDownLatch(brokerAddrs.size());
				final List<QueryResult> queryResultList = new LinkedList<>();
				final ReadWriteLock lock = new ReentrantReadWriteLock(false);

				for (String addr : brokerAddrs) {
					try {
						QueryMessageRequestHeader requestHeader = new QueryMessageRequestHeader();
						requestHeader.setTopic(topic);
						requestHeader.setTopic(key);
						requestHeader.setMaxNum(maxNum);
						requestHeader.setBeginTimestamp(begin);
						requestHeader.setEndTimestamp(end);

						this.mqClientFactory.getMQClientAPIImpl().queryMessage(addr, requestHeader, timeoutMillis * 3, new InvokeCallback() {
							@Override
							public void operationComplete(ResponseFuture responseFuture) {
							}

							@Override
							public void operationSucceed(RemotingCommand response) {
								try {
									switch (response.getCode()) {
										case ResponseCode.SUCCESS:
											QueryMessageResponseHeader responseHeader;
											try {
												responseHeader = response.decodeCommandCustomHeader(QueryMessageResponseHeader.class);
											}
											catch (RemotingCommandException e) {
												log.error("decodeCommandCustomHeader exception", e);
												return;
											}

											List<MessageExt> wrappers = MessageDecoder.decodes(ByteBuffer.wrap(response.getBody()), true);

											QueryResult qr = new QueryResult(responseHeader.getIndexLastUpdateTimestamp(), wrappers);
											try {
												lock.writeLock().lock();
												queryResultList.add(qr);
											}
											finally {
												lock.writeLock().unlock();
											}
											break;
										default:
											log.warn("getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
											break;
									}
								}
								finally {
									countDownLatch.countDown();
								}
							}

							@Override
							public void operationFail(Throwable throwable) {
								log.error("queryMessage error, requestHeader={}", requestHeader);
								countDownLatch.countDown();
							}
						}, isUniqKey);
					}
					catch (Exception e) {
						log.warn("queryMessage exception", e);
					}
				}

				boolean ok = countDownLatch.await(timeoutMillis * 4, TimeUnit.MILLISECONDS);
				if (!ok) {
					log.warn("query message, maybe some broker failed");
				}

				long indexLastUpdateTimestamp = 0;
				List<MessageExt> messageList = new LinkedList<>();
				for (QueryResult qr : queryResultList) {
					if (qr.getIndexLastUdpateTimestamp() > indexLastUpdateTimestamp) {
						indexLastUpdateTimestamp = qr.getIndexLastUdpateTimestamp();
					}

					for (MessageExt messageExt : qr.getMessageList()) {
						if (isUniqKey) {
							if (messageExt.getMsgId().equals(key)) {
								messageList.add(messageExt);
							}
							else {
								log.warn("queryMessage by uniqKey, find message key not matched, maybe hash duplicate {}", messageExt);
							}
						}
						else {
							String keys = messageExt.getKeys();
							String msgTopic = messageExt.getTopic();
							if (keys != null) {
								boolean matched = false;
								boolean topicEquals = Objects.equals(topic, msgTopic);
								String[] keyArray = keys.split(MessageConst.KEY_SEPARATOR);
								for (String k : keyArray) {
									if (Objects.equals(k, key)) {
										matched = true;
										break;
									}
								}

								if (matched) {
									messageList.add(messageExt);
								}
								else {
									log.warn("queryMessage, find message key not matched, maybe hash duplicate {}", messageExt);
								}
							}
						}
					}

					String namespace = this.mqClientFactory.getClientConfig().getNamespace();
					if (namespace != null) {
						for (MessageExt messageExt : messageList) {
							messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), namespace));
						}
					}

					if (!messageList.isEmpty()) {
						return new QueryResult(indexLastUpdateTimestamp, messageList);
					}
					else {
						throw new MQClientException(ResponseCode.NO_MESSAGE, "query message by key finished, but no message.");
					}
				}
			}
		}

		throw new MQClientException(ResponseCode.TOPIC_NOT_EXIST, "The topic[" + topic + "] not matched route info");
	}
}
