package com.mawen.learn.rocketmq.client.impl.factory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.fastjson.JSON;
import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.admin.MQAdminExtInner;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.impl.ClientRemotingProcessor;
import com.mawen.learn.rocketmq.client.impl.FindBrokerResult;
import com.mawen.learn.rocketmq.client.impl.MQAdminImpl;
import com.mawen.learn.rocketmq.client.impl.MQClientAPIImpl;
import com.mawen.learn.rocketmq.client.impl.MQClientManager;
import com.mawen.learn.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import com.mawen.learn.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.mawen.learn.rocketmq.client.impl.consumer.MQConsumerInner;
import com.mawen.learn.rocketmq.client.impl.consumer.ProcessQueue;
import com.mawen.learn.rocketmq.client.impl.consumer.PullMessageService;
import com.mawen.learn.rocketmq.client.impl.consumer.RebalanceService;
import com.mawen.learn.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.mawen.learn.rocketmq.client.impl.producer.MQProducerInner;
import com.mawen.learn.rocketmq.client.impl.producer.TopicPublishInfo;
import com.mawen.learn.rocketmq.client.producer.DefaultMQProducer;
import com.mawen.learn.rocketmq.client.stat.ConsumerStatsManager;
import com.mawen.learn.rocketmq.common.MQVersion;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.ServiceState;
import com.mawen.learn.rocketmq.common.constant.PermName;
import com.mawen.learn.rocketmq.common.filter.ExpressionType;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.message.MessageQueueAssignment;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.remoting.ChannelEventListener;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.common.HeartbeatV2Result;
import com.mawen.learn.rocketmq.remoting.exception.RemotingConnectException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.netty.NettyClientConfig;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ConsumerData;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ProducerData;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import com.mawen.learn.rocketmq.remoting.protocol.route.BrokerData;
import com.mawen.learn.rocketmq.remoting.protocol.route.QueueData;
import com.mawen.learn.rocketmq.remoting.protocol.route.TopicRouteData;
import com.mawen.learn.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import com.mawen.learn.rocketmq.remoting.protocol.statictopic.TopicQueueMappingUtils;
import io.netty.channel.Channel;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/21
 */
@Getter
@Setter
public class MQClientInstance {

	private static final Logger log = LoggerFactory.getLogger(MQClientInstance.class);

	private static final long LOCK_TIMEOUT_MILLIS = 3000;

	private final ClientConfig clientConfig;
	private final String clientId;
	private final NettyClientConfig nettyClientConfig;
	private final MQClientAPIImpl mqClientAPIImpl;
	private final MQAdminImpl mqAdminImpl;
	private final ConcurrentMap<String, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<>();
	private final long bootTimestamp = System.currentTimeMillis();
	private final ConcurrentMap<String, MQProducerInner> producerTable = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, MQConsumerInner> consumerTable = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, ConcurrentMap<MessageQueue, String>> topicEndPointsTable = new ConcurrentHashMap<>();
	private final Lock lockNamesrv = new ReentrantLock();
	private final Lock lockHeartbeat = new ReentrantLock();
	private final ConcurrentMap<String, Map<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, Map<String, Integer>> brokerVersionTable = new ConcurrentHashMap<>();
	private final Set<String> brokerSupportV2HeartbeatSet = new HashSet<>();
	private final ConcurrentMap<String, Integer> brokerAddrHeartbeatFingerprintTable = new ConcurrentHashMap<>();
	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryScheduledThread"));
	private final ScheduledExecutorService scheduledRemoteConfigExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryFetchRemoteConfigScheduledThread"));

	private final PullMessageService pullMessageService;
	private final RebalanceService rebalanceService;
	private final DefaultMQProducer defaultMQProducer;
	private final ConsumerStatsManager consumerStatsManager;
	private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);
	private ServiceState serviceState = ServiceState.CREATE_JUST;
	private final Random random = new Random();

	public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
		this(clientConfig, instanceIndex, clientId, null);
	}

	public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
		this.clientConfig = clientConfig;
		this.nettyClientConfig = new NettyClientConfig();
		this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
		this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());
		this.nettyClientConfig.setSocksProxyConfig(clientConfig.getSocksProxyConfig());

		ClientRemotingProcessor clientRemotingProcessor = new ClientRemotingProcessor(this);
		ChannelEventListener channelEventListener;
		if (clientConfig.isEnableHeartbeatChannelEventListener()) {
			channelEventListener = new ChannelEventListener() {

				private final ConcurrentMap<String, Map<Long, String>> brokerAddrTable = MQClientInstance.this.brokerAddrTable;

				@Override
				public void onChannelConnect(String remoteAddr, Channel channel) {
				}

				@Override
				public void onChannelClose(String remoteAddr, Channel channel) {
				}

				@Override
				public void onChannelException(String remoteAddr, Channel channel) {
				}

				@Override
				public void onChannelIdle(String remoteAddr, Channel channel) {
				}

				@Override
				public void onChannelActive(String remoteAddr, Channel channel) {
					for (Map.Entry<String, Map<Long, String>> addressEntry : brokerAddrTable.entrySet()) {
						for (Map.Entry<Long, String> entry : addressEntry.getValue().entrySet()) {
							String addr = entry.getValue();
							if (addr.equals(remoteAddr)) {
								long id = entry.getKey();
								String brokerName = addressEntry.getKey();

								if (sendHeartbeatToBroker(id, brokerName, addr)) {
									rebalanceImmediately();
								}
								break;
							}
						}
					}
				}
			};
		}
		else {
			channelEventListener = null;
		}
		this.mqClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, clientRemotingProcessor, rpcHook, clientConfig, channelEventListener);

		if (this.clientConfig.getNamesrvAddr() != null) {
			this.mqClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
			log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
		}

		this.clientId = clientId;
		this.mqAdminImpl = new MQAdminImpl(this);
		this.pullMessageService = new PullMessageService(this);
		this.rebalanceService = new RebalanceService(this);

		this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
		this.defaultMQProducer.resetClientConfig(clientConfig);

		this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

		log.info("Created a new client instance, InstanceIndex:{}, ClientID:{}, ClientConfig:{}, ClientVersion:{}, SerializerType:{}",
				instanceIndex, this.clientId, this.clientConfig, MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
	}

	public static TopicPublishInfo topicRouteData2TopicPublishInfo(String topic, TopicRouteData routeData) {
		TopicPublishInfo info = new TopicPublishInfo();

		info.setTopicRouteData(routeData);

		if (StringUtils.isNotEmpty(routeData.getOrderTopicConf())) {
			String[] brokers = routeData.getOrderTopicConf().split(";");
			for (String broker : brokers) {
				String[] brokerNameAndNums = broker.split(":");
				int nums = Integer.parseInt(brokerNameAndNums[1]);
				for (int i = 0; i < nums; i++) {
					MessageQueue mq = new MessageQueue(topic, brokerNameAndNums[0], i);
					info.getMessageQueueList().add(mq);
				}
			}

			info.setOrderTopic(true);
		}
		else if (MapUtils.isNotEmpty(routeData.getTopicQueueMappingByBroker())) {
			info.setOrderTopic(false);

			ConcurrentMap<MessageQueue, String> mqEndpoints = topicRouteData2EndpointsForStaticTopic(topic, routeData);
			info.getMessageQueueList().addAll(mqEndpoints.keySet());
			info.getMessageQueueList().sort((mq1, mq2) -> MixAll.compareInteger(mq1.getQueueId(), mq2.getQueueId()));
		}
		else {
			List<QueueData> queueDatas = routeData.getQueueDatas();
			Collections.sort(queueDatas);

			for (QueueData queueData : queueDatas) {
				if (PermName.isWriteable(queueData.getPerm())) {
					BrokerData brokerData = routeData.getBrokerDatas().stream().filter(data -> data.getBrokerName().equals(queueData.getBrokerName())).findFirst().orElse(null);

					if (brokerData == null) {
						continue;
					}

					if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
						continue;
					}

					for (int i = 0; i < queueData.getWriteQueueNums(); i++) {
						MessageQueue mq = new MessageQueue(topic, queueData.getBrokerName(), i);
						info.getMessageQueueList().add(mq);
					}
				}
			}

			info.setOrderTopic(false);
		}

		return info;
	}

	public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(String topic, TopicRouteData routeData) {
		Set<MessageQueue> mqList = new HashSet<>();
		if (MapUtils.isNotEmpty(routeData.getTopicQueueMappingByBroker())) {
			ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, routeData);
			return mqEndPoints.keySet();
		}

		List<QueueData> queueDatas = routeData.getQueueDatas();
		for (QueueData queueData : queueDatas) {
			if (PermName.isReadable(queueData.getPerm())) {
				for (int i = 0; i < queueData.getReadQueueNums(); i++) {
					MessageQueue mq = new MessageQueue(topic, queueData.getBrokerName(), i);
					mqList.add(mq);
				}
			}
		}

		return mqList;
	}

	public static ConcurrentMap<MessageQueue, String> topicRouteData2EndpointsForStaticTopic(final String topic, final TopicRouteData route) {
		if (route.getTopicQueueMappingByBroker() == null || route.getTopicQueueMappingByBroker().isEmpty()) {
			return new ConcurrentHashMap<>();
		}

		ConcurrentMap<MessageQueue, String> mqEndPointsOfBroker = new ConcurrentHashMap<>();
		Map<String, Map<String, TopicQueueMappingInfo>> mappingInfoByScope = new HashMap<>();

		for (Map.Entry<String, TopicQueueMappingInfo> entry : route.getTopicQueueMappingByBroker().entrySet()) {
			TopicQueueMappingInfo info = entry.getValue();
			String scope = info.getScope();
			if (scope != null) {
				mappingInfoByScope.computeIfAbsent(scope, k -> new HashMap<>())
						.put(entry.getKey(), entry.getValue());
			}
		}

		for (Map.Entry<String, Map<String, TopicQueueMappingInfo>> mapEntry : mappingInfoByScope.entrySet()) {
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
					mqEndPointsOfBroker.put(mq, MixAll.LOGICAL_QUEUE_MOCK_BROKER_NAME_NOT_EXIST);
				}
				else {
					mqEndPointsOfBroker.put(mq, mqEndPoints.get(mq).getBname());
				}
			}
		}

		return mqEndPointsOfBroker;
	}

	public void start() throws MQClientException {
		synchronized (this) {
			switch (this.serviceState) {
				case CREATE_JUST:
					this.serviceState = ServiceState.START_FAILED;

					if (this.clientConfig.getNamesrvAddr() == null) {
						this.mqClientAPIImpl.fetchNameServerAddr();
					}

					this.mqClientAPIImpl.start();
					this.startScheduledTask();
					this.pullMessageService.start();
					this.rebalanceService.start();
					this.defaultMQProducer.getDefaultMQProducerImpl().start(false);

					log.info("the client factory [{}] start OK", this.clientId);

					this.serviceState = ServiceState.RUNNING;
					break;
				case START_FAILED:
					throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
				default:
					break;
			}
		}
	}

	public void checkClientInBroker() throws MQClientException {
		for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
			Set<SubscriptionData> subscriptionInner = entry.getValue().subscriptions();
			if (subscriptionInner == null || subscriptionInner.isEmpty()) {
				return;
			}

			for (SubscriptionData subscriptionData : subscriptionInner) {
				if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
					continue;
				}

				String addr = findBrokerAddrByTopic(subscriptionData.getTopic());
				if (addr != null) {
					try {
						this.mqClientAPIImpl.checkClientInBroker(addr, entry.getKey(), this.clientId, subscriptionData, clientConfig.getMqClientApiTimeout());
					}
					catch (Exception e) {
						if (e instanceof MQClientException) {
							throw (MQClientException)e;
						}
						else {
							throw new MQClientException("Check client in broker error, maybe because you use" + subscriptionData.getExpressionType()
									+ " to filter message, but server has not been upgraded to to support! This error would not affect the launch of consumer,"
									+ "but may has impact on message receiving if you have use the new features which are not supported by server, please check the log!", e);
						}
					}
				}
			}
		}
	}

	public boolean sendHeartbeatToAllBrokerWithLockV2(boolean isRebalance) {
		if (this.lockHeartbeat.tryLock()) {
			try {
				if (clientConfig.isUseHeartbeatV2()) {
					return this.sendHeartbeatToAllBrokerV2(isRebalance);
				}
				else {
					return this.sendHeartbeatToAllBroker();
				}
			}
			catch (Exception e) {
				log.error("sendHeartbeatToAllBrokerWithLockV2 exception", e);
			}
			finally {
				this.lockHeartbeat.unlock();
			}
		}
		else {
			log.warn("sendHeartbeatToAllBrokerWithLockV2 lock heartBeat, but failed. [{}]", this.clientId);
		}
		return false;
	}

	public boolean sendHeartbeatToAllBrokerWithLock() {
		if (this.lockHeartbeat.tryLock()) {
			try {
				if (clientConfig.isUseHeartbeatV2()) {
					return this.sendHeartbeatToAllBrokerV2(false);
				}
				else {
					return this.sendHeartbeatToAllBroker();
				}
			}
			catch (Exception e) {
				log.error("sendHeartbeatToAllBrokerWithLock exception", e);
			}
			finally {
				this.lockHeartbeat.unlock();
			}
		}
		else {
			log.warn("lock heartBeat, but failed. [{}]", this.clientId);
		}
		return false;
	}

	public void adjustThreadPool() {
		for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
			MQConsumerInner impl = entry.getValue();
			if (impl != null) {
				try {
					if (impl instanceof DefaultMQPushConsumerImpl) {
						DefaultMQPushConsumerImpl defaultMQPushConsumer = (DefaultMQPushConsumerImpl) impl;
						defaultMQPushConsumer.adjustThreadPool();
					}
				}catch (Exception ignored) {}
			}
		}
	}

	public boolean sendHeartbeatToBroker(long id, String brokerName, String addr) {
		if (this.lockHeartbeat.tryLock()) {
			HeartbeatData heartbeatData = this.prepareHeartbeatData(false);
			boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
			boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
			if (producerEmpty && consumerEmpty) {
				log.warn("sendHeartbeatToBroker sending heartbeat, but no producer and no consumer. [{}]", this.clientId);
				return false;
			}

			try {
				if (clientConfig.isUseHeartbeatV2()) {
					int currentHeartbeatFingerprint = heartbeatData.computeHeartbeatFingerprint();
					heartbeatData.setHeartbeatFingerprint(currentHeartbeatFingerprint);
					HeartbeatData heartbeatDataWithoutSub = this.prepareHeartbeatData(true);
					heartbeatDataWithoutSub.setHeartbeatFingerprint(currentHeartbeatFingerprint);

					return this.sendHeartbeatToBrokerV2(id, brokerName, addr, heartbeatData, heartbeatDataWithoutSub, currentHeartbeatFingerprint);
				}
				else {
					return this.sendHeartbeatToBroker(id, brokerName, addr, heartbeatData);
				}
			}
			catch (Exception e) {
				log.error("sendHeartbeatToBroker exception",e);
			}
			finally {
				this.lockHeartbeat.unlock();
			}
		}
		else {
			log.warn("lock heartBeat, but failed, [{}]", this.clientId);
		}
		return false;
	}

	public void updateTopicRouteInfoFromNameServer() {
		Set<String> topicList = new HashSet<>();

		this.consumerTable.values().stream()
				.filter(Objects::nonNull)
				.map(MQConsumerInner::subscriptions)
				.filter(Objects::nonNull)
				.flatMap(Collection::stream)
				.map(SubscriptionData::getTopic)
				.forEach(topicList::add);

		this.producerTable.values().stream()
				.filter(Objects::nonNull)
				.map(MQProducerInner::getPublishTopicList)
				.forEach(topicList::addAll);

		for (String topic : topicList) {
			this.updateTopicRouteInfoFromNameServer(topic);
		}
	}

	public boolean updateTopicRouteInfoFromNameServer(String topic) {
		return this.updateTopicRouteInfoFromNameServer(topic, false, null);
	}

	public boolean updateTopicRouteInfoFromNameServer(String topic, boolean isDefault, DefaultMQProducer defaultMQProducer) {
		try {
			if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
				try {
					TopicRouteData topicRouteData;
					if (isDefault && defaultMQProducer != null) {
						topicRouteData = this.mqClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(clientConfig.getMqClientApiTimeout());
						if (topicRouteData != null) {
							for (QueueData data : topicRouteData.getQueueDatas()) {
								int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
								data.setReadQueueNums(queueNums);
								data.setWriteQueueNums(queueNums);
							}
						}
					}
					else {
						topicRouteData = this.mqClientAPIImpl.getTopicRouteInfoFromNameServer(topic, clientConfig.getMqClientApiTimeout());
					}

					if (topicRouteData != null) {
						TopicRouteData old = this.topicRouteTable.get(topic);
						boolean changed = topicRouteData.topicRouteDataChange(old);
						if (!changed) {
							changed = this.isNeedUpdateTopicRouteInfo(topic);
						}
						else {
							log.info("the topic[{}] route info changed, old[{}], new[{}]", topic, old, topicRouteData);
						}

						if (changed) {
							for (BrokerData bd : topicRouteData.getBrokerDatas()) {
								this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
							}

							ConcurrentMap<MessageQueue, String> mqEndpoints = topicRouteData2EndpointsForStaticTopic(topic, topicRouteData);
							if (!mqEndpoints.isEmpty()) {
								topicEndPointsTable.put(topic, mqEndpoints);
							}

							TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
							publishInfo.setHaveTopicRouterInfo(true);
							for (Map.Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
								MQProducerInner impl = entry.getValue();
								if (impl != null) {
									impl.updateTopicPublishInfo(topic, publishInfo);
								}
							}

							if (!consumerTable.isEmpty()) {
								Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
								for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
									MQConsumerInner impl = entry.getValue();
									if (impl != null) {
										impl.updateTopicSubscribeInfo(topic, subscribeInfo);
									}
								}
							}

							TopicRouteData cloneTopicRouteData = new TopicRouteData(topicRouteData);
							log.info("topicRouteTable.put. Topic = {}, TopicRouteData{}", topic, cloneTopicRouteData);
							this.topicRouteTable.put(topic, cloneTopicRouteData);
							return true;
						}
						else {
							log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}. [{}]", topic, this.clientId);
						}
					}
				}
				catch (MQClientException e) {
					if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
						log.warn("updateTopicRouteInfoFromNameServer Exception", e);
					}
				}
				catch (RemotingException e) {
					log.error("updateTopicRouteInfoFromNameServer Exception", e);
					throw new IllegalStateException(e);
				}
				finally {
					this.lockNamesrv.unlock();
				}
			}
			else {
				log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms. [{}]", LOCK_TIMEOUT_MILLIS, this.clientId);
			}
		}
		catch (InterruptedException e) {
			log.warn("updateTopicRouteInfoFromNameServer exception", e);
		}

		return false;
	}

	public void shutdown() {
		if (!this.consumerTable.isEmpty()) {
			return;
		}
		if (!this.adminExtTable.isEmpty()) {
			return;
		}
		if (this.producerTable.size() > 1) {
			return;
		}

		synchronized (this) {
			switch (this.serviceState) {
				case RUNNING:
					this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);

					this.serviceState = ServiceState.SHUTDOWN_ALREADY;
					this.pullMessageService.shutdown(true);
					this.scheduledExecutorService.shutdown();
					this.mqClientAPIImpl.shutdown();
					this.rebalanceService.shutdown();

					MQClientManager.getInstance().removeClientFactory(this.clientId);
					log.info("the client factory [{}] shutdown ok", this.clientId);
					break;
				case CREATE_JUST:
				case SHUTDOWN_ALREADY:
				default:
					break;
			}
		}
	}

	public synchronized boolean registerConsumer(final String group, final MQConsumerInner consumer) {
		if (group == null || consumer == null) {
			return false;
		}

		MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
		if (prev != null) {
			log.warn("the consumer group[{}] exist already", group);
			return false;
		}
		return true;
	}

	public synchronized void unregisterConsumer(final String group) {
		this.consumerTable.remove(group);
		this.unregisterClient(null, group);
	}

	public synchronized boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
		if (group == null || producer == null) {
			return false;
		}

		MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
		if (prev != null) {
			log.warn("the producer group[{}] exist already.", group);
			return false;
		}
		return true;
	}

	public synchronized void unregisterProducer(final String group) {
		this.producerTable.remove(group);
		this.unregisterClient(group, null);
	}

	public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
		if (group == null || admin == null) {
			return false;
		}

		MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
		if (prev != null) {
			log.warn("the admin group[{}] exist already.", group);
			return false;
		}

		return true;
	}

	public void unregisterAdminExt(final String group) {
		this.adminExtTable.remove(group);
	}

	public void rebalanceLater(long delayMillis) {
		if (delayMillis <= 0) {
			this.rebalanceService.wakeup();
		}
		else {
			this.scheduledExecutorService.schedule(this.rebalanceService::wakeup, delayMillis, TimeUnit.MILLISECONDS);
		}
	}

	public void rebalanceImmediately() {
		this.rebalanceService.wakeup();
	}

	public boolean doRebalance() {
		boolean balanced = true;
		for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
			MQConsumerInner impl = entry.getValue();
			if (impl != null) {
				try {
					if (!impl.tryBalance()) {
						balanced = false;
					}
				}
				catch (Throwable e) {
					log.error("doRebalance exception", e);
				}
			}
		}
		return balanced;
	}

	public MQProducerInner selectProducer(final String group) {
		return this.producerTable.get(group);
	}

	public MQConsumerInner selectConsumer(final String group) {
		return this.consumerTable.get(group);
	}

	public String getBrokerNameFromMessageQueue(final MessageQueue mq) {
		if (topicEndPointsTable.get(mq.getTopic()) != null && !topicEndPointsTable.get(mq.getTopic()).isEmpty()) {
			return topicEndPointsTable.get(mq.getTopic()).get(mq);
		}
		return mq.getBrokerName();
	}

	public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
		if (brokerName == null) {
			return null;
		}
		String brokerAddr = null;
		boolean slave = false;
		boolean found = false;

		Map<Long, String> map = this.brokerAddrTable.get(brokerName);
		if (MapUtils.isNotEmpty(map)) {
			for (Map.Entry<Long, String> entry : map.entrySet()) {
				Long id = entry.getKey();
				brokerAddr = entry.getValue();

				if (brokerAddr != null) {
					found = true;
					slave = MixAll.MASTER_ID != id;
					break;
				}
			}
		}

		if (found) {
			return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
		}
		return null;
	}

	public String findBrokerAddressInPublish(final String brokerName) {
		if (brokerName == null) {
			return null;
		}

		Map<Long, String> map = this.brokerAddrTable.get(brokerName);
		if (MapUtils.isNotEmpty(map)) {
			return map.get(MixAll.MASTER_ID);
		}
		return null;
	}

	public FindBrokerResult findBrokerAddressInSubscribe(String brokerName, long brokerId, boolean onlyThisBroker) {
		if (brokerName == null) {
			return null;
		}

		String brokerAddr = null;
		boolean slave = false;
		boolean found = false;

		Map<Long, String> map = this.brokerAddrTable.get(brokerName);
		if (MapUtils.isNotEmpty(map)) {
			brokerAddr = map.get(brokerId);
			slave = brokerId != MixAll.MASTER_ID;
			found = brokerAddr != null;

			if (!found && slave) {
				brokerAddr = map.get(brokerId + 1);
				found = brokerAddr != null;
			}

			if (!found && !onlyThisBroker) {
				Map.Entry<Long, String> entry = map.entrySet().iterator().next();
				brokerAddr = entry.getValue();
				slave = entry.getKey() != MixAll.MASTER_ID;
				found = brokerAddr != null;
			}
		}

		if (found) {
			return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
		}
		return null;
	}

	public List<String> findConsumerIdList(final String topic, final String group) {
		String brokerAddr = this.findBrokerAddrByTopic(topic);
		if (brokerAddr == null) {
			this.updateTopicRouteInfoFromNameServer(topic);
			brokerAddr = this.findBrokerAddrByTopic(topic);
		}

		if (brokerAddr != null) {
			try {
				return this.mqClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, clientConfig.getMqClientApiTimeout());
			}
			catch (Exception e) {
				log.warn("getConsumerIdListByGroup exception, {} {}", brokerAddr, group, e);
			}
		}
		return null;
	}

	public Set<MessageQueueAssignment> queryAssignment(final String topic, final String consumerGroup, final String strategyName, final MessageModel messageModel, int timeout) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
		String brokerAddr = this.findBrokerAddrByTopic(topic);
		if (brokerAddr == null) {
			this.updateTopicRouteInfoFromNameServer(topic);
			brokerAddr = this.findBrokerAddrByTopic(topic);
		}

		if (brokerAddr != null) {
			return this.mqClientAPIImpl.queryAssignment(brokerAddr, topic, consumerGroup, clientId, strategyName, messageModel, timeout);
		}
		return null;
	}

	public String findBrokerAddrByTopic(final String topic) {
		TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
		if (topicRouteData != null) {
			List<BrokerData> brokers = topicRouteData.getBrokerDatas();
			if (!brokers.isEmpty()) {
				BrokerData bd = brokers.get(random.nextInt(brokers.size()));
				return bd.selectBrokerAddr();
			}
		}
		return null;
	}

	public synchronized void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
		DefaultMQPushConsumerImpl consumer = null;
		try {
			MQConsumerInner impl = this.consumerTable.get(group);
			if (impl instanceof DefaultMQPushConsumerImpl) {
				consumer = (DefaultMQPushConsumerImpl) impl;
			}
			else {
				log.info("[reset-offset] consumer does not exist, group={}", group);
				return;
			}
			consumer.suspend();

			ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
			for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
				MessageQueue mq = entry.getKey();
				if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
					ProcessQueue pq = entry.getValue();
					pq.setDropped(true);
					pq.clear();
				}
			}

			try {
				TimeUnit.SECONDS.sleep(10);
			}
			catch (InterruptedException ignored) {}

			Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
			while (iterator.hasNext()) {
				MessageQueue mq = iterator.next();
				Long offset = offsetTable.get(mq);
				if (topic.equals(mq.getTopic()) && offset != null) {
					try {
						consumer.updateConsumerOffset(mq, offset);
						consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, processQueueTable.get(mq));
						iterator.remove();
					}
					catch (Exception e) {
						log.warn("reset offset failed, group={}, {}", group, mq, e);
					}
				}
			}
		}
		finally {
			if (consumer != null) {
				consumer.resume();
			}
		}
	}

	public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
		MQConsumerInner mqConsumerInner = this.consumerTable.get(group);
		if (mqConsumerInner instanceof DefaultMQPushConsumerImpl) {
			DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;
			return consumer.getOffsetStore().cloneOffsetTable(topic);
		}
		else if (mqConsumerInner instanceof DefaultMQPullConsumerImpl) {
			DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) mqConsumerInner;
			return consumer.getOffsetStore().cloneOffsetTable(topic);
		}
		return Collections.emptyMap();
	}

	public TopicRouteData getAnExistTopicRouteData(final String topic) {
		return this.topicRouteTable.get(topic);
	}

	public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String consumerGroup, final String brokerName) {
		MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
		if (mqConsumerInner instanceof DefaultMQPushConsumerImpl) {
			DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;
			return consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
		}
		return null;
	}

	public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
		MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
		if (mqConsumerInner == null) {
			return null;
		}

		ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();
		List<String> nsList = this.mqClientAPIImpl.getRemotingClient().getNameServerAddressList();

		StringBuilder sb = new StringBuilder();
		if (nsList != null) {
			for (String addr : nsList) {
				sb.append(addr).append(";");
			}
		}

		String nsAddr = sb.toString();
		consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
		consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType());
		consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION, MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));

		return consumerRunningInfo;
	}

	public TopicRouteData queryTopicRouteData(String topic) {
		TopicRouteData data = this.getAnExistTopicRouteData(topic);
		if (data == null) {
			this.updateTopicRouteInfoFromNameServer(topic);
			data = this.getAnExistTopicRouteData(topic);
		}
		return data;
	}

	private void cleanOfflineBroker() {
		try {
			if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
				try {
					ConcurrentMap<String, Map<Long, String>> updateTable = new ConcurrentHashMap<>(this.brokerAddrTable.size(), 1);
					Iterator<Map.Entry<String, Map<Long, String>>> iterator = this.brokerAddrTable.entrySet().iterator();
					while (iterator.hasNext()) {
						Map.Entry<String, Map<Long, String>> entry = iterator.next();
						String brokerName = entry.getKey();
						Map<Long, String> oneTable = entry.getValue();

						Map<Long, String> cloneAddrTable = new HashMap<>(oneTable.size(), 1);
						cloneAddrTable.putAll(oneTable);

						Iterator<Map.Entry<Long, String>> itt = cloneAddrTable.entrySet().iterator();
						while (itt.hasNext()) {
							Map.Entry<Long, String> ee = itt.next();
							String addr = ee.getValue();
							if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
								itt.remove();
								log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
							}
						}

						if (cloneAddrTable.isEmpty()) {
							iterator.remove();
							log.info("the broker[{}] name's host is offline, remove it", brokerName);
						}
						else {
							updateTable.put(brokerName, cloneAddrTable);
						}
					}

					if (!updateTable.isEmpty()) {
						this.brokerAddrTable.putAll(updateTable);
					}
				}
				finally {
					this.lockNamesrv.unlock();
				}
			}
		}
		catch (InterruptedException e) {
			log.warn("cleanOfflineBroker Exception", e);
		}
	}

	private void persistAllConsumerOffset() {
		for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
			MQConsumerInner impl = entry.getValue();
			impl.persistConsumerOffset();
		}
	}

	private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
		for (Map.Entry<String, TopicRouteData> entry : this.topicRouteTable.entrySet()) {
			TopicRouteData topicRouteData = entry.getValue();
			List<BrokerData> bds = topicRouteData.getBrokerDatas();
			for (BrokerData bd : bds) {
				if (bd.getBrokerAddrs() != null) {
					boolean exist =bd.getBrokerAddrs().containsValue(addr);
					if (exist) {
						return true;
					}
				}
			}
		}
		return false;
	}

	private boolean sendHeartbeatToBroker(long id, String brokerName, String addr, HeartbeatData heartbeatData) {
		try {
			int version = this.mqClientAPIImpl.sendHeartbeat(addr, heartbeatData, clientConfig.getMqClientApiTimeout());
			this.brokerVersionTable.computeIfAbsent(brokerName, k -> new HashMap<>(4)).put(addr, version);
			long times = this.sendHeartbeatTimesTotal.getAndIncrement();
			if (times % 20 == 0) {
				log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
				log.info(heartbeatData.toString());
			}
			return true;
		}
		catch (Exception e) {
			if (this.isBrokerInNameServer(addr)) {
				log.warn("send heart beat to broker[{} {} {}] failed", brokerName, id, addr, e);
			}
			else {
				log.warn("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName, id, addr,e);
			}
		}
		return false;
	}

	private boolean sendHeartbeatToAllBroker() {
		HeartbeatData heartbeatData = this.prepareHeartbeatData(false);
		boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
		boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
		if (producerEmpty && consumerEmpty) {
			log.warn("sending heartbeat, but no consumer and no producer. [{}]", this.clientId);
			return false;
		}
		if (this.brokerAddrTable.isEmpty()) {
			return false;
		}

		for (Map.Entry<String, Map<Long, String>> entry : this.brokerAddrTable.entrySet()) {
			String brokerName = entry.getKey();
			Map<Long, String> oneTable = entry.getValue();
			if (oneTable == null) {
				continue;
			}

			for (Map.Entry<Long, String> singleBrokerInstance : oneTable.entrySet()) {
				Long id = singleBrokerInstance.getKey();
				String addr = singleBrokerInstance.getValue();
				if (addr == null) {
					continue;
				}
				if (consumerEmpty && MixAll.MASTER_ID != id) {
					continue;
				}

				sendHeartbeatToBroker(id, brokerName, addr, heartbeatData);
			}
		}
		return true;
	}

	private boolean sendHeartbeatToBrokerV2(long id, String brokerName, String addr, HeartbeatData heartbeatData, HeartbeatData heartbeatDataWithoutSub, int currentHeartbeatFingerprint) {
		try {
			int version = 0;
			boolean isBrokerSupportV2 = brokerSupportV2HeartbeatSet.contains(addr);
			HeartbeatV2Result result = null;
			if (isBrokerSupportV2 && brokerAddrHeartbeatFingerprintTable.get(addr) == currentHeartbeatFingerprint) {
				result = this.mqClientAPIImpl.sendHeartbeatV2(addr, heartbeatDataWithoutSub, clientConfig.getMqClientApiTimeout());
				if (result.isSubChange()) {
					brokerAddrHeartbeatFingerprintTable.remove(addr);
				}
			}
			else {
				result = this.mqClientAPIImpl.sendHeartbeatV2(addr, heartbeatData, clientConfig.getMqClientApiTimeout());
				if (result.isSupportV2()) {
					brokerSupportV2HeartbeatSet.add(addr);
					if (result.isSubChange()) {
						brokerAddrHeartbeatFingerprintTable.remove(addr);
					}
					else if (!brokerAddrHeartbeatFingerprintTable.containsKey(addr) || brokerAddrHeartbeatFingerprintTable.get(addr) != currentHeartbeatFingerprint) {
						brokerAddrHeartbeatFingerprintTable.put(addr, currentHeartbeatFingerprint);
					}
				}
				log.info("sendHeartbeatToBrokerV2 normal brokerName: {} subChange: {} brokerAddrHeartbeatFingerprintTable: {}", brokerName, result.isSubChange(), JSON.toJSONString(brokerAddrHeartbeatFingerprintTable));
			}

			version = result.getVersion();
			this.brokerVersionTable.computeIfAbsent(brokerName, k -> new HashMap<>(4)).put(addr, version);

			long times = this.sendHeartbeatTimesTotal.getAndIncrement();
			if (times % 20 == 0) {
				log.info("send heart beat to broker[{} {} {]] success", brokerName, id, addr);
				log.info(heartbeatData.toString());
			}
			return true;
		}
		catch (Exception e) {
			if (this.isBrokerInNameServer(addr)) {
				log.warn("sendHeartbeatToBrokerV2 send heart beat to broker[{} {} {}] failed", brokerName, id, addr, e);
			}
			else {
				log.warn("sendHeartbeatToBrokerV2 send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName, id, addr, e);
			}
		}
		return false;
	}

	private boolean sendHeartbeatToAllBrokerV2(boolean isRebalance) {
		HeartbeatData heartbeatData = this.prepareHeartbeatData(false);
		boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
		boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
		if (producerEmpty && consumerEmpty) {
			log.warn("sendHeartbeatToAllBrokerV2 sending heartbeat, but no consumer and no producer. [{}]", this.clientId);
			return false;
		}
		if (this.brokerAddrTable.isEmpty()) {
			return false;
		}
		if (isRebalance) {
			resetBrokerAddrHeartbeatFingerprintMap();
		}

		int currentHeartbeatFingerprint = heartbeatData.computeHeartbeatFingerprint();
		heartbeatData.setHeartbeatFingerprint(currentHeartbeatFingerprint);
		HeartbeatData heartbeatDataWithoutSub = this.prepareHeartbeatData(true);
		heartbeatDataWithoutSub.setHeartbeatFingerprint(currentHeartbeatFingerprint);

		for (Map.Entry<String, Map<Long, String>> entry : this.brokerAddrTable.entrySet()) {
			String brokerName = entry.getKey();
			Map<Long, String> oneTable = entry.getValue();
			if (oneTable == null) {
				continue;
			}

			for (Map.Entry<Long, String> singleBrokerInstance : oneTable.entrySet()) {
				Long id = singleBrokerInstance.getKey();
				String addr = singleBrokerInstance.getValue();
				if (addr == null) {
					continue;
				}
				if (consumerEmpty && MixAll.MASTER_ID != id) {
					continue;
				}

				sendHeartbeatToBrokerV2(id, brokerName, addr, heartbeatData, heartbeatData, currentHeartbeatFingerprint);
			}
		}
		return true;
	}

	private HeartbeatData prepareHeartbeatData(boolean isWithoutSub) {
		HeartbeatData heartbeatData = new HeartbeatData();

		heartbeatData.setClientID(this.clientId);

		for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
			MQConsumerInner impl = entry.getValue();
			if (impl != null) {
				ConsumerData consumerData = new ConsumerData();
				consumerData.setGroupName(impl.groupName());
				consumerData.setConsumeType(impl.consumeType());
				consumerData.setMessageModel(impl.messageModel());
				consumerData.setConsumeFromWhere(impl.consumeFromWhere());
				consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
				consumerData.setUnitMode(impl.isUnitMode());
				if (!isWithoutSub) {
					consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
				}
				heartbeatData.getConsumerDataSet().add(consumerData);
			}
		}

		for (Map.Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
			MQProducerInner impl = entry.getValue();
			if (impl != null) {
				ProducerData producerData = new ProducerData();
				producerData.setGroupName(entry.getKey());

				heartbeatData.getProducerDataSet().add(producerData);
			}
		}

		heartbeatData.setWithoutSub(isWithoutSub);
		return heartbeatData;
	}

	private boolean isBrokerInNameServer(final String brokerAddr) {
		for (Map.Entry<String, TopicRouteData> entry : this.topicRouteTable.entrySet()) {
			List<BrokerData> brokerDatas = entry.getValue().getBrokerDatas();
			for (BrokerData bd : brokerDatas) {
				boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
				if (contain) {
					return true;
				}
			}
		}
		return false;
	}

	private boolean isNeedUpdateTopicRouteInfo(final String topic) {
		boolean result = false;
		Iterator<Map.Entry<String, MQProducerInner>> iterator = this.producerTable.entrySet().iterator();
		while (iterator.hasNext() && !result) {
			Map.Entry<String, MQProducerInner> entry = iterator.next();
			MQProducerInner impl = entry.getValue();
			if (impl != null) {
				result = impl.isPublishTopicNeedUpdate(topic);
			}
		}

		if (result) {
			return true;
		}

		Iterator<Map.Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
		while (it.hasNext() && !result) {
			Map.Entry<String, MQConsumerInner> next = it.next();
			MQConsumerInner impl = next.getValue();
			if (impl != null) {
				result = impl.isSubscribeTopicNeedUpdate(topic);
			}
		}

		return result;
	}

	private void unregisterClient(final String producerGroup, final String consumerGroup) {
		for (Map.Entry<String, Map<Long, String>> entry : this.brokerAddrTable.entrySet()) {
			String brokerName = entry.getKey();
			Map<Long, String> oneTable = entry.getValue();

			if (oneTable == null) {
				continue;
			}

			for (Map.Entry<Long, String> singleBrokerInstance : oneTable.entrySet()) {
				String addr = singleBrokerInstance.getValue();
				if (addr != null) {
					try {
						this.mqClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, clientConfig.getMqClientApiTimeout());
						log.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success",
								producerGroup, consumerGroup, brokerName, singleBrokerInstance.getKey(), addr);
					}
					catch (RemotingException e) {
						log.warn("unregister client RemotingException from broker: {}, {}", addr, e.getMessage());
					}
					catch (InterruptedException e) {
						log.warn("unregister client InterruptedException from broker: {}, {}", addr, e.getMessage());
					}
					catch (MQBrokerException e) {
						log.warn("unregister client MQBrokerException from broker: {}, {}", addr, e.getMessage());
					}
				}
			}
		}
	}

	private int findBrokerVersion(String brokerName, String brokerAddr) {
		if (this.brokerVersionTable.containsKey(brokerName)) {
			if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
				return this.brokerVersionTable.get(brokerName).get(brokerAddr);
			}
		}
		return 0;
	}

	private void startScheduledTask() {
		if (this.clientConfig.getNamesrvAddr() == null) {
			this.scheduledExecutorService.scheduleAtFixedRate(() -> {
				try {
					MQClientInstance.this.mqClientAPIImpl.fetchNameServerAddr();
				}
				catch (Throwable t) {
					log.error("ScheduledTask fetchNameServerAddr exception", t);
				}
			}, 10 * 1000, 2 * 60 * 1000, TimeUnit.MILLISECONDS);
		}

		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				MQClientInstance.this.updateTopicRouteInfoFromNameServer();
			}
			catch (Throwable t) {
				log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", t);
			}
		}, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);

		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				MQClientInstance.this.cleanOfflineBroker();
				MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
			}
			catch (Throwable t) {
				log.error("ScheduledTask sendHeartbeatToAllBroker exception", t);
			}
		}, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				MQClientInstance.this.persistAllConsumerOffset();
			}
			catch (Throwable t) {
				log.error("ScheduledTask persistAllConsumerOffset exception", t);
			}
		}, 10 * 1000, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				MQClientInstance.this.adjustThreadPool();
			}
			catch (Throwable t) {
				log.error("ScheduledTask adjustThreadPoll exception", t);
			}
		}, 1, 1, TimeUnit.MINUTES);
	}

	private void resetBrokerAddrHeartbeatFingerprintMap() {
		this.brokerAddrHeartbeatFingerprintTable.clear();
	}
}
