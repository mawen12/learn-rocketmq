package com.mawen.learn.rocketmq.client.impl.factory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
import java.util.stream.Stream;

import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.admin.MQAdminExtInner;
import com.mawen.learn.rocketmq.client.consumer.RebalanceService;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.impl.ClientRemotingProcessor;
import com.mawen.learn.rocketmq.client.impl.MQAdminImpl;
import com.mawen.learn.rocketmq.client.impl.MQClientAPIImpl;
import com.mawen.learn.rocketmq.client.impl.consumer.MQConsumerInner;
import com.mawen.learn.rocketmq.client.impl.consumer.PullMessageService;
import com.mawen.learn.rocketmq.client.impl.producer.MQProducerInner;
import com.mawen.learn.rocketmq.client.impl.producer.TopicPublishInfo;
import com.mawen.learn.rocketmq.client.producer.DefaultMQProducer;
import com.mawen.learn.rocketmq.client.stat.ConsumerStatsManager;
import com.mawen.learn.rocketmq.common.MQVersion;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.ServiceState;
import com.mawen.learn.rocketmq.common.constant.PermName;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.ChannelEventListener;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.netty.NettyClientConfig;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import com.mawen.learn.rocketmq.remoting.protocol.route.BrokerData;
import com.mawen.learn.rocketmq.remoting.protocol.route.QueueData;
import com.mawen.learn.rocketmq.remoting.protocol.route.TopicRouteData;
import io.netty.channel.Channel;
import lombok.Getter;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/21
 */
@Getter
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
			}
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
			ConcurrentMap<MessageQueue, Long> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, routeData);
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
						if ()
					}
				}
				catch () {

				}
				finally {
					this.lockNamesrv.unlock();
				}
			}
		}
		catch (InterruptedException e) {
			log.warn("updateTopicRouteInfoFromNameServer exception", e);
		}

		return false;
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
}
