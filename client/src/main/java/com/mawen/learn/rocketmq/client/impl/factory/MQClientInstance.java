package com.mawen.learn.rocketmq.client.impl.factory;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.impl.producer.MQProducerInner;
import com.mawen.learn.rocketmq.client.producer.DefaultMQProducer;
import com.mawen.learn.rocketmq.common.ServiceState;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.netty.NettyClientConfig;
import com.mawen.learn.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/21
 */
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
	private final ConcurrentMap<String, MQCOnsumerInner> consumerTable = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, ConcurrentMap<MessageQueue, String>> topicEndPointsTable = new ConcurrentHashMap<>();
	private final Lock lockNamesrv = new ReentrantLock();
	private final Lock lockHeartbeat = new ReentrantLock();
	private final ConcurrentMap<String, Map<String, String>> brokerAddrTable = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, Map<String, Integer>> brokerVersionTable = new ConcurrentHashMap<>();
	private final Set<String> brokerSupportV2HeartbeatSet = new HashSet<>();
	private final ConcurrentMap<String, Integer> brokerAddrHeartbeatFingerprintTable = new ConcurrentHashMap<>();
	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryScheduledThread"));
	private final ScheduledExecutorService scheduledRemoteConfigExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryFetchRemoteConfigScheduledThread"));

	private final PullMessageService pullMessageService;
	private final RebalanceService rebalanceService;
	private final DefaultMQProducer defaultMQProducer;
	private final ConsumeStatsManager consumeStatsManager;
	private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);
	private ServiceState serviceStater = ServiceState.CREATE_JUST;
	private final Random random = new Random();
}
