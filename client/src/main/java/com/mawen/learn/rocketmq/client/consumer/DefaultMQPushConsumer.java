package com.mawen.learn.rocketmq.client.consumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.QueryResult;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListener;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.mawen.learn.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.mawen.learn.rocketmq.client.consumer.store.OffsetStore;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageHook;
import com.mawen.learn.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.mawen.learn.rocketmq.client.trace.AsyncTraceDispatcher;
import com.mawen.learn.rocketmq.client.trace.TraceDispatcher;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.consumer.ConsumeFromWhere;
import com.mawen.learn.rocketmq.common.message.MessageDecoder;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
@Setter
public class DefaultMQPushConsumer extends ClientConfig implements MQPushConsumer {

	private static final Logger log = LoggerFactory.getLogger(DefaultMQPushConsumer.class);

	protected final transient DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

	private String consumerGroup;

	private MessageModel messageModel = MessageModel.CLUSTERING;

	private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

	private String consumeTimestamp = UtilAll.timeMillisToHumanString(System.currentTimeMillis());

	private AllocateMessageQueueStrategy allocateMessageQueueStrategy;

	private Map<String, String> subscription = new HashMap<>();

	private MessageListener messageListener;

	private MessageQueueListener messageQueueListener;

	private OffsetStore offsetStore;

	private int consumeThreadMin = 20;

	private int consumeThreadMax = 20;

	private long adjustThreadPoolNumsThread = 100000;

	private int consumeConcurrentlyMaxSpan = 2000;

	private int pullThresholdForQueue = 1000;

	private int popThresholdForQueue = 96;

	private int pullThresholdSizeForQueue = 100;

	private int pullThreadForTopic = -1;

	private long pullInterval = 0;

	private int consumeMessageBatchMaxSize = 1;

	private int pullBatchSize = 32;

	private int pullBatchSizeInBytes = 256 * 1024;

	private boolean postSubscriptionWhenPull = false;

	private boolean unitMode = false;

	private int maxReconsumeTimes = -1;

	private long suspendCurrentQueueTimeMillis = 1000;

	private long consumeTimeout = 15;

	private long popInvisibleTime = 60000;

	private int popBatchNums = 32;

	private long awaitTerminationMillisWhenShutdown = 0;

	private TraceDispatcher traceDispatcher;

	private boolean clientRebalance = true;

	private RPCHook rpcHook;

	public DefaultMQPushConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook, AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
		this.consumerGroup = consumerGroup;
		this.namespace = namespace;
		this.rpcHook = rpcHook;
		this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
		this.defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
	}

	public DefaultMQPushConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook, AllocateMessageQueueStrategy allocateMessageQueueStrategy, boolean enableMsgTrace, final String customizedTraceTopic) {
		this.consumerGroup = consumerGroup;
		this.namespace = namespace;
		this.rpcHook = rpcHook;
		this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
		this.defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(this, rpcHook);
		this.enableTrace = enableMsgTrace;
		this.traceTopic = customizedTraceTopic;
	}

	@Override
	public void createTopic(String key, String newTopic, int queueNum, Map<String, String> attributes) throws MQClientException {
		createTopic(key, newTopic, queueNum, 0, null);
	}

	@Override
	public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag, Map<String, String> attributes) throws MQClientException {
		this.defaultMQPushConsumerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
	}

	@Override
	public void setUseTLS(boolean useTLS) {
		super.setUseTLS(useTLS);
	}

	@Override
	public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
		return this.defaultMQPushConsumerImpl.searchOffset(queueWithNamespace(mq), timestamp);
	}

	@Override
	public long maxOffset(MessageQueue mq) throws MQClientException {
		return this.defaultMQPushConsumerImpl.maxOffset(queueWithNamespace(mq));
	}

	@Override
	public long minOffset(MessageQueue mq) throws MQClientException {
		return this.defaultMQPushConsumerImpl.minOffset(queueWithNamespace(mq));
	}

	@Override
	public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
		return this.defaultMQPushConsumerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
	}

	@Override
	public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws MQClientException, InterruptedException {
		return this.defaultMQPushConsumerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
	}

	@Override
	public MessageExt viewMessage(String topic, String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
		try {
			MessageDecoder.decodeMessageId(msgId);
			return this.defaultMQPushConsumerImpl.viewMessage(withNamespace(topic), msgId);
		}
		catch (Exception ignored){}

		return this.defaultMQPushConsumerImpl.queryMessageByUniqKey(withNamespace(topic), msgId);
	}

	public void setSubscription(Map<String, String> subscription) {
		Map<String, String> subscriptionWithNamespace = new HashMap<>(subscription.size(), 1);
		for (Map.Entry<String, String> topicEntry : subscription.entrySet()) {
			subscriptionWithNamespace.put(withNamespace(topicEntry.getKey()), topicEntry.getValue());
		}
		this.subscription = subscriptionWithNamespace;
	}

	@Override
	public void sendMessageBack(MessageExt msg, int delayLevel) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, null);
	}

	@Override
	public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
	}

	@Override
	public Set<MessageQueue> fetchSubscribeMessageQueue(String topic) throws MQClientException {
		return this.defaultMQPushConsumerImpl.fetchSubscribeMessageQueue(withNamespace(topic));
	}

	@Override
	public void start() throws MQClientException {
		setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
		this.defaultMQPushConsumerImpl.start();

		if (enableTrace) {
			try {
				AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(consumerGroup, TraceDispatcher.Type.CONSUME, traceTopic, rpcHook);
				dispatcher.setHostConsumer(this.defaultMQPushConsumerImpl);
				dispatcher.setNamespaceV2(namespaceV2);
				this.traceDispatcher = dispatcher;
				this.defaultMQPushConsumerImpl.registerConsumeMessageHook(new ConsumeMessageHookImpl(traceDispatcher));
			}
			catch (Throwable e) {
				log.error("system mqtrace hook init failed, maybe can't send msg trace data");
			}
		}

		if (traceDispatcher != null) {
			if (traceDispatcher instanceof AsyncTraceDispatcher) {
				((AsyncTraceDispatcher) traceDispatcher).getTraceProducer().setUseTLS(isUseTLS());
			}
			try {
				traceDispatcher.start(this.getNamesrvAddr(), this.getAccessChannel());
			}
			catch (MQClientException e) {
				log.warn("trace dispatcher start failed", e);
			}
		}
	}

	@Override
	public void shutdown() {
		this.defaultMQPushConsumerImpl.shutdown(awaitTerminationMillisWhenShutdown);
		if (traceDispatcher != null) {
			traceDispatcher.shutdown();
		}
	}

	@Override
	public void registerMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
		this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
	}

	@Override
	public void registerMessageListener(MessageListenerConcurrently messageListener) {
		this.messageListener = messageListener;
		this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
	}

	@Override
	public void registerMessageListener(MessageListenerOrderly messageListener) {
		this.messageListener = messageListener;
		this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
	}

	@Override
	public void subscribe(String topic, String subExpression) throws MQClientException {
		this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), subExpression);
	}

	@Override
	public void subscribe(String topic, String fullClassName, String filterClassName) throws MQClientException {
		this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), fullClassName, filterClassName);
	}

	@Override
	public void subscribe(String topic, MessageSelector selector) throws MQClientException {
		this.defaultMQPushConsumerImpl.subscribe(withNamespace(topic), selector);
	}

	@Override
	public void unsubscribe(String topic) {
		this.defaultMQPushConsumerImpl.unsubscribe(topic);
	}

	@Override
	public void updateCorePoolSize(int corePoolSize) {
		this.defaultMQPushConsumerImpl.updateCorePoolSize(corePoolSize);
	}

	@Override
	public void suspend() {
		this.defaultMQPushConsumerImpl.suspend();
	}

	@Override
	public void resume() {
		this.defaultMQPushConsumerImpl.resume();
	}

	public boolean isPause() {
		return this.defaultMQPushConsumerImpl.isPause();
	}

	public boolean isConsumeOrderly() {
		return this.defaultMQPushConsumerImpl.isConsumeOrderly();
	}

	public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
		this.defaultMQPushConsumerImpl.registerConsumeMessageHook(hook);
	}
}
