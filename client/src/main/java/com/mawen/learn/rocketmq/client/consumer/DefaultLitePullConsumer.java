package com.mawen.learn.rocketmq.client.consumer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import com.mawen.learn.rocketmq.client.consumer.store.OffsetStore;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.trace.AsyncTraceDispatcher;
import com.mawen.learn.rocketmq.client.trace.TraceDispatcher;
import com.mawen.learn.rocketmq.client.trace.hook.ConsumeMessageTraceHookImpl;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.consumer.ConsumeFromWhere;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import static com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
@Setter
public class DefaultLitePullConsumer extends ClientConfig implements LitePullConsumer {

	private static final Logger log = LoggerFactory.getLogger(DefaultLitePullConsumer.class);

	private static final long MIN_AUTOCOMMIT_INTERVAL_MILLIS = 1000;

	private final DefaultLitePullConsumerIml defaultLitePullConsumerIml;

	private String consumerGroup;

	private long brokerSuspendMaxTimeMillis = 20 * 1000;

	private long consumerTimeoutMillisWhenSuspend = 30 * 1000;

	private long consumerPullTimeoutMillis = 10 * 1000;

	private MessageModel messageModel = MessageModel.CLUSTERING;

	private MessageQueueListener messageQueueListener;

	private OffsetStore offsetStore;

	private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();

	private boolean unitMode = false;

	private boolean autoCommit = true;

	private int pullThreadNums = 20;

	private long autoCommitIntervalMillis = 5 * 1000;

	private int pullBatchSize = 10;

	private long pullThresholdForAll = 10000;

	private int consumeMaxSpan = 2000;

	private int pullThresholdForQueue = 1000;

	private int pullThresholdSizeForQueue = 100;

	private long pollTimeoutMillis = 5 * 1000;

	private long topicMetadataCheckIntervalMillis = 30 * 1000;

	private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

	private String consumeTimestamp = UtilAll.timeMillisToHumanString3(System.currentTimeMillis() - (30 * 60 * 1000));

	private TraceDispatcher traceDispatcher;

	private RPCHook rpcHook;

	public DefaultLitePullConsumer(final String consumerGroup, RPCHook rpcHook) {
		this.consumerGroup = consumerGroup;
		this.rpcHook = rpcHook;
		this.enableStreamRequestType = true;
		this.defaultLitePullConsumerIml = new DefaultLitePullConsumerImpl(this, rpcHook);
	}

	public DefaultLitePullConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook) {
		this.namespace = namespace;
		this.consumerGroup = consumerGroup;
		this.rpcHook = rpcHook;
		this.enableStreamRequestType = true;
		this.defaultLitePullConsumerIml = new DefaultLitePullConsumerImpl(this, rpcHook);
	}

	@Override
	public void start() throws MQClientException {
		setTraceDispatcher();
		setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
		this.defaultLitePullConsumerIml.start();
		if (traceDispatcher != null) {
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
		this.defaultLitePullConsumerIml.shutdown();
		if (traceDispatcher != null) {
			traceDispatcher.shutdown();
		}
	}

	@Override
	public boolean isRunning() {
		return this.defaultLitePullConsumerIml.isRunning();
	}

	@Override
	public void subscribe(String topic) throws MQClientException {
		this.subscribe(topic, SUB_ALL);
	}

	@Override
	public void subscribe(String topic, String subExpression) throws MQClientException {
		this.defaultLitePullConsumerIml.subscribe(withNamespace(topic), subExpression);
	}

	@Override
	public void subscribe(String topic, String subExpression, MessageQueueListener listener) throws MQClientException {
		this.defaultLitePullConsumerIml.subscribe(withNamespace(topic), subExpression, listener);
	}

	@Override
	public void subscribe(String topic, MessageSelector selector) throws MQClientException {
		this.defaultLitePullConsumerIml.subscribe(withNamespace(topic), selector);
	}

	@Override
	public void unsubscribe(String topic) {
		this.defaultLitePullConsumerIml.unsubscribe(withNamespace(topic));
	}

	@Override
	public void assign(Collection<MessageQueue> messageQueues) {
		this.defaultLitePullConsumerIml.assign(queuesWithNamespace(messageQueues));
	}

	@Override
	public void setSubExpressionForAssign(String topic, String subExpression) {
		this.defaultLitePullConsumerIml.setSubExpressionForAssign(withNamespace(topic), subExpression);
	}

	@Override
	public List<MessageExt> poll() {
		return this.defaultLitePullConsumerIml.poll(this.getPollTimeoutMillis());
	}

	@Override
	public List<MessageExt> poll(long timeout) {
		return this.defaultLitePullConsumerIml.poll(timeout);
	}

	@Override
	public void seek(MessageQueue mq, long offset) throws MQClientException {
		this.defaultLitePullConsumerIml.seek(queueWithNamespace(mq), offset);
	}

	@Override
	public void pause(Collection<MessageQueue> messageQueues) {
		this.defaultLitePullConsumerIml.pause(queuesWithNamespace(messageQueues));
	}

	@Override
	public void resume(Collection<MessageQueue> messageQueues) {
		this.defaultLitePullConsumerIml.resume(queuesWithNamespace(messageQueues));
	}

	@Override
	public Collection<MessageQueue> fetchMessageQueues(String topic) throws MQClientException {
		return this.defaultLitePullConsumerIml.fetchMessageQueues(withNamespace(topic));
	}

	@Override
	public Long offsetForTimestamp(MessageQueue mq, Long timestamp) throws MQClientException {
		return this.defaultLitePullConsumerIml.searchOffset(queueWithNamespace(mq), timestamp);
	}

	@Override
	public void commitSync() {
		this.defaultLitePullConsumerIml.commitAll();
	}

	@Override
	public void commitSync(Map<MessageQueue, Long> offsetMap, boolean persist) {
		this.defaultLitePullConsumerIml.commit(offsetMap, persist);
	}

	@Override
	public void commit() {
		this.defaultLitePullConsumerIml.commitAll();
	}

	@Override
	public void commit(Map<MessageQueue, Long> offsetMap, boolean persist) {
		this.defaultLitePullConsumerIml.commit(offsetMap, persist);
	}

	@Override
	public void commit(Set<MessageQueue> messageQueues, boolean persist) {
		this.defaultLitePullConsumerIml.commit(messageQueues, persist);
	}

	@Override
	public Long committed(MessageQueue mq) throws MQClientException {
		return this.defaultLitePullConsumerIml.committed(queueWithNamespace(mq));
	}

	@Override
	public Set<MessageQueue> assignment() throws MQClientException {
		return this.defaultLitePullConsumerIml.assignment();
	}

	@Override
	public void registerTopicMessageQueueChangeListener(String topic, TopicMessageQueueChangeListener listener) throws MQClientException {
		this.defaultLitePullConsumerIml.registerTopicMessageQueueChangeListener(withNamespace(topic), listener);
	}

	@Override
	public void updateNameServerAddress(String nameServerAddress) {
		this.defaultLitePullConsumerIml.updateNameServerAddress(nameServerAddress);
	}

	@Override
	public void seekToBegin(MessageQueue mq) throws MQClientException {
		this.defaultLitePullConsumerIml.seekToBegin(queueWithNamespace(mq));
	}

	@Override
	public void seekToEnd(MessageQueue mq) throws MQClientException {
		this.defaultLitePullConsumerIml.seekToEnd(queueWithNamespace(mq));
	}

	public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
		if (consumeFromWhere != ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET
				|| consumeFromWhere != ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
				|| consumeFromWhere != ConsumeFromWhere.CONSUME_FROM_TIMESTAMP) {
			throw new RuntimeException("Invalid ConsumeFromWhere Value", null);
		}

		this.consumeFromWhere = consumeFromWhere;
	}

	public void setAutoCommitIntervalMillis(long autoCommitIntervalMillis) {
		if (autoCommitIntervalMillis >= MIN_AUTOCOMMIT_INTERVAL_MILLIS) {
			this.autoCommitIntervalMillis = autoCommitIntervalMillis;
		}
	}

	private void setTraceDispatcher() {
		if (enableTrace) {
			try {
				AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(consumerGroup, TraceDispatcher.Type.CONSUME, traceTopic, rpcHook);
				dispatcher.getTraceProducer().setUseTLS(isUseTLS());
				dispatcher.setNamespaceV2(namespaceV2);
				this.traceDispatcher = dispatcher;
				this.defaultLitePullConsumerIml.registerConsumeMessageHook(new ConsumeMessageTraceHookImpl(dispatcher));
			}
			catch (Throwable e) {
				log.error("system mqtrace hook init failed, maybe can't send msg trace data");
			}
		}
	}
}
