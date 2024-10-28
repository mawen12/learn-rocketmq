package com.mawen.learn.rocketmq.client.producer;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;

import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.QueryResult;
import com.mawen.learn.rocketmq.client.Validators;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.impl.MQClientManager;
import com.mawen.learn.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.mawen.learn.rocketmq.client.trace.AsyncTraceDispatcher;
import com.mawen.learn.rocketmq.client.trace.TraceDispatcher;
import com.mawen.learn.rocketmq.client.trace.hook.EndTransactionTraceHookImpl;
import com.mawen.learn.rocketmq.client.trace.hook.SendMessageTraceHookImpl;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageBatch;
import com.mawen.learn.rocketmq.common.message.MessageClientIDSetter;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
@Getter
@Setter
public class DefaultMQProducer extends ClientConfig implements MQProducer{

	private final Logger log = LoggerFactory.getLogger(DefaultMQProducer.class);

	protected final transient DefaultMQProducerImpl defaultMQProducerImpl;

	private final Set<Integer> retryResponseCodes = new CopyOnWriteArraySet<>(Arrays.asList(
			ResponseCode.TOPIC_NOT_EXIST,
			ResponseCode.SERVICE_NOT_AVAILABLE,
			ResponseCode.SYSTEM_ERROR,
			ResponseCode.SYSTEM_BUSY,
			ResponseCode.NO_PERMISSION,
			ResponseCode.NO_BUYER_ID,
			ResponseCode.NOT_IN_CURRENT_UNIT
			));

	private String producerGroup;

	private List<String> topics;

	private String createTopicKey = TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC;

	private volatile int defaultTopicQueueNums = 4;

	private int sendMsgTimeout = 3000;

	private int compressMsgBodyOverHowmuch = 4 * 1024;

	private int retryTimesWhenSendFailed = 2;

	private int retryTimesWhenSendAsyncFailed = 2;

	private boolean retryAnotherBrokerWhenNotStoreOK = false;

	private int maxMessageSize = 4 * 1024 * 1024;

	private TraceDispatcher traceDispatcher;

	private boolean authBatch = false;

	private ProduceAccumulator produceAccumulator;

	private boolean enableBackpressureForAsyncMode = false;

	private int backPressureForAsyncSendNum = 10000;

	private int backPressureForAsyncSendSize = 100 * 1024 * 1024;

	private RPCHook rpcHook;

	public DefaultMQProducer() {
		this(null, MixAll.DEFAULT_PRODUCER_GROUP, null);
	}

	public DefaultMQProducer(RPCHook rpcHook) {
		this(null, MixAll.DEFAULT_PRODUCER_GROUP, rpcHook);
	}

	public DefaultMQProducer(final String producerGroup) {
		this(null, producerGroup, null);
	}

	public DefaultMQProducer(final String producerGroup, RPCHook rpcHook) {
		this(null, producerGroup, rpcHook);
	}

	public DefaultMQProducer(final String producerGroup, RPCHook rpcHook, final List<String> topics) {
		this(null, producerGroup, rpcHook);
		this.topics = topics;
	}

	public DefaultMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook) {
		this.namespace = namespace;
		this.producerGroup = producerGroup;
		this.rpcHook = rpcHook;

		this.defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
		this.produceAccumulator = MQClientManager.getInstance().getOrCreateProduceAccumulator(this);
	}

	public DefaultMQProducer(final String namespace, final String producerGroup, RPCHook rpcHook, boolean enableMsgTrace, final String customizedTraceTopic) {
		this(namespace, producerGroup, rpcHook);
		this.enableTrace = enableMsgTrace;
		this.traceTopic = customizedTraceTopic;
	}

	@Override
	public void start() throws MQClientException {
		this.setProducerGroup(withNamespace(this.producerGroup));
		this.defaultMQProducerImpl.start();
		if (this.produceAccumulator != null) {
			this.produceAccumulator.start();
		}

		if (enableTrace) {
			try {
				AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(producerGroup, TraceDispatcher.Type.PRODUCE, traceTopic, rpcHook);
				dispatcher.setHostProducer(this.defaultMQProducerImpl);
				dispatcher.setNamespaceV2(this.namespaceV2);

				this.traceDispatcher = dispatcher;
				this.defaultMQProducerImpl.registerSendMessageHook(new SendMessageTraceHookImpl(traceDispatcher));
				this.defaultMQProducerImpl.registerEndTransactionHook(new EndTransactionTraceHookImpl(traceDispatcher));
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
		this.defaultMQProducerImpl.shutdown();
		if (this.produceAccumulator != null) {
			this.produceAccumulator.shutdown();
		}
		if (this.traceDispatcher != null) {
			this.traceDispatcher.shutdown();
		}
	}

	@Override
	public List<MessageQueue> fetchPublishMessageQueues(String topic) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		return this.defaultMQProducerImpl.fetchPublishMessageQueues(withNamespace(topic));
	}

	@Override
	public SendResult send(Message msg) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		if (this.getAutoBatch() && !(msg instanceof MessageBatch)) {
			return sendByAccumulator(msg, null, null);
		}
		else {
			return sendDirect(msg, null, null);
		}
	}

	@Override
	public SendResult send(Message msg, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		return this.defaultMQProducerImpl.send(msg, timeout);
	}

	@Override
	public void send(Message msg, SendCallback sendCallback) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQProducerImpl.send(msg, sendCallback);
	}

	@Override
	public void send(Message msg, SendCallback sendCallback, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
	}

	@Override
	public void sendOneway(Message msg) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQProducerImpl.sendOneway(msg);
	}

	@Override
	public SendResult send(Message msg, MessageQueue mq) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq));
	}

	@Override
	public SendResult send(Message msg, MessageQueue mq, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		return this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), timeout);
	}

	@Override
	public void send(Message msg, MessageQueue mq, SendCallback sendCallback) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback);
	}

	@Override
	public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQProducerImpl.send(msg, queueWithNamespace(mq), sendCallback, timeout);
	}

	@Override
	public void sendOneway(Message msg, MessageQueue mq) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQProducerImpl.sendOneway(msg, queueWithNamespace(mq));
	}

	@Override
	public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		return this.defaultMQProducerImpl.send(msg, selector, arg);
	}

	@Override
	public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
	}

	@Override
	public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback);
	}

	@Override
	public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback, timeout);
	}

	@Override
	public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
	}

	@Override
	public TransactionResult sendMessageInTransaction(Message msg, Object arg) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
	}

	@Override
	public SendResult send(Collection<Message> msgs) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		return this.defaultMQProducerImpl.send(batch(msgs));
	}

	@Override
	public void send(Collection<Message> msgs, SendCallback sendCallback) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		this.defaultMQProducerImpl.send(batch(msgs), sendCallback);
	}

	@Override
	public SendResult send(Collection<Message> msgs, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		return this.defaultMQProducerImpl.send(batch(msgs), timeout);
	}

	@Override
	public void send(Collection<Message> msgs, SendCallback sendCallback, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		this.defaultMQProducerImpl.send(batch(msgs), sendCallback, timeout);
	}

	@Override
	public SendResult send(Collection<Message> msgs, MessageQueue mq) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		return this.defaultMQProducerImpl.send(batch(msgs), queueWithNamespace(mq));
	}

	@Override
	public void send(Collection<Message> msgs, MessageQueue mq, SendCallback sendCallback) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		this.defaultMQProducerImpl.send(batch(msgs), queueWithNamespace(mq), sendCallback);
	}

	@Override
	public SendResult send(Collection<Message> msgs, MessageQueue mq, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		return this.defaultMQProducerImpl.send(batch(msgs), queueWithNamespace(mq), timeout);
	}

	@Override
	public void send(Collection<Message> msgs, MessageQueue mq, SendCallback sendCallback, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		this.defaultMQProducerImpl.send(batch(msgs), queueWithNamespace(mq), sendCallback, timeout);
	}

	@Override
	public Message request(Message msg, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		return this.defaultMQProducerImpl.request(msg, timeout);
	}

	@Override
	public void request(Message msg, RequestCallback requestCallback, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQProducerImpl.request(msg, requestCallback, timeout);
	}

	@Override
	public Message request(Message msg, MessageQueue mq, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		return this.defaultMQProducerImpl.request(msg, mq, timeout);
	}

	@Override
	public void request(Message msg, MessageQueue mq, RequestCallback requestCallback, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQProducerImpl.request(msg, mq, requestCallback, timeout);
	}

	@Override
	public Message request(Message msg, MessageQueueSelector selector, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		return this.defaultMQProducerImpl.request(msg, selector, timeout);
	}

	@Override
	public void request(Message msg, MessageQueueSelector selector, RequestCallback requestCallback, long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQProducerImpl.request(msg, selector, requestCallback, timeout);
	}

	@Override
	public void createTopic(String key, String newTopic, int queueNum, Map<String, String> attributes) throws MQClientException {
		this.defaultMQProducerImpl.createTopic(key, withNamespace(newTopic), queueNum, 0);
	}

	@Override
	public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag, Map<String, String> attributes) throws MQClientException {
		this.defaultMQProducerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
	}

	@Override
	public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
		return this.defaultMQProducerImpl.searchOffset(queueWithNamespace(mq), timestamp);
	}

	@Override
	public long maxOffset(MessageQueue mq) throws MQClientException {
		return this.defaultMQProducerImpl.maxOffset(queueWithNamespace(mq));
	}

	@Override
	public long minOffset(MessageQueue mq) throws MQClientException {
		return this.defaultMQProducerImpl.minOffset(queueWithNamespace(mq));
	}

	@Override
	public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
		return this.defaultMQProducerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
	}

	@Override
	public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws MQClientException, InterruptedException {
		return this.defaultMQProducerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
	}

	@Override
	public MessageExt viewMessage(String topic, String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
		try {
			return this.defaultMQProducerImpl.viewMessage(topic, msgId);
		}
		catch (Exception ignored) {
		}
		return this.defaultMQProducerImpl.queryMessageByUniqKey(withNamespace(topic), msgId);
	}

	public void setCallbackExecutor(final ExecutorService callbackExecutor) {
		this.defaultMQProducerImpl.setCallbackExecutor(callbackExecutor);
	}

	public void setAsyncSenderExecutor(final ExecutorService asyncSenderExecutor) {
		this.defaultMQProducerImpl.setAsyncSenderExecutor(asyncSenderExecutor);
	}

	public void addRetryResponse(int responseCode) {
		this.retryResponseCodes.add(responseCode);
	}

	public int getBatchMaxDelayMs() {
		if (this.produceAccumulator == null) {
			return 0;
		}
		return this.produceAccumulator.getBatchMaxDelayMs();
	}

	public void batchMaxDelayMs(int holdMs) {
		if (this.produceAccumulator == null) {
			throw new UnsupportedOperationException("The currently constructed producer does not support batchMaxDelayMs");
		}
		this.produceAccumulator.batchMaxDelayMs(holdMs);
	}

	public long getBatchMaxBytes() {
		if (this.produceAccumulator == null) {
			return 0;
		}
		return produceAccumulator.getBatchMaxBytes();
	}

	public void batchMaxBytes(long holdSize) {
		if (this.produceAccumulator == null) {
			throw new UnsupportedOperationException("The currently constructed producer does not support batchMaxBytes");
		}
		this.produceAccumulator.batchMaxBytes(holdSize);
	}

	public long getTotalBatchMaxBytes() {
		if (this.produceAccumulator == null) {
			return 0;
		}

		return this.produceAccumulator.getTotalBatchMaxBytes();
	}

	public void totalBatchMaxBytes(long totalHoldSize) {
		if (this.produceAccumulator == null) {
			throw new UnsupportedOperationException("The currently constructed producer does not support totalBatchMaxBytes");
		}

		this.produceAccumulator.totalBatchMaxBytes(totalHoldSize);
	}

	public boolean getAutoBatch() {
		if (this.produceAccumulator == null) {
			return false;
		}
		return this.authBatch;
	}

	public void setAutoBatch(boolean autoBatch) {
		if (this.produceAccumulator == null) {
			throw new UnsupportedOperationException("The currently constructed producer does not support autoBatch");
		}
		this.authBatch = autoBatch;
	}

	public boolean isSendMessageWithVIPChannel() {
		return isVipChannelEnabled();
	}

	public void setSendMessageWithVIPChannel(final boolean sendMessageWithVIPChannel) {
		this.setVipChannelEnabled(sendMessageWithVIPChannel);
	}

	public long[] getNotAvailableDuration() {
		return this.defaultMQProducerImpl.getNotAvailableDuration();
	}

	public void setNotAvailableDuration(final long[] notAvailableDuration) {
		this.defaultMQProducerImpl.setNotAvailableDuration(notAvailableDuration);
	}

	public long[] getLatencyMax() {
		return this.defaultMQProducerImpl.getLatencyMax();
	}

	public void sendLatencyMax(final long[] latencyMax) {
		this.defaultMQProducerImpl.setLatencyMax(latencyMax);
	}

	public boolean isSendLatencyFaultEnable() {
		return this.defaultMQProducerImpl.isSendLatencyFaultEnable();
	}

	public void sendSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
		this.defaultMQProducerImpl.setSendLatencyFaultEnable(sendLatencyFaultEnable);
	}

	public void setBackPressureForAsyncSendNum(int backPressureForAsyncSendNum) {
		this.backPressureForAsyncSendNum = backPressureForAsyncSendNum;
		this.defaultMQProducerImpl.setSemaphoreAsyncSendNum(backPressureForAsyncSendNum);
	}

	public void setBackpressureForAsyncSendSize(int backpressureForAsyncSendSize) {
		this.backPressureForAsyncSendSize = backpressureForAsyncSendSize;
		this.defaultMQProducerImpl.setSemaphoreAsyncSendSize(backpressureForAsyncSendSize);
	}

	private boolean canBatch(Message msg) {
		if (!produceAccumulator.tryAddMessage(msg)) {
			return false;
		}
		if (msg.getDelayTimeLevel() > 0 || msg.getDelayTimeMs() > 0 || msg.getDelayTimeSec() > 0 || msg.getDeliverTimeMs() > 0) {
			return false;
		}
		if (msg.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
			return false;
		}
		if (msg.getProperties().containsKey(MessageConst.PROPERTY_PRODUCER_GROUP)) {
			return false;
		}
		return true;
	}

	private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
		MessageBatch batch;
		try {
			batch = MessageBatch.generateFromList(msgs);
			for (Message msg : batch) {
				Validators.checkMessage(msg, this);
				MessageClientIDSetter.setUniqID(msg);
				msg.setTopic(batch.getTopic());
			}
			MessageClientIDSetter.setUniqID(batch);
			batch.setBody(batch.encode());
		}
		catch (Exception e) {
			throw new MQClientException("Failed to initiate the MessageBatch", e);
		}
		batch.setTopic(withNamespace(batch.getTopic()));
		return batch;
	}
}
