package com.mawen.learn.rocketmq.client.consumer;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.QueryResult;
import com.mawen.learn.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import com.mawen.learn.rocketmq.client.consumer.store.OffsetStore;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.message.MessageDecoder;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
@Setter
public class DefaultMQPullConsumer extends ClientConfig implements MQPullConsumer {

	protected final transient DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;

	private String consumeGroup;

	private long brokerSuspendMaxTimeMillis = 20 * 1000;

	private long consumerTimeoutMillisWhenSuspend = 10 * 1000;

	private MessageModel messageModel = MessageModel.CLUSTERING;

	private MessageQueueListener messageQueueListener;

	private OffsetStore offsetStore;

	private Set<String> registerTopics = new HashSet<>();

	private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();

	private boolean unitMode = false;

	private int maxReconsumeTimes = 16;

	public DefaultMQPullConsumer(String consumerGroup) {
		this(consumerGroup, null);
	}

	public DefaultMQPullConsumer(RPCHook rpcHook) {
		this(MixAll.DEFAULT_CONSUMER_GROUP, rpcHook);
	}

	public DefaultMQPullConsumer(final String consumerGroup, RPCHook rpcHook) {
		this(null, consumerGroup, rpcHook);
	}

	public DefaultMQPullConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook) {
		this.namespace = namespace;
		this.consumeGroup = consumerGroup;
		this.enableStreamRequestType = true;
		this.defaultMQPullConsumerImpl = new DefaulqMQPullConsumerImpl(this, rpcHook);
	}

	@Override
	public void createTopic(String key, String newTopic, int queueNum, Map<String, String> attributes) throws MQClientException {
		createTopic(key, withNamespace(newTopic), queueNum, 0, null);
	}

	@Override
	public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag, Map<String, String> attributes) throws MQClientException {
		this.defaultMQPullConsumerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
	}

	@Override
	public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
		return this.defaultMQPullConsumerImpl.searchOffset(queueWithNamespace(mq), timestamp);
	}

	@Override
	public long maxOffset(MessageQueue mq) throws MQClientException {
		return this.defaultMQPullConsumerImpl.maxOffset(queueWithNamespace(mq));
	}

	@Override
	public long minOffset(MessageQueue mq) throws MQClientException {
		return this.defaultMQPullConsumerImpl.minOffset(queueWithNamespace(mq));
	}

	@Override
	public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
		return this.defaultMQPullConsumerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
	}

	@Override
	public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end) throws MQClientException, InterruptedException {
		return this.defaultMQPullConsumerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
	}

	@Override
	public MessageExt viewMessage(String topic, String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
		try {
			MessageDecoder.decodeMessageId(msgId);
			return this.defaultMQPullConsumerImpl.viewMessage(topic, msgId);
		}
		catch (Exception ignored) {

		}
		return this.defaultMQPullConsumerImpl.queryMessageByUniqKey(withNamespace(topic), msgId);
	}

	@Override
	public void start() throws MQClientException {
		this.setConsumeGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumeGroup));
		this.defaultMQPullConsumerImpl.start();
	}

	@Override
	public void shutdown() {
		this.defaultMQPullConsumerImpl.shutdown();
	}

	@Override
	public void registerMessageQueueListener(String topic, MessageQueueListener listener) {
		synchronized (this.registerTopics) {
			this.registerTopics.add(withNamespace(topic));
			if (listener != null) {
				this.messageQueueListener = listener;
			}
		}
	}

	@Override
	public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums);
	}

	@Override
	public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, timeout);
	}

	@Override
	public PullResult pull(MessageQueue mq, MessageSelector selector, long offset, int maxNums) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), selector, offset, maxNums);
	}

	@Override
	public PullResult pull(MessageQueue mq, MessageSelector selector, long offset, int maxNums, long timeout) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), selector, offset, maxNums, timeout);
	}

	@Override
	public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback callback) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, callback);
	}

	@Override
	public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback callback, long timeout) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, callback, timeout);
	}

	@Override
	public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, int maxSize, PullCallback callback) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, maxSize, callback);
	}

	@Override
	public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, int maxSize, PullCallback callback, long timeout) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, maxSize, callback, timeout);
	}

	@Override
	public void pull(MessageQueue mq, MessageSelector selector, long offset, int maxNums, int maxSize, PullCallback callback) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), selector, offset, maxNums, maxSize, callback);
	}

	@Override
	public void pull(MessageQueue mq, MessageSelector selector, long offset, int maxNums, int maxSize, PullCallback callback, long timeout) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), selector, offset, maxNums, maxSize, callback, timeout);
	}

	@Override
	public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		return this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), subExpression, offset, maxNums);
	}

	@Override
	public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback callback) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		return this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), subExpression, offset, maxNums, callback);
	}

	@Override
	public PullResult pullBlockIfNotFoundWithMessageSelector(MessageQueue mq, MessageSelector selector, long offset, int maxNums) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		return this.defaultMQPullConsumerImpl.pullBlockIfNotFoundWithMessageSelector(mq, selector, offset, maxNums);
	}

	@Override
	public PullResult pullBlockIfNotFoundWithMessageSelector(MessageQueue mq, MessageSelector selector, long offset, int maxNums, PullCallback callback) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		return this.defaultMQPullConsumerImpl.pullBlockIfNotFoundWithMessageSelector(queueWithNamespace(mq), selector, offset, maxNums, callback);
	}

	@Override
	public void updateConsumeOffset(MessageQueue mq, long offset) throws MQClientException {
		this.defaultMQPullConsumerImpl.updateConsumeOffset(queueWithNamespace(mq), offset);
	}

	@Override
	public long fetchConsumeOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
		return this.defaultMQPullConsumerImpl.fetchConsumeOffset(queueWithNamespace(mq), fromStore);
	}

	@Override
	public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
		return this.defaultMQPullConsumerImpl.fetchMessageQueuesInBalance(withNamespace(topic));
	}

	@Override
	public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName, String consumerGroup) throws MQClientException, RuntimeException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName, consumerGroup);
	}

	@Override
	public void sendMessageBack(MessageExt msg, int delayLevel) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, null);
	}

	@Override
	public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName) throws RemotingException, MQClientException, MQBrokerException, InterruptedException {
		msg.setTopic(withNamespace(msg.getTopic()));
		this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
	}

	@Override
	public Set<MessageQueue> fetchSubscribeMessageQueue(String topic) throws MQClientException {
		return this.defaultMQPullConsumerImpl.fetchSubscribeMessageQueue(withNamespace(topic));
	}
}
