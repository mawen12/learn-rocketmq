package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.mawen.learn.rocketmq.client.consumer.listener.MessageListener;
import com.mawen.learn.rocketmq.client.consumer.store.OffsetStore;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageContext;
import com.mawen.learn.rocketmq.client.hook.ConsumeMessageHook;
import com.mawen.learn.rocketmq.client.hook.FilterMessageHook;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.ServiceState;
import com.mawen.learn.rocketmq.common.consumer.ConsumeFromWhere;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
public class DefaultMQPushConsumerImpl implements MQConsumerInner {
	private static final Logger log = LoggerFactory.getLogger(DefaultMQPushConsumerImpl.class);

	private static final long PULL_TIME_DELAY_MILLIS_WHEN_CACHE_FLOW_CONTROL = 50;
	private static final long PULL_TIME_DELAY_MILLIS_WHEN_BROKER_FLOW_CONTROL = 20;
	private static final long PULL_TIME_DELAY_MILLIS_WHEN_SUSPEND = 1000;
	private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 15 * 1000;
	private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 30 * 1000;
	private static final int MAX_POP_INVISIBLE_TIME = 300000;
	private static final int MIN_POP_INVISIBLE_TIME = 5000;
	private static final int ASYNC_TIMEOUT = 3000;
	private static boolean doNotUpdateTopicSubscribeInfoWhenSubscriptionChange = false;

	private long pullTimeDelayMillisWhenException = 3000;

	private final DefaultMQPushConsumer defaultMQPushConsumer;
	private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
	private final List<FilterMessageHook> filterMessageHookList = new ArrayList<>();
	private final long consumerStartTimestamp = System.currentTimeMillis();
	private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
	private final RPCHook rpcHook;

	private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

	private MQClientInstance mqClientFactory;
	private PullAPIWrapper pullAPIWrapper;
	private volatile boolean pause = false;
	private boolean consumeOrderly = false;
	private MessageListener messageListenerInner;
	private OffsetStore offsetStore;
	private ConsumeMessageService consumeMessageService;
	private ConsumeMessageService consumeMessagePopService;
	private long queueFlowControlTimes = 0;
	private long queueMaxSpanFlowControlTimes = 0;

	private final int[] popDelayLevel = {10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};

	public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
		this.defaultMQPushConsumer = defaultMQPushConsumer;
		this.rpcHook = rpcHook;
		this.pullTimeDelayMillisWhenException = defaultMQPushConsumer.getPullTimeDelayMillisWhenException();
	}

	public void registerFilterMessageHook(final FilterMessageHook hook) {
		this.filterMessageHookList.add(hook);
		log.info("register filterMessageHook Hook, {}", hook.hookName());
	}

	public boolean hasHook() {
		return !this.filterMessageHookList.isEmpty();
	}

	public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
		this.consumeMessageHookList.add(hook);
		log.info("register consumeMessageHook Hook, {}", hook.hookName());
	}

	public void executeHookBefore(final ConsumeMessageContext context) {
		if (!this.consumeMessageHookList.isEmpty()) {

		}
	}

	@Override
	public String groupName() {
		return "";
	}

	@Override
	public MessageModel messageModel() {
		return null;
	}

	@Override
	public ConsumeType consumeType() {
		return null;
	}

	@Override
	public ConsumeFromWhere consumeFromWhere() {
		return null;
	}

	@Override
	public Set<SubscriptionData> subscriptions() {
		return Set.of();
	}

	@Override
	public void doBalance() {

	}

	@Override
	public boolean tryBalance() {
		return false;
	}

	@Override
	public void persistConsumerOffset() {

	}

	@Override
	public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {

	}

	@Override
	public boolean isSubscribeTopicNeedUpdate(String topic) {
		return false;
	}

	@Override
	public boolean isUnitMode() {
		return false;
	}

	@Override
	public ConsumerRunningInfo consumerRunningInfo() {
		return null;
	}
}
