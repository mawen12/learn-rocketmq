package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.Set;

import com.mawen.learn.rocketmq.common.consumer.ConsumeFromWhere;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
public interface MQConsumerInner {

	String groupName();

	MessageModel messageModel();

	ConsumeType consumeType();

	ConsumeFromWhere consumeFromWhere();

	Set<SubscriptionData> subscriptions();

	void doBalance();

	boolean tryBalance();

	void persistConsumerOffset();

	void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);

	boolean isSubscribeTopicNeedUpdate(final String topic);

	boolean isUnitMode();

	ConsumerRunningInfo consumerRunningInfo();
}
