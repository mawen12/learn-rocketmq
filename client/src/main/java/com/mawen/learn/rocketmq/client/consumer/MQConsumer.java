package com.mawen.learn.rocketmq.client.consumer;

import java.util.Set;

import com.mawen.learn.rocketmq.client.MQAdmin;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public interface MQConsumer extends MQAdmin {

	void sendMessageBack(final MessageExt msg, final int delayLevel) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void sendMessageBack(final MessageExt msg, final int delayLevel, final String brokerName) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	Set<MessageQueue> fetchSubscribeMessageQueue(final String topic) throws MQClientException;
}
