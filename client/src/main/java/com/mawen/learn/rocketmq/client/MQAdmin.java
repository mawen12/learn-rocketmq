package com.mawen.learn.rocketmq.client;

import java.util.Map;

import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public interface MQAdmin {

	void createTopic(final String key, final String newTopic, final int queueNum, Map<String, String> attributes) throws MQClientException;

	void createTopic(final String key, final String newTopic, final int queueNum, final int topicSysFlag, Map<String, String> attributes) throws MQClientException;

	long searchOffset(final MessageQueue mq, final long timestamp) throws MQClientException;

	long maxOffset(final MessageQueue mq) throws MQClientException;

	long minOffset(final MessageQueue mq) throws MQClientException;

	long earliestMsgStoreTime(final MessageQueue mq) throws MQClientException;

	QueryResult queryMessage(final String topic, final String key, final int maxNum, final long begin, final long end) throws MQClientException, InterruptedException;

	MessageExt viewMessage(String topic, String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;
}
