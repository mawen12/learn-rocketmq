package com.mawen.learn.rocketmq.client.impl.producer;

import java.util.Set;

import com.mawen.learn.rocketmq.client.producer.TransactionCheckListener;
import com.mawen.learn.rocketmq.client.producer.TransactionListener;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
public interface MQProducerInner {

	Set<String> getPublishTopicList();

	boolean isPublishTopicNeedUpdate(final String topic);

	TransactionCheckListener checkListener();

	TransactionListener getCheckListener();

	void checkTransactionState(String addr, MessageExt msg, CheckTransactionStateRequestHeader checkRequestHeader);

	void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);

	boolean isUnitMode();
}
