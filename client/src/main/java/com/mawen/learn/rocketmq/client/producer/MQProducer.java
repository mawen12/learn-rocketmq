package com.mawen.learn.rocketmq.client.producer;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.mawen.learn.rocketmq.client.MQAdmin;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public interface MQProducer extends MQAdmin {

	void start() throws MQClientException;

	void shutdown();

	List<MessageQueue> fetchPublishMessageQueues(final String topic) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	// region common send
	SendResult send(final Message msg) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	SendResult send(final Message msg, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void send(final Message msg, final SendCallback sendCallback) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void send(final Message msg, final SendCallback sendCallback, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void sendOneway(final Message msg) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;
	// endregion

	// region messageQueue
	SendResult send(final Message msg, final MessageQueue mq) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	SendResult send(final Message msg, final MessageQueue mq, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void sendOneway(final Message msg, final MessageQueue mq) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;
	// endregion

	// region messageQueueSelector
	SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void send(final Message msg, final MessageQueueSelector selector, final Object arg, final SendCallback sendCallback) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void send(final Message msg, final MessageQueueSelector selector, final Object arg, final SendCallback sendCallback, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;
	// endregion

	TransactionResult sendMessageInTransaction(final Message msg, final Object arg) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	// region batch send
	SendResult send(final Collection<Message> msgs) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void send(final Collection<Message> msgs, final SendCallback sendCallback) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	SendResult send(final Collection<Message> msgs, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void send(final Collection<Message> msgs, final SendCallback sendCallback, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	SendResult send(final Collection<Message> msgs, final MessageQueue mq) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void send(final Collection<Message> msgs, final MessageQueue mq, final SendCallback sendCallback) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	SendResult send(final Collection<Message> msgs, final MessageQueue mq, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void send(final Collection<Message> msgs, final MessageQueue mq, final SendCallback sendCallback, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;
	// endregion

	Message request(final Message msg, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void request(final Message msg, final RequestCallback requestCallback, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	Message request(final Message msg, final MessageQueue mq, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	Message request(final Message msg, final MessageQueueSelector selector, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;

	void request(final Message msg, final MessageQueueSelector selector, final RequestCallback requestCallback, final long timeout) throws RemotingException, MQClientException, MQBrokerException, InterruptedException;


}
