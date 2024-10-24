package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.List;

import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public interface ConsumeMessageService {

	void start();

	void shutdown(long awaitTerminateMillis);

	void incCorePoolSize();

	void decCorePoolSize();

	int getCorePoolSize();

	ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String brokerName);

	void submitConsumeRequest(final List<MessageExt> msgs, final ProcessQueue processQueue, final MessageQueue messageQueue, final boolean dispathToConsume);

	void submitConsumeRequest(final List<MessageExt> msgs, final PopProcessQueue popProcessQueue, final MessageQueue messageQueue);
}
