package com.mawen.learn.rocketmq.store;

import java.util.concurrent.CompletableFuture;

import com.mawen.learn.rocketmq.common.message.MessageExtBatch;
import com.mawen.learn.rocketmq.common.message.MessageExtBrokerInner;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
public interface MessageStore {

	boolean load();

	void start() throws Exception;

	void shutdown();

	void destroy();

	default CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
		return CompletableFuture.completedFuture(putMessage(msg));
	}

	default CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBatch messageExtBatch) {
		return CompletableFuture.completedFuture(putMessages(messageExtBatch));
	}

	PutMessageResult putMessage(final MessageExtBrokerInner msg);

	PutMessageResult putMessages(final MessageExtBatch messageExtBatch);


}
