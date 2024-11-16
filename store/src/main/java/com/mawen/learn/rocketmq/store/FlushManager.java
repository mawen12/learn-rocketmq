package com.mawen.learn.rocketmq.store;

import java.util.concurrent.CompletableFuture;

import com.mawen.learn.rocketmq.common.message.MessageExt;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
public interface FlushManager {

	void start();

	void shutdown();

	void wakeUpFlush();

	void wakeUpCommit();

	void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt);

	CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt);
}
