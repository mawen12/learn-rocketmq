package com.mawen.learn.rocketmq.client.consumer.listener;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
@RequiredArgsConstructor
@Setter
@Getter
public class ConsumeOrderlyContext {
	private final MessageQueue messageQueue;
	private boolean autoCommit = true;
	private long suspendCurrentQueueTimeMillis = -1;
}
