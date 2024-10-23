package com.mawen.learn.rocketmq.client.impl.consumer;

import com.mawen.learn.rocketmq.common.message.MessageRequestMode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
public interface MessageRequest {

	MessageRequestMode getMessageRequestMode();
}
