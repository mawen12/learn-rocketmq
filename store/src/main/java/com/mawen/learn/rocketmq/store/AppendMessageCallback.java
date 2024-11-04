package com.mawen.learn.rocketmq.store;

import java.nio.ByteBuffer;

import com.mawen.learn.rocketmq.common.message.MessageExtBatch;
import com.mawen.learn.rocketmq.common.message.MessageExtBrokerInner;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
public interface AppendMessageCallback {

	AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer buffer, final int maxBlank, final MessageExtBrokerInner msg, PutMessageContext putMessageContext);

	AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer buffer, final int maxBlank, final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext);
}
