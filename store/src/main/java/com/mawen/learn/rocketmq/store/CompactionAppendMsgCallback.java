package com.mawen.learn.rocketmq.store;

import java.nio.ByteBuffer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
public interface CompactionAppendMsgCallback {

	AppendMessageResult doAppend(ByteBuffer buffer, long fileFromOffset, int maxBlank, ByteBuffer bbSrc);
}
