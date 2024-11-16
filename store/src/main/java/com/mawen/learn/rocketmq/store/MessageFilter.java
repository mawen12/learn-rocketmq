package com.mawen.learn.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
public interface MessageFilter {

	boolean isMatchedByConsumeQueue(final Long tagsCode, final ConsumeQueueExt.CqUnit cqUnit);

	boolean isMatchedByCommitLog(final ByteBuffer buffer, final Map<String, String> properties);
}
