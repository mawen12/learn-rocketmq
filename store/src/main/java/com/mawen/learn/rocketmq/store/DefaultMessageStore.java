package com.mawen.learn.rocketmq.store;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import com.mawen.learn.rocketmq.store.util.PerfCounter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
public class DefaultMessageStore implements MessageStore {

	protected static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
	protected static final Logger ERROR_LOG = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

	private final PerfCounter.Ticks perfs = new PerfCounter.Ticks(LOGGER);

	private final MessageStoreConfig messageStoreConfig;

	protected final CommitLog commitLog;

	protected final ConsumeQueueStoreInterface consumeQueueStore;
}
