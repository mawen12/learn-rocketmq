package com.mawen.learn.rocketmq.store.rocksdb;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.store.MessageStore;
import lombok.RequiredArgsConstructor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.AbstractCompactionFilter;
import org.rocksdb.AbstractCompactionFilterFactory;
import org.rocksdb.RemoveConsumeQueueCompactionFilter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
@RequiredArgsConstructor
public class ConsumeQueueCompactionFilterFactory extends AbstractCompactionFilterFactory<RemoveConsumeQueueCompactionFilter> {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKSDB_LOGGER_NAME);

	private final MessageStore messageStore;

	@Override
	public String name() {
		return "ConsumeQueueCompactionFilterFactory";
	}

	@Override
	public RemoveConsumeQueueCompactionFilter createCompactionFilter(AbstractCompactionFilter.Context context) {
		long minPhyOffset = messageStore.getMinPhyOffset();
		log.info("manualCompaction minPhyOffset: {}, isFull: {}, isManual: {}",
				minPhyOffset, context.isFullCompaction(), context.isManualCompaction());
		return new RemoveConsumeQueueCompactionFilter(minPhyOffset);
	}
}
