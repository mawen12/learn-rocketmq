package com.mawen.learn.rocketmq.store;

import java.io.File;
import java.nio.ByteBuffer;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.store.config.StorePathConfigHelper;
import com.mawen.learn.rocketmq.store.queue.ConsumeQueueInterface;
import com.mawen.learn.rocketmq.store.queue.FileQueueLifeCycle;
import io.netty.buffer.ByteBuf;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
public class ConsumeQueue implements ConsumeQueueInterface, FileQueueLifeCycle {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
	private static final Logger logError = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

	public static final int CQ_STORE_UNIT_SIZE = 10;
	public static final int MSG_TAG_OFFSET_INDEX = 12;

	private final MessageStore messageStore;

	private final MappedFileQueue mappedFileQueue;
	private final String topic;
	private final int queueId;
	private final ByteBuffer byteBufferIndex;

	private final String storePath;
	private final int mappedFileSize;
	private long maxPhysicOffset = -1;

	private volatile long minLogicOffset = 0;
	private ConsumeQueueExt consumeQueueExt;

	public ConsumeQueue(final String topic, final int queueId, final String storePath, final int mappedFileSize, final MessageStore messageStore) {
		this.topic = topic;
		this.queueId = queueId;
		this.storePath = storePath;
		this.mappedFileSize = mappedFileSize;
		this.messageStore = messageStore;

		String queueDir = storePath + File.separator + topic + File.separator + queueId;

		this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

		this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

		if (messageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
			this.consumeQueueExt = new ConsumeQueueExt(topic, queueId,
					StorePathConfigHelper.getStorePathConsumeQueueExt(messageStore.getMessageStoreConfig().getStorePathRootDir()),
					messageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
					messageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt());
		}
	}


}
