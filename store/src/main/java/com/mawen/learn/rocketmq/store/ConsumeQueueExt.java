package com.mawen.learn.rocketmq.store;

import java.nio.ByteBuffer;

import lombok.Getter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
@Getter
public class ConsumeQueueExt {
	private static final Logger log = LoggerFactory.getLogger(ConsumeQueueExt.class);

	public static final int END_BLANK_DATA_LENGTH = 4;

	public static final long MAX_ADDR = Integer.MIN_VALUE - 1L;

	public static final long MAX_REAL_OFFSET = MAX_ADDR - Long.MIN_VALUE;

	private final MappedFileQueue mappedFileQueue;

	private final String topic;

	private final int queueId;

	private final String storePath;

	private final int mappedFileSize;

	private ByteBuffer tempContainer;

	public static class CqUnit {

		/**
		 * 2 * 1: size, 32k max
		 * 8 * 2: msg time + tagCode
		 * 2: bitMapSize
		 */
		private static final short MIN_EXT_UNIT_SIZE = 2 * 1 + 8 * 2 + 2;



	}
}
