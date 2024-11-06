package com.mawen.learn.rocketmq.store;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
public class CommitLog implements Swappable {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	public static final int MESSAGE_MAGIC_CODE = -626843481;
	public static final int BLANK_MAGIC_CODE = -875286124;
	public static final int CRC32_RESERVED_LEN = MessageConst.PROPERTY_CRC32.length() + 1 + 10 + 1;

	protected final MappedFileQueue mappedFileQueue;
	protected final DefaultMessageStore defaultMessageStore;


}
