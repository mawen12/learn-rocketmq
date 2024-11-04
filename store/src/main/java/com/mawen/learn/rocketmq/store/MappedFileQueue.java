package com.mawen.learn.rocketmq.store;

import java.util.concurrent.CopyOnWriteArrayList;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
public class MappedFileQueue implements Swappable {

	private static final Logger log = LoggerFactory.getLogger(MappedFileQueue.class);
	private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

	protected final String storePath;

	protected final int mappedFileSize;

	protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

	protected final AllocateMappedFileService allocateMappedFileService;
}
