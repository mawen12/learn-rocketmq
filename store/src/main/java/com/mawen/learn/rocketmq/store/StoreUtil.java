package com.mawen.learn.rocketmq.store;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
public class StoreUtil {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	public static final long TOTAL_PHYSICAL_MEMORY_SIZE = getTotalPhysicalMemorySize();

	public static long getTotalPhysicalMemorySize() {
		long physicalTotal = 24L * 1024 * 1024 * 1024;
		OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
		if (bean instanceof com.sun.management.OperatingSystemMXBean) {
			physicalTotal = ((com.sun.management.OperatingSystemMXBean) bean).getTotalPhysicalMemorySize();
		}

		return physicalTotal;
	}

	public static void fileAppend(MappedFile file, ByteBuffer data) {
		boolean success = file.appendMessage(data);
		if (!success) {
			throw new RuntimeException(String.format("fileAppend failed for file: %s and data remaining: %d", file, data.remaining()));
		}
	}

	public static FileQueueSnapshot getFileQueueSnapshot(MappedFileQueue mappedFileQueue) {
		return getFileQueueSnapshot(mappedFileQueue, mappedFileQueue.getLastMappedFile().getFileFromOffset());
	}

	public static FileQueueSnapshot getFileQueueSnapshot(MappedFileQueue mappedFileQueue, final long currentFile) {
		try {
			Preconditions.checkNotNull(mappedFileQueue, "file queue shouldn't be null");

			MappedFile firstFile = mappedFileQueue.getFirstMappedFile();
			MappedFile lastFile = mappedFileQueue.getLastMappedFile();
			int mappedFileSize = mappedFileQueue.getMappedFileSize();

			if (firstFile == null || lastFile == null) {
				return new FileQueueSnapshot(firstFile, -1, lastFile, -1, currentFile, -1, 0, false);
			}

			long firstFileIndex = 0;
			long lastFileIndex = (lastFile.getFileFromOffset() - firstFile.getFileFromOffset()) / mappedFileSize;
			long currentFileIndex = (currentFile - firstFile.getFileFromOffset()) / mappedFileSize;
			long behind = (lastFile.getFileFromOffset() - currentFile) / mappedFileSize;
			boolean exist = firstFile.getFileFromOffset() <= currentFile && currentFile <= lastFile.getFileFromOffset();

			return new FileQueueSnapshot(firstFile, firstFileIndex, lastFile, lastFileIndex, currentFile, currentFileIndex, behind, exist);
		}
		catch (Exception e) {
			log.error("[BUG] get file queue snapshot failed. fileQueue: {}, currentFile: {}", mappedFileQueue, currentFile, e);
		}
		return new FileQueueSnapshot();
	}
}
