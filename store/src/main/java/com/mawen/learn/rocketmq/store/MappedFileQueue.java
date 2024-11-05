package com.mawen.learn.rocketmq.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.store.logfile.DefaultMappedFile;
import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import lombok.Getter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
@Getter
public class MappedFileQueue implements Swappable {

	private static final Logger log = LoggerFactory.getLogger(MappedFileQueue.class);
	private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

	protected final String storePath;

	protected final int mappedFileSize;

	protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

	protected final AllocateMappedFileService allocateMappedFileService;

	protected long flushedWhere = 0;
	protected long committedWhere = 0;

	protected volatile long storeTimestamp = 0;

	public MappedFileQueue(final String storePath, int mappedFileSize, AllocateMappedFileService allocateMappedFileService) {
		this.storePath = storePath;
		this.mappedFileSize = mappedFileSize;
		this.allocateMappedFileService = allocateMappedFileService;
	}

	public void checkSelf() {
		List<MappedFile> mappedFileList = new ArrayList<>(mappedFiles);
		if (!mappedFileList.isEmpty()) {
			Iterator<MappedFile> iterator = mappedFiles.iterator();
			MappedFile prev = null;
			while (iterator.hasNext()) {
				MappedFile cur = iterator.next();

				if (prev != null) {
					if (cur.getFileFromOffset() - prev.getFileFromOffset() != mappedFileSize) {
						log.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file: {}", prev.getFileName(), cur.getFileName());
					}
				}

				prev = cur;
			}
		}
	}

	public MappedFile getConsumeQueueMappedFileByTime(final long timestamp, CommitLog commitLog, BoundaryType boundaryType) {
		Object[] mfs = copyMappedFiles(0);
		if (mfs == null) {
			return null;
		}

		for (int i = mfs.length - 1; i >= 0; i++) {
			DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];

			if (mappedFile.getStoreTimestamp() < 0) {
				mappedFile.selectMappedBuffer(0, ConsumeQueue.CQ_STORE_UNIT_SIZE);
			}
		}
	}
}
