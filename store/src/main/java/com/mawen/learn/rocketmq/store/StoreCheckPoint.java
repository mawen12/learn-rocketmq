package com.mawen.learn.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.store.logfile.DefaultMappedFile;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * <p>File Format
 * <pre>
 *
 * +----------------------+----------------------+---------------------+-----------------------+-----------------------+
 * | physic msg timestamp | logics msg timestamp | index msg timestamp | master flushed offset | confirm physic offset |
 * +----------------------+----------------------+---------------------+-----------------------+-----------------------+
 * | 8 bytes              | 8 bytes              | 8 bytes             | 8 bytes               | 8 bytes               |
 * +----------------------+----------------------+---------------------+-----------------------+-----------------------+
 * </pre>
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
@Setter
@Getter
public class StoreCheckPoint {

	private static final Logger log = LoggerFactory.getLogger(StoreCheckPoint.class);

	private final RandomAccessFile randomAccessFile;
	private final FileChannel fileChannel;
	private final MappedByteBuffer mappedByteBuffer;

	private volatile long physicMsgTimestamp = 0;
	private volatile long logicsMsgTimestamp = 0;
	private volatile long indexMsgTimestamp = 0;
	private volatile long masterFlushedOffset = 0;
	private volatile long confirmPhyOffset = 0;

	public StoreCheckPoint(final String scpPath) throws IOException {
		File file = new File(scpPath);
		UtilAll.ensureDirOK(file.getParent());
		boolean exists = file.exists();

		this.randomAccessFile = new RandomAccessFile(file, "rw");
		this.fileChannel = randomAccessFile.getChannel();
		this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, DefaultMappedFile.OS_PAGE_SIZE);

		if (exists) {
			log.info("store checkpoint file exists, {}", scpPath);

			this.physicMsgTimestamp = mappedByteBuffer.getLong(0);
			this.logicsMsgTimestamp = mappedByteBuffer.getLong(8);
			this.indexMsgTimestamp = mappedByteBuffer.getLong(16);
			this.masterFlushedOffset = mappedByteBuffer.getLong(24);
			this.confirmPhyOffset = mappedByteBuffer.getLong(32);

			log.info("store checkpoint file physicMsgTimestamp {}, {}", physicMsgTimestamp, UtilAll.timeMillisToHumanString(physicMsgTimestamp));
			log.info("store checkpoint file logicsMsgTimestamp {}, {}", logicsMsgTimestamp, UtilAll.timeMillisToHumanString(logicsMsgTimestamp));
			log.info("store checkpoint file indexMsgTimestamp {}, {}", indexMsgTimestamp, UtilAll.timeMillisToHumanString(indexMsgTimestamp));
			log.info("store checkpoint file masterFlushedOffset {}", masterFlushedOffset);
			log.info("store checkpoint file confirmPhyOffset {}", confirmPhyOffset);
		}
		else {
			log.info("store checkpoint file not exist, {}", scpPath);
		}
	}

	public void shutdown() {
		flush();

		UtilAll.cleanBuffer(mappedByteBuffer);

		try {
			fileChannel.close();
		}
		catch (IOException e) {
			log.error("Failed to properly close the channel", e);
		}
	}

	public void flush() {
		mappedByteBuffer.putLong(0, physicMsgTimestamp);
		mappedByteBuffer.putLong(8, logicsMsgTimestamp);
		mappedByteBuffer.putLong(16, indexMsgTimestamp);
		mappedByteBuffer.putLong(24, masterFlushedOffset);
		mappedByteBuffer.putLong(32, confirmPhyOffset);
		mappedByteBuffer.force();
	}

	public long getMinTimestampIndex() {
		return Math.min(getMinTimestamp(), indexMsgTimestamp);
	}

	public long getMinTimestamp() {
		long min = Math.min(physicMsgTimestamp, logicsMsgTimestamp);

		min -= 3 * 1000;
		if (min < 0) {
			min = 0;
		}

		return min;
	}
}
