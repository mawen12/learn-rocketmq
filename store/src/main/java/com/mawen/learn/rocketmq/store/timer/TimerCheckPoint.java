package com.mawen.learn.rocketmq.store.timer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;
import com.mawen.learn.rocketmq.store.logfile.DefaultMappedFile;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
@Getter
@Setter
public class TimerCheckPoint {

	private static final Logger log = LoggerFactory.getLogger(TimerCheckPoint.class);

	private final RandomAccessFile randomAccessFile;
	private final FileChannel fileChannel;
	private final MappedByteBuffer mappedByteBuffer;

	private volatile long lastReadTimeMs = 0;
	private volatile long lastTimerLogFlushPos = 0;
	private volatile long lastTimerQueueOffset = 0;
	private volatile long masterTimerQueueOffset = 0;

	private final DataVersion dataVersion = new DataVersion();

	public TimerCheckPoint() {
		this.randomAccessFile = null;
		this.fileChannel = null;
		this.mappedByteBuffer = null;
	}

	public TimerCheckPoint(String scpPath) throws IOException {
		File file = new File(scpPath);
		UtilAll.ensureDirOK(file.getParent());
		boolean fileExists = file.exists();

		this.randomAccessFile = new RandomAccessFile(file, "rw");
		this.fileChannel = randomAccessFile.getChannel();
		this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, DefaultMappedFile.OS_PAGE_SIZE);

		if (fileExists) {
			log.info("timer checkpoint file exists, {}", scpPath);
			this.lastReadTimeMs = mappedByteBuffer.getLong(0);
			this.lastTimerLogFlushPos = mappedByteBuffer.getLong(8);
			this.lastTimerQueueOffset = mappedByteBuffer.getLong(16);
			this.masterTimerQueueOffset = mappedByteBuffer.getLong(24);

			if (mappedByteBuffer.hasRemaining()) {
				dataVersion.setStateVersion(mappedByteBuffer.getLong(32));
				dataVersion.setTimestamp(mappedByteBuffer.getLong(40));
				dataVersion.setCounter(new AtomicLong(mappedByteBuffer.getLong(48)));
			}

			log.info("timer checkpoint file lastReadTimeMs {}, {}", lastReadTimeMs, UtilAll.timeMillisToHumanString(lastReadTimeMs));
			log.info("timer checkpoint file lastTimerLogFlushPos {}", lastTimerLogFlushPos);
			log.info("timer checkpoint file lastTimerQueueOffset {}", lastTimerQueueOffset);
			log.info("timer checkpoint file masterTimerQueueOffset {}", masterTimerQueueOffset);
			log.info("timer checkpoint file data version state version {}", dataVersion.getStateVersion());
			log.info("timer checkpoint file data version timestamp {}", dataVersion.getTimestamp());
			log.info("timer checkpoint file data version version counter {}", dataVersion.getCounter());
		}
		else {
			log.info("timer checkpoint file not exists. {}", scpPath);
		}
	}

	public void shutdown() {
		if (mappedByteBuffer == null) {
			return;
		}

		flush();

		UtilAll.cleanBuffer(mappedByteBuffer);

		try {
			fileChannel.close();
		}
		catch (IOException e) {
			log.error("Shutdown error in timer check point", e);
		}
	}

	public void flush() {
		if (mappedByteBuffer == null) {
			return;
		}

		mappedByteBuffer.putLong(0, lastReadTimeMs);
		mappedByteBuffer.putLong(8, lastTimerLogFlushPos);
		mappedByteBuffer.putLong(16, lastTimerQueueOffset);
		mappedByteBuffer.putLong(24, masterTimerQueueOffset);
		mappedByteBuffer.putLong(32, dataVersion.getStateVersion());
		mappedByteBuffer.putLong(40, dataVersion.getTimestamp());
		mappedByteBuffer.putLong(48, dataVersion.getCounter().get());

		mappedByteBuffer.force();
	}

	public static ByteBuffer encode(TimerCheckPoint another) {
		ByteBuffer buffer = ByteBuffer.allocate(56);
		buffer.putLong(another.getLastReadTimeMs());
		buffer.putLong(another.getLastTimerLogFlushPos());
		buffer.putLong(another.getLastTimerQueueOffset());
		buffer.putLong(another.getMasterTimerQueueOffset());
		buffer.putLong(another.getDataVersion().getStateVersion());
		buffer.putLong(another.getDataVersion().getTimestamp());
		buffer.putLong(another.getDataVersion().getCounter().get());
		buffer.flip();

		return buffer;
	}

	public static TimerCheckPoint decode(ByteBuffer buffer) {
		TimerCheckPoint tmp = new TimerCheckPoint();
		tmp.setLastReadTimeMs(buffer.getLong());
		tmp.setLastTimerLogFlushPos(buffer.getLong());
		tmp.setLastTimerQueueOffset(buffer.getLong());
		tmp.setMasterTimerQueueOffset(buffer.getLong());

		if (buffer.hasRemaining()) {
			tmp.getDataVersion().setStateVersion(buffer.getLong());
			tmp.getDataVersion().setTimestamp(buffer.getLong());
			tmp.getDataVersion().setCounter(new AtomicLong(buffer.getLong()));
		}
		return tmp;
	}

	public void updateDataVersion(long stateVersion) {
		dataVersion.nextVersion(stateVersion);
	}
}
