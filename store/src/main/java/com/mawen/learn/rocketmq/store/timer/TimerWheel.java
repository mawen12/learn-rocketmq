package com.mawen.learn.rocketmq.store.timer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.mawen.learn.rocketmq.common.UtilAll;
import io.netty.buffer.ByteBuf;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
public class TimerWheel {

	private static final Logger log = LoggerFactory.getLogger(TimerWheel.class);

	private static final int BLANK = -1, IGNORE = -2;

	public final int slotsTotal;
	public final int precisionMs;

	private String fileName;
	private final RandomAccessFile randomAccessFile;
	private final FileChannel fileChannel;
	private final MappedByteBuffer mappedByteBuffer;
	private final ByteBuffer byteBuffer;
	private final ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<ByteBuffer>() {
		@Override
		protected ByteBuffer initialValue() {
			return byteBuffer.duplicate();
		}
	};
	private final int wheelLength;

	public TimerWheel(String fileName, int slotsTotal, int precisionMs) throws IOException {
		this.fileName = fileName;
		this.slotsTotal = slotsTotal;
		this.precisionMs = precisionMs;
		this.wheelLength = slotsTotal * 2 * Slot.SIZE;

		File file = new File(fileName);
		UtilAll.ensureDirOK(file.getParent());

		try {
			this.randomAccessFile = new RandomAccessFile(fileName, "rw");
			if (file.exists() && randomAccessFile.length() != 0 && randomAccessFile.length() != wheelLength) {
				throw new RuntimeException(String.format("Timer wheel length:%d != expected:%d", randomAccessFile.length(), wheelLength));
			}
			randomAccessFile.setLength(wheelLength);
			fileChannel = randomAccessFile.getChannel();
			mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, wheelLength);

			assert wheelLength == mappedByteBuffer.remaining();

			byteBuffer = ByteBuffer.allocateDirect(wheelLength);
			byteBuffer.put(mappedByteBuffer);
		}
		catch (FileNotFoundException e) {
			log.error("create file channel {} Failed.", fileName, e);
			throw e;
		}
		catch (IOException e) {
			log.error("map file {} Failed.", fileName, e);
			throw e;
		}
	}

	public void shutdown() {
		shutdown(true);
	}

	public void shutdown(boolean flush) {
		if (flush) {
			flush();
		}

		UtilAll.cleanBuffer(mappedByteBuffer);
		UtilAll.cleanBuffer(byteBuffer);

		try {
			fileChannel.close();
		}
		catch (IOException e) {
			log.error("Shutdown error in timer wheel", e);
		}
	}

	public void flush() {
		ByteBuffer buffer = localBuffer.get();
		buffer.position(0);
		buffer.limit(wheelLength);

		mappedByteBuffer.position(0);
		mappedByteBuffer.limit(wheelLength);

		for (int i = 0; i < wheelLength; i++) {
			if (buffer.get(i) != mappedByteBuffer.get(i)) {
				mappedByteBuffer.put(i, buffer.get(i));
			}
		}

		mappedByteBuffer.force();
	}

	public Slot getSlot(long timeMs) {
		Slot slot = getRawSlot(timeMs);
		if (slot.timeMs != timeMs / precisionMs * precisionMs) {
			return new Slot(-1, -1, -1);
		}
		return slot;
	}

	public Slot getRawSlot(long timeMs) {
		localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
		return new Slot(localBuffer.get().getLong() * precisionMs,
				localBuffer.get().getLong(),
				localBuffer.get().getLong(),
				localBuffer.get().getInt(),
				localBuffer.get().getInt());
	}

	public int getSlotIndex(long timeMs) {
		return (int) (timeMs / precisionMs * (slotsTotal * 2));
	}

	public void putSlot(long timeMs, long firstPos, long lastPos) {
		localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
		localBuffer.get().putLong(timeMs / precisionMs);
		localBuffer.get().putLong(firstPos);
		localBuffer.get().putLong(lastPos);
	}

	public void putSlot(long timeMs, long firstPos, long lastPos, int num, int magic) {
		localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
		localBuffer.get().putLong(timeMs / precisionMs);
		localBuffer.get().putLong(firstPos);
		localBuffer.get().putLong(lastPos);
		localBuffer.get().putInt(num);
		localBuffer.get().putInt(magic);
	}

	public void reviseLot(long timeMs, long firstPos, long lastPos, boolean force) {
		localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);

		if (timeMs / precisionMs != localBuffer.get().getLong()) {
			if (force) {
				putSlot(timeMs, firstPos != IGNORE ? firstPos : lastPos, lastPos);
			}
		}
		else {
			if (firstPos != IGNORE) {
				localBuffer.get().putLong(firstPos);
			}
			else {
				localBuffer.get().getLong();
			}

			if (IGNORE != lastPos) {
				localBuffer.get().putLong(lastPos);
			}
		}
	}

	public long checkPhyPos(long timeStartMs, long maxOffset) {
		long minFirst = Long.MAX_VALUE;
		int firstSlotIndex = getSlotIndex(timeStartMs);

		for (int i = 0; i < slotsTotal * 2; i++) {
			int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
			localBuffer.get().position(slotIndex * Slot.SIZE);
			if ((timeStartMs + i * precisionMs) / precisionMs != localBuffer.get().getLong()) {
				continue;
			}

			long first = localBuffer.get().getLong();
			long last = localBuffer.get().getLong();
			if (last > maxOffset) {
				if (first < minFirst) {
					minFirst = first;
				}
			}
		}
		return minFirst;
	}

	public long getNum(long timeMs) {
		return getSlow(timeMs).num;
	}

	public long getAllNum(long timeStartMs) {
		int allNum = 0;
		int firstSlotIndex = getSlotIndex(timeStartMs);

		for (int i = 0; i < slotsTotal * 2; i++) {
			int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
			localBuffer.get().position(slotIndex * Slot.SIZE);
			if ((timeStartMs + i * precisionMs) / precisionMs == localBuffer.get().getLong()) {
				localBuffer.get().getLong();
				localBuffer.get().getLong();
				allNum += localBuffer.get().getInt();
			}
		}
		return allNum;
	}
}
