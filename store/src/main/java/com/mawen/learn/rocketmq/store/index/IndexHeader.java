package com.mawen.learn.rocketmq.store.index;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lombok.RequiredArgsConstructor;

/**
 * Index File Header, size: 8 + 8 + 8 + 8 + 4 + 4 = 40
 * <pre>
 * ┌─────────────────┬───────────────┬───────────────────────┬─────────────────────┬─────────────────┬─────────────┐
 * │ Begin Timestamp │ End Timestamp │ Begin Physical Offset │ End Physical Offset │ Hash Slot Count │ Index Count │
 * ├─────────────────┼───────────────┼───────────────────────┼─────────────────────┼─────────────────┼─────────────┤
 * │ 8 bytes         │ 8 bytes       │ 8 bytes               │ 8 bytes             │ 4 bytes         │ 4 bytes     │
 * └─────────────────┴───────────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────┘
 * </pre>
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@RequiredArgsConstructor
public class IndexHeader {
	public static final int INDEX_HEADER_SIZE = 40;

	private static int beginTimestampIndex = 0;
	private static int endTimestampIndex = 8;
	private static int beginPhyOffsetIndex = 16;
	private static int endPhyOffsetIndex = 24;
	private static int hashSlotCountIndex = 32;
	private static int indexCountIndex = 36;

	private final ByteBuffer byteBuffer;
	private final AtomicLong beginTimestamp = new AtomicLong(0);
	private final AtomicLong endTimestamp = new AtomicLong(0);
	private final AtomicLong beginPhyOffset = new AtomicLong(0);
	private final AtomicLong endPhyOffset = new AtomicLong(0);
	private final AtomicInteger hashSlotCount = new AtomicInteger(0);
	private final AtomicInteger indexCount = new AtomicInteger(0);

	public void load() {
		beginTimestamp.set(byteBuffer.getLong(beginTimestampIndex));
		endTimestamp.set(byteBuffer.getLong(endTimestampIndex));
		beginPhyOffset.set(byteBuffer.getLong(beginPhyOffsetIndex));
		endPhyOffset.set(byteBuffer.getLong(endPhyOffsetIndex));
		hashSlotCount.set(byteBuffer.getInt(hashSlotCountIndex));
		indexCount.set(byteBuffer.getInt(indexCountIndex));

		if (indexCount.get() <= 0) {
			indexCount.set(1);
		}
	}

	public void updateByteBuffer() {
		byteBuffer.putLong(beginTimestampIndex, beginTimestamp.get());
		byteBuffer.putLong(endTimestampIndex, endTimestamp.get());
		byteBuffer.putLong(beginPhyOffsetIndex, beginPhyOffset.get());
		byteBuffer.putLong(endPhyOffsetIndex, endPhyOffset.get());
		byteBuffer.putInt(hashSlotCountIndex, hashSlotCount.get());
		byteBuffer.putInt(indexCountIndex, indexCount.get());
	}

	public long getBeginTimestamp() {
		return beginTimestamp.get();
	}

	public void setBeginTimestamp(long beginTimestamp) {
		this.beginTimestamp.set(beginTimestamp);
		byteBuffer.putLong(beginTimestampIndex, beginTimestamp);
	}

	public long getEndTimestamp() {
		return endTimestamp.get();
	}

	public void setEndTimestamp(long endTimestamp) {
		this.endTimestamp.set(endTimestamp);
		byteBuffer.putLong(endTimestampIndex, endTimestamp);
	}

	public long getBeginPhyOffset() {
		return beginPhyOffset.get();
	}

	public void setBeginPhyOffset(long beginPhyOffset) {
		this.beginPhyOffset.set(beginPhyOffset);
		byteBuffer.putLong(beginPhyOffsetIndex, beginPhyOffset);
	}

	public long getEndPhyOffset() {
		return endPhyOffset.get();
	}

	public void setEndPhyOffset(long endPhyOffset) {
		this.endPhyOffset.set(endPhyOffset);
		byteBuffer.putLong(endPhyOffsetIndex, endPhyOffset);
	}

	public int getHashSlotCount() {
		return hashSlotCount.get();
	}

	public void incHashSlotCount() {
		int value = hashSlotCount.incrementAndGet();
		byteBuffer.putInt(hashSlotCountIndex, value);
	}

	public int getIndexCount() {
		return indexCount.get();
	}

	public void incIndexCount() {
		int value = indexCount.incrementAndGet();
		byteBuffer.putInt(indexCountIndex, value);
	}
}
