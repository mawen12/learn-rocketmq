package com.mawen.learn.rocketmq.store.queue;

import java.nio.ByteBuffer;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
@Setter
@Getter
@ToString
public class CqUnit {

	private final long queueOffset;

	private final int size;

	private final long pos;

	private final short batchNum;

	private long tagsCode;

	private ConsumeQueueExt.CqExtUnit cqExtUnit;

	private final ByteBuffer nativeBuffer;

	private final int compactedOffset;

	public CqUnit(long queueOffset, long pos, int size, long tagsCode) {
		this(queueOffset, pos, size, tagsCode, (short) 1, 0, null);
	}

	public CqUnit(long queueOffset, long pos, int size, long tagsCode, short batchNum, int compactedOffset, ByteBuffer buffer) {
		this.queueOffset = queueOffset;
		this.size = size;
		this.pos = pos;
		this.tagsCode = tagsCode;
		this.batchNum = batchNum;

		this.nativeBuffer = buffer;
		this.compactedOffset = compactedOffset;
	}

	public long getValidTagsCodeAsLong() {
		return isTagsCodeValid ? tagsCode : null;
	}

	public boolean isTagsCodeValid() {
		return !ConsumeQueueExt.isExtAttr(tagsCode);
	}

	public void correctCompactOffset(int correctedOffset) {
		nativeBuffer.putInt(correctedOffset);
	}
}
