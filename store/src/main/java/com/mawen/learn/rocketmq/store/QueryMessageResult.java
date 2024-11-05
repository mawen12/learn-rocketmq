package com.mawen.learn.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
@Getter
@Setter
public class QueryMessageResult {

	private final List<SelectMappedBufferResult> messageMapedList = new ArrayList<>(100);

	private final List<ByteBuffer> messageBufferList = new ArrayList<>(100);

	private long indexLastUpdateTimestamp;
	private long indexLastUpdatePhyoffset;
	private int bufferTotalSize = 0;

	public void addMessage(final SelectMappedBufferResult mapedBuffer) {
		messageMapedList.add(mapedBuffer);
		messageBufferList.add(mapedBuffer.getByteBuffer());
		bufferTotalSize += mapedBuffer.getSize();
	}

	public void release() {
		for (SelectMappedBufferResult select : messageMapedList) {
			select.release();
		}
	}
}
