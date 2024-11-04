package com.mawen.learn.rocketmq.store;

import java.nio.ByteBuffer;

import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
@Setter
@Getter
public class SelectMappedBufferResult {

	private final long startOffset;

	private final ByteBuffer byteBuffer;

	private int size;

	protected MappedFile mappedFile;

	private boolean isInCache = true;

	public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
		this.startOffset = startOffset;
		this.byteBuffer = byteBuffer;
		this.size = size;
		this.mappedFile = mappedFile;
	}

	public void setSize(final int s) {
		this.size = s;
		this.byteBuffer.limit(this.size);
	}

	public synchronized void release() {
		if (mappedFile != null) {
			mappedFile.release();
			mappedFile = null;
		}
	}

	public synchronized boolean hasReleased() {
		return mappedFile == null;
	}

	public boolean isInMem() {
		if (mappedFile == null) {
			return true;
		}

		long pos = startOffset - mappedFile.getFileFromOffset();
		return mappedFile.isLoaded(pos, size);
	}
}
