package com.mawen.learn.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import com.mawen.learn.rocketmq.store.util.LibC;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
public class TransientStorePool {

	private static final Logger log = LoggerFactory.getLogger(TransientStorePool.class);

	private final int poolSize;
	private final int fileSize;
	private final Deque<ByteBuffer> availableBuffers;
	@Setter @Getter
	private volatile boolean isRealCommit = true;

	public TransientStorePool(int poolSize, int fileSize) {
		this.poolSize = poolSize;
		this.fileSize = fileSize;
		this.availableBuffers = new ConcurrentLinkedDeque<>();
	}

	public void init() {
		for (int i = 0; i < poolSize; i++) {
			ByteBuffer buffer = ByteBuffer.allocateDirect(fileSize);

			long address = ((DirectBuffer) buffer).address();
			Pointer pointer = new Pointer(address);
			LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

			availableBuffers.offer(buffer);
		}
	}

	public void destroy() {
		for (ByteBuffer buffer : availableBuffers) {
			long address = ((DirectBuffer) buffer).address();
			Pointer pointer = new Pointer(address);
			LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
		}
	}

	public void returnBuffer(ByteBuffer buffer) {
		buffer.position(0);
		buffer.limit(fileSize);
		availableBuffers.offerFirst(buffer);
	}

	public ByteBuffer borrowBuffer() {
		ByteBuffer buffer = availableBuffers.pollFirst();
		if (availableBuffers.size() < poolSize * 0.4) {
			log.warn("TransientStorePool only remain {} sheets", availableBuffers.size());
		}
		return buffer;
	}

	public int availableBufferNums() {
		return availableBuffers.size();
	}
}
