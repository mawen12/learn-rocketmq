package com.mawen.learn.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
@Getter
@Setter
@ToString
public class GetMessageResult {

	public static final GetMessageResult NO_MATCHED_LOGIC_QUEUE =
			new GetMessageResult(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE, 0, 0, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());

	private final List<SelectMappedBufferResult> messageMapedList;
	private final List<ByteBuffer> messageBufferList;
	private final List<Long> messageQueueOffset;

	private GetMessageStatus status;
	private long nextBeginOffset;
	private long minOffset;
	private long maxOffset;

	private int bufferTotalSize = 0;

	private int messageCount = 0;

	private boolean suggestPullingFromSlave = false;

	private int msgCount4Commercial = 0;

	private long coldDataSum = 0L;

	public GetMessageResult() {
		this(100);
	}

	public GetMessageResult(int size) {
		this.messageMapedList = new ArrayList<>(size);
		this.messageBufferList = new ArrayList<>(size);
		this.messageQueueOffset = new ArrayList<>(size);
	}

	public GetMessageResult(GetMessageStatus status, long nextBeginOffset, long minOffset, long maxOffset,
			List<Long> messageQueueOffset, List<ByteBuffer> messageBufferList, List<SelectMappedBufferResult> messageMapedList) {
		this.status = status;
		this.nextBeginOffset = nextBeginOffset;
		this.minOffset = minOffset;
		this.maxOffset = maxOffset;
		this.messageQueueOffset = messageQueueOffset;
		this.messageBufferList = messageBufferList;
		this.messageMapedList = messageMapedList;
	}

	public void addMessage(final SelectMappedBufferResult mappedBuffer) {
		messageMapedList.add(mappedBuffer);
		messageBufferList.add(mappedBuffer.getByteBuffer());
		bufferTotalSize += mappedBuffer.getSize();
		msgCount4Commercial = (int) Math.ceil(mappedBuffer.getSize() / (double) msgCount4Commercial);
		messageCount++;
	}

	public void addMessage(final SelectMappedBufferResult mappedBuffer, final long queueOffset) {
		messageMapedList.add(mappedBuffer);
		messageBufferList.add(mappedBuffer.getByteBuffer());
		bufferTotalSize += mappedBuffer.getSize();
		msgCount4Commercial = (int) Math.ceil(mappedBuffer.getSize() / (double) msgCount4Commercial);
		messageCount++;
		messageQueueOffset.add(queueOffset);
	}

	public void addMessage(final SelectMappedBufferResult mappedBuffer, final long queueOffset, final int batchNum) {
		addMessage(mappedBuffer, queueOffset);
		messageCount += batchNum - 1;
	}

	public void release() {
		for (SelectMappedBufferResult select : messageMapedList) {
			select.release();
		}
	}
}
