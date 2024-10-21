package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.List;

import com.mawen.learn.rocketmq.client.consumer.PullResult;
import com.mawen.learn.rocketmq.client.consumer.PullStatus;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/21
 */
@Getter
@Setter
public class PullResultExt extends PullResult {

	private final long suggestWhichBrokerId;
	private byte[] messageBinary;
	private final Long offsetDelta;

	public PullResultExt(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset, List<MessageExt> msgFoundList, final long suggestWhichBrokerId, final byte[] messageBinary) {
		this(pullStatus, nextBeginOffset, minOffset, maxOffset, msgFoundList, suggestWhichBrokerId, messageBinary, 0L);
	}

	public PullResultExt(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset, List<MessageExt> msgFoundList, long suggestWhichBrokerId, byte[] messageBinary, Long offsetDelta) {
		super(pullStatus, nextBeginOffset, minOffset, maxOffset, msgFoundList);
		this.suggestWhichBrokerId = suggestWhichBrokerId;
		this.messageBinary = messageBinary;
		this.offsetDelta = offsetDelta;
	}
}
