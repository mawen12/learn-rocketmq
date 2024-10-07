package com.mawen.learn.rocketmq.common.message;

import java.nio.ByteBuffer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/6
 */
public class MessageExtBatch extends MessageExtBrokerInner {

	private static final long serialVersionUID = 1800205771744290011L;

	private boolean isInnerBatch = false;

	private ByteBuffer encodedBuff;

	public ByteBuffer wrap() {
		assert getBody() != null;
		return ByteBuffer.wrap(getBody(), 0, getBody().length);
	}

	public boolean isInnerBatch() {
		return isInnerBatch;
	}

	public void setInnerBatch(boolean innerBatch) {
		isInnerBatch = innerBatch;
	}

	public ByteBuffer getEncodedBuff() {
		return encodedBuff;
	}

	public void setEncodedBuff(ByteBuffer encodedBuff) {
		this.encodedBuff = encodedBuff;
	}
}
