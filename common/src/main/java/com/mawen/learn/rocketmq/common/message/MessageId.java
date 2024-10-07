package com.mawen.learn.rocketmq.common.message;

import java.net.SocketAddress;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/1
 */
public class MessageId {

	private SocketAddress address;
	private long offset;

	public MessageId(SocketAddress address, long offset) {
		this.address = address;
		this.offset = offset;
	}

	public SocketAddress getAddress() {
		return address;
	}

	public void setAddress(SocketAddress address) {
		this.address = address;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}
}
