package com.mawen.learn.rocketmq.common.message;

import java.nio.ByteBuffer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/1
 */
public enum MessageVersion {
	MESSAGE_VERSION_V1(MessageDecoder.MESSAGE_MAGIC_CODE) {
		@Override
		public int getTopicLengthSize() {
			return 1;
		}

		@Override
		public int getTopicLength(ByteBuffer buffer) {
			return buffer.get();
		}

		@Override
		public int getTopicLength(ByteBuffer buffer, int index) {
			return buffer.get(index);
		}

		@Override
		public void putTopicLength(ByteBuffer buffer, int topicLength) {
			buffer.put((byte) topicLength);
		}
	},

	MESSAGE_VERSION_V2(MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
		@Override
		public int getTopicLengthSize() {
			return 2;
		}

		@Override
		public int getTopicLength(ByteBuffer buffer) {
			return buffer.getShort();
		}

		@Override
		public int getTopicLength(ByteBuffer buffer, int index) {
			return buffer.getShort(index);
		}

		@Override
		public void putTopicLength(ByteBuffer buffer, int topicLength) {
			buffer.putShort((short) topicLength);
		}
	};

	private final int magicCode;

	MessageVersion(int magicCode) {
		this.magicCode = magicCode;
	}

	public int getMagicCode() {
		return magicCode;
	}

	public abstract int getTopicLengthSize();

	public abstract int getTopicLength(ByteBuffer buffer);

	public abstract int getTopicLength(ByteBuffer buffer, int index);

	public abstract void putTopicLength(ByteBuffer buffer, int topicLength);


	public static MessageVersion valueOfMagicCode(int magicCode) {
		for (MessageVersion version : MessageVersion.values()) {
			if (version.getMagicCode() == magicCode) {
				return version;
			}
		}

		throw new IllegalArgumentException("Invalid magicCode " + magicCode);
	}
}
