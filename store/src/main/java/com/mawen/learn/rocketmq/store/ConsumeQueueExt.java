package com.mawen.learn.rocketmq.store;

import java.nio.ByteBuffer;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
@Getter
public class ConsumeQueueExt {
	private static final Logger log = LoggerFactory.getLogger(ConsumeQueueExt.class);

	public static final int END_BLANK_DATA_LENGTH = 4;

	public static final long MAX_ADDR = Integer.MIN_VALUE - 1L;

	public static final long MAX_REAL_OFFSET = MAX_ADDR - Long.MIN_VALUE;

	private final MappedFileQueue mappedFileQueue;

	private final String topic;

	private final int queueId;

	private final String storePath;

	private final int mappedFileSize;

	private ByteBuffer tempContainer;

	@ToString
	@NoArgsConstructor
	@EqualsAndHashCode
	public static class CqUnit {

		/**
		 * 2 * 1: size, 32k max
		 * 8 * 2: msg time + tagCode
		 * 2: bitMapSize
		 */
		public static final short MIN_EXT_UNIT_SIZE = 2 * 1 + 8 * 2 + 2;

		public static final int MAX_EXT_UNIT_SIZE = Short.MAX_VALUE;

		@Setter @Getter
		private short size;
		@Setter @Getter
		private long tagsCode;
		@Setter @Getter
		private long msgStoreTime;
		private short bitMapSize;
		private byte[] filterBitMap;

		public CqUnit(Long tagsCode, long msgStoreTime, byte[] filterBitMap) {
			this.tagsCode = tagsCode == null ? 0 : tagsCode;
			this.msgStoreTime = msgStoreTime;
			this.filterBitMap = filterBitMap;
			this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
			this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);
		}

		public byte[] getFilterBitMap() {
			return bitMapSize < 1 ? null : filterBitMap;
		}

		public void setFilterBitMap(final byte[] filterBitMap) {
			this.filterBitMap = filterBitMap;
			this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
		}

		private int calcUnitSize() {
			return MIN_EXT_UNIT_SIZE + (filterBitMap == null ? 0 : filterBitMap.length);
		}

		private byte[] write(final ByteBuffer buffer) {
			bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
			size = (short) (MIN_EXT_UNIT_SIZE + bitMapSize);

			ByteBuffer tmp = buffer;

			if (tmp == null || tmp.capacity() < size) {
				tmp = ByteBuffer.allocate(size);
			}

			tmp.flip();
			tmp.limit(size);

			tmp.putShort(size);
			tmp.putLong(tagsCode);
			tmp.putLong(msgStoreTime);
			tmp.putShort(bitMapSize);
			if (bitMapSize > 0) {
				tmp.put(filterBitMap);
			}

			return tmp.array();
		}

		private boolean read(final ByteBuffer buffer) {
			if (buffer.position() + 2 > buffer.limit()) {
				return false;
			}

			size = buffer.getShort();

			if (size < 1) {
				return false;
			}

			tagsCode = buffer.getLong();
			msgStoreTime = buffer.getLong();
			bitMapSize = buffer.getShort();

			if (bitMapSize < 1) {
				return true;
			}

			if (filterBitMap == null || filterBitMap.length != bitMapSize) {
				filterBitMap = new byte[bitMapSize];
			}

			buffer.get(filterBitMap);
			return true;
		}

		private void readBySkip(final ByteBuffer buffer) {
			ByteBuffer tmp = buffer.slice();

			short tmpSize = tmp.getShort();
			size = tmpSize;

			if (tmpSize > 0) {
				buffer.position(buffer.position() + size);
			}
		}
	}
}
