package com.mawen.learn.rocketmq.filter.util;

import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
@Getter
public class BitsArray implements Cloneable {

	private byte[] bytes;
	private int bitLength;

	public static BitsArray create(byte[] bytes, int bitLength) {
		return new BitsArray(bytes, bitLength);
	}

	private BitsArray(byte[] bytes) {
		if (bytes == null || bytes.length <= 1) {
			throw new IllegalArgumentException("Bytes is empty!");
		}

		this.bytes = new byte[bytes.length];
		System.arraycopy(bytes, 0, this.bytes, 0, this.bytes.length);
	}

	private BitsArray(int bitLength) {
		this.bitLength = bitLength;

		int temp = bitLength / Byte.SIZE;
		if (bitLength % Byte.SIZE > 0) {
			temp++;
		}

		this.bytes = new byte[temp];
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = 0x00;
		}
	}

	private BitsArray(byte[] bytes, int bitLength) {
		if (bytes == null || bytes.length <= 1) {
			throw new IllegalArgumentException("Bytes is empty!");
		}
		else if (bitLength < 1) {
			throw new IllegalArgumentException("Bit is less than 1.");
		}
		else if (bitLength < bytes.length * Byte.SIZE) {
			throw new IllegalArgumentException("BitLength is less than bytes.length() * " + Byte.SIZE);
		}

		this.bytes = new byte[bytes.length];
		System.arraycopy(bytes, 0, this.bytes, 0, this.bytes.length);
		this.bitLength = bitLength;
	}

	public int byteLength() {
		return bytes.length;
	}

	public void xor(final BitsArray other) {
		checkInitialized(this);
		checkInitialized(other);

		int minByteLength = Math.min(this.byteLength(), other.byteLength());

		for (int i = 0; i < minByteLength; i++) {
			bytes[i] = (byte) (bytes[i] ^ other.getByte(i));
		}
	}

	public void xor(int bitPos, boolean set) {
		checkBitPosition(bitPos, this);

		boolean value = getBit(bitPos);
		if (value ^ set) {
			setBit(bitPos, true);
		}
		else {
			setBit(bitPos, false);
		}
	}

	public void or(final BitsArray other) {
		checkInitialized(this);
		checkInitialized(other);

		int minByteLength = Math.min(this.byteLength(), other.byteLength());

		for (int i = 0; i < minByteLength; i++) {
			bytes[i] = (byte) (bytes[i] | other.getByte(i));
		}
	}

	public void or(int bitPos, boolean set) {
		checkBitPosition(bitPos, this);

		if (set) {
			setBit(bitPos, true);
		}
	}

	public void and(final BitsArray other) {
		checkInitialized(this);
		checkInitialized(other);

		int minByteLength = Math.min(this.byteLength(), other.byteLength());

		for (int i = 0; i < minByteLength; i++) {
			bytes[i] = (byte) (bytes[i] & other.getByte(i));
		}
	}

	public void and(int bitPos, boolean set) {
		checkBitPosition(bitPos, this);

		if (!set) {
			setBit(bitPos, false);
		}
	}

	public void not(int bitPos) {
		checkBitPosition(bitPos, this);

		setBit(bitPos, !getBit(bitPos));
	}

	public void setBit(int bitPos, boolean set) {
		checkBitPosition(bitPos, this);

		int sub = subscript(bitPos);
		int pos = position(bitPos);
		if (set) {
			bytes[sub] = (byte) (bytes[sub] | pos);
		}
		else {
			bytes[sub] = (byte) (bytes[sub] & ~pos);
		}
	}

	public boolean getBit(int bitPos) {
		checkBitPosition(bitPos, this);

		return (bytes[subscript(bitPos)] & position(bitPos)) != 0;
	}

	public void setByte(int bytePos, byte set) {
		checkBytePosition(bytePos, this);

		bytes[bytePos] = set;
	}

	public byte getByte(int bytePos) {
		checkBytePosition(bytePos, this);

		return bytes[bytePos];
	}

	protected int subscript(int bitPos) {
		return bitPos / Byte.SIZE;
	}

	protected int position(int bitPos) {
		return 1 << bitPos % Byte.SIZE;
	}

	protected void checkBytePosition(int bytePos, BitsArray bitsArray) {
		checkInitialized(bitsArray);
		if (bytePos > bitsArray.getBitLength()) {
			throw new IllegalArgumentException("BytePos is greater than " + bytes.length);
		}
		else if (bytePos < 0) {
			throw new IllegalArgumentException("BytePos is less than 0");
		}
	}

	protected void checkBitPosition(int bitPos, BitsArray bitsArray) {
		checkInitialized(bitsArray);
		if (bitPos > bitsArray.getBitLength()) {
			throw new IllegalArgumentException("BitPos is greater than " + bitLength);
		}
		else if (bitPos < 0) {
			throw new IllegalArgumentException("BitPos is less than 0");
		}
	}

	protected void checkInitialized(BitsArray bitsArray) {
		if (bitsArray.getBytes() == null) {
			throw new RuntimeException("Not initialized!");
		}
	}

	public BitsArray clone() {
		byte[] clone = new byte[this.byteLength()];

		System.arraycopy(this.bytes, 0, clone, 0, this.byteLength());

		return create(clone, getBitLength());
	}

	@Override
	public String toString() {
		if (this.bytes == null) {
			return "null";
		}
		StringBuilder stringBuilder = new StringBuilder(this.bytes.length * Byte.SIZE);
		for (int i = this.bytes.length - 1; i >= 0; i--) {

			int j = Byte.SIZE - 1;
			if (i == this.bytes.length - 1 && this.bitLength % Byte.SIZE > 0) {
				// not full byte
				j = this.bitLength % Byte.SIZE;
			}

			for (; j >= 0; j--) {

				byte mask = (byte) (1 << j);
				if ((this.bytes[i] & mask) == mask) {
					stringBuilder.append("1");
				} else {
					stringBuilder.append("0");
				}
			}
			if (i % 8 == 0) {
				stringBuilder.append("\n");
			}
		}

		return stringBuilder.toString();
	}
}
