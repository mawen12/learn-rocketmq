package com.mawen.learn.rocketmq.filter.util;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
@Getter
@ToString
@EqualsAndHashCode
public class BloomFilter {

	public static final Charset UTF_8 = StandardCharsets.UTF_8;

	private int f = 10;
	private int n = 128;

	private int k;
	private int m;

	public void hashTo(int[] bitPositions, BitsArray bits) {
		check(bits);

		for (int i : bitPositions) {
			bits.setBit(i, true);
		}
	}



	public BloomFilterData generate(String str) {
		int[] bitPositions = calcBitPositions(str);

		return new BloomFilterData(bitPositions, m);
	}

	public void hashTo(String str, BitsArray bits) {
		hashTo(calcBitPositions(str), bits);
	}

	public void hashTo(BloomFilterData data, BitsArray bits) {
		if (!isValid(data)) {
			throw new IllegalArgumentException("Bloom filter data may not belong to this filter!" + data + ", " + this);
		}
		hashTo(data.getBitPos(), bits);
	}

	public boolean isHit(String str, BitsArray bits) {
		return isHit(calcBitPositions(str), bits);
	}

	public boolean isHit(int[] bitPositions, BitsArray bits) {
		check(bits);

		boolean ret = bits.getBit(bitPositions[0]);
		for (int i = 0; i < bitPositions.length; i++) {
			ret &= bits.getBit(bitPositions[i]);
		}
		return ret;
	}

	public boolean isHit(BloomFilterData data, BitsArray bits) {
		if (!isValid(data)) {
			throw new IllegalArgumentException("Bloom filter data may not belong to this filter !" + data + ", " + this);
		}

		return isHit(data.getBitPos(), bits);
	}

	public boolean checkFalseHit(int[] bitPositions, BitsArray bits) {
		for (int i = 0; i < bitPositions.length; i++) {
			int pos = bitPositions[i];
			if (!bits.getBit(pos)) {
				return false;
			}
		}
		return true;
	}

	public boolean isValid(BloomFilterData data) {
		if (data == null
				|| data.getBitNum() != m
				|| data.getBitPos() == null
				|| data.getBitPos().length != k) {
			return false;
		}

		return true;
	}

	protected void check(BitsArray bits) {
		if (bits.getBitLength() != m) {
			throw new IllegalArgumentException("Length(" + bits.getBitLength() + ") of bits in BitsArray is not equal to " + m + "!");
		}
	}

	protected double logMN(double m, double n) {
		return Math.log(n) / Math.log(m);
	}
}
