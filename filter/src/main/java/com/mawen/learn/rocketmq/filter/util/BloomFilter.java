package com.mawen.learn.rocketmq.filter.util;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.google.common.hash.Hashing;
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

	private BloomFilter(int f, int n) {
		if (f < 1 || f >= 100) {
			throw new IllegalArgumentException("f must be greater or equal than 1 and less than 100");
		}
		if (n < 1) {
			throw new IllegalArgumentException("n must be greater than 0");
		}

		this.f = f;
		this.n = n;

		double errorRate = f / 100.0;
		this.k = (int) Math.ceil(logMN(0.5, errorRate));

		if (k < 1) {
			throw new IllegalArgumentException("Hash function num is less than 1, maybe you should change the value of error rate or bit sum!");
		}

		this.m = (int) Math.ceil(this.n * logMN(2, 1 / errorRate) * logMN(2, Math.E));
		this.m = (int) (Byte.SIZE * Math.ceil(this.m / (Byte.SIZE * 1.0)));
	}

	public static BloomFilter createByFn(int f, int n) {
		return new BloomFilter(f, n);
	}

	public void hashTo(int[] bitPositions, BitsArray bits) {
		check(bits);

		for (int i : bitPositions) {
			bits.setBit(i, true);
		}
	}

	public int[] calcBitPositions(String str) {
		int[] bitPositions = new int[this.k];

		long hash64 = Hashing.murmur3_128().hashString(str, UTF_8).asLong();

		int hash1 = (int) hash64;
		int hash2 = (int) (hash64 >>> 32);

		for (int i = 1; i < this.k; i++) {
			int combinedHash = hash1 + (i * hash2);
			if (combinedHash < 0) {
				combinedHash = ~combinedHash;
			}
			bitPositions[i - 1] = combinedHash % this.m;
		}

		return bitPositions;
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
