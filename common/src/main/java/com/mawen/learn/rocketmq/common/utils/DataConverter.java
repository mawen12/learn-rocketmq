package com.mawen.learn.rocketmq.common.utils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class DataConverter {

	public static final Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

	public static byte[] long2Byte(Long v) {
		ByteBuffer tmp = ByteBuffer.allocate(8);
		tmp.putLong(v);
		return tmp.array();
	}

	public static int setBit(int value, int index, boolean flag) {
		if (flag) {
			return (int) (value | (1L << index));
		}
		else {
			return (int) (value & ~(1L << index));
		}
	}

	public static boolean getBit(int value, int index) {
		return (value & (1L << index)) != 0;
	}
}
