package com.mawen.learn.rocketmq.common.compression;

import com.mawen.learn.rocketmq.common.sysflag.MessageSysFlag;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public enum CompressionType {
	LZ4(1),
	ZSTD(2),
	ZLIB(3);

	private final int value;

	CompressionType(int value) {
		this.value = value;
	}

	public static CompressionType of(String name) {
		switch (name.trim().toUpperCase()) {
			case "LZ4":
				return LZ4;
			case "ZSTD":
				return ZSTD;
			case "ZLIB":
				return ZLIB;
			default:
				throw new RuntimeException("Unsupported compress type name: " + name);
		}
	}

	public static CompressionType findByValue(int value) {
		switch (value) {
			case 1:
				return LZ4;
			case 2:
				return ZSTD;
			case 0:
			case 3:
				return ZLIB;
			default:
				throw new RuntimeException("Unknown compress type value: " + value);
		}
	}

	public int getValue() {
		return value;
	}

	public int getCompressionFlag() {
		switch (value) {
			case 1:
				return MessageSysFlag.COMPRESSION_LZ4_TYPE;
			case 2:
				return MessageSysFlag.COMPRESSION_ZSTD_TYPE;
			case 3:
				return MessageSysFlag.COMPRESSION_ZLIB_TYPE;
			default:
				throw new RuntimeException("Unsupported compress type flag: " + value);
		}
	}
}
