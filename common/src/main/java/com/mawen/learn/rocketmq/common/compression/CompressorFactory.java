package com.mawen.learn.rocketmq.common.compression;

import java.util.EnumMap;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class CompressorFactory {

	private static final EnumMap<CompressionType, Compressor> COMPRESSORS;

	static {
		COMPRESSORS = new EnumMap<>(CompressionType.class);
		COMPRESSORS.put(CompressionType.LZ4, new Lz4Compressor());
		COMPRESSORS.put(CompressionType.ZSTD, new ZstdCompressor());
		COMPRESSORS.put(CompressionType.ZLIB, new ZlibCompressor());
	}

	public static Compressor getCompressor(CompressionType type) {
		return COMPRESSORS.get(type);
	}
}
