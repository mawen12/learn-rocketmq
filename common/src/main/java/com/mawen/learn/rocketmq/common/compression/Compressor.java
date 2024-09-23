package com.mawen.learn.rocketmq.common.compression;

import java.io.IOException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public interface Compressor {

	byte[] compress(byte[] src, int level) throws IOException;

	byte[] decompress(byte[] src) throws IOException;
}
