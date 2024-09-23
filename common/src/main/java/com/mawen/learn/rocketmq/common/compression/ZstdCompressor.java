package com.mawen.learn.rocketmq.common.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class ZstdCompressor implements Compressor{

	private static final Logger log = LoggerFactory.getLogger(ZstdCompressor.class);

	@Override
	public byte[] compress(byte[] src, int level) throws IOException {
		byte[] result = src;
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
		ZstdOutputStream outputStream = new ZstdOutputStream(byteArrayOutputStream, level);

		try {
			outputStream.write(src);
			outputStream.flush();
			outputStream.close();
			result = byteArrayOutputStream.toByteArray();
		}
		catch (IOException e) {
			log.error("Failed to compress data by zstd", e);
			throw e;
		}
		finally {
			try {
				byteArrayOutputStream.close();
			}
			catch (IOException ignored) {

			}
		}

		return result;
	}

	@Override
	public byte[] decompress(byte[] src) throws IOException {
		byte[] result = src;
		byte[] uncompressData = new byte[src.length];
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
		ZstdInputStream zstdInputStream = new ZstdInputStream(byteArrayInputStream);
		ByteArrayOutputStream resultArrayOutputStream = new ByteArrayOutputStream(src.length);

		try {
			while (true) {
				int len = zstdInputStream.read(uncompressData, 0, uncompressData.length);
				if (len <= 0) {
					break;
				}

				resultArrayOutputStream.write(uncompressData, 0, len);
			}

			resultArrayOutputStream.flush();
			resultArrayOutputStream.close();
			result = resultArrayOutputStream.toByteArray();
		}
		catch (IOException e) {
			throw e;
		}
		finally {
			try {
				zstdInputStream.close();
				byteArrayInputStream.close();
			}
			catch (IOException e) {
				log.warn("Failed to close the zstd compress stream", e);
			}
		}

		return result;
	}
}
