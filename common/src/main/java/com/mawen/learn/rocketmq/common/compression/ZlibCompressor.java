package com.mawen.learn.rocketmq.common.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import com.google.common.io.ByteSink;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class ZlibCompressor implements Compressor{

	private static final Logger log = LoggerFactory.getLogger(ZlibCompressor.class);

	@Override
	public byte[] compress(byte[] src, int level) throws IOException {
		byte[] result = src;
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
		Deflater deflater = new Deflater(level);
		DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream, deflater);

		try {
			deflaterOutputStream.write(src);
			deflaterOutputStream.flush();
			deflaterOutputStream.close();
			result = byteArrayOutputStream.toByteArray();
		}
		catch (IOException e) {
			deflater.end();
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
		InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);

		try {
			while (true) {
				int len = inflaterInputStream.read(uncompressData, 0, uncompressData.length);
				if (len <= 0) {
					break;
				}

				byteArrayOutputStream.write(uncompressData, 0, len);
			}

			byteArrayOutputStream.flush();
			result = byteArrayOutputStream.toByteArray();
		}
		catch (IOException e) {
			throw e;
		}
		finally {
			try {
				byteArrayInputStream.close();
			}
			catch (IOException e) {
				log.error("Failed to close the stream", e);
			}

			try {
				inflaterInputStream.close();
			}
			catch (IOException e) {
				log.error("Failed to close the stream", e);
			}

			try {
				byteArrayOutputStream.close();
			}
			catch (IOException e) {
				log.error("Failed to close the stream", e);
			}
		}

		return result;
	}
}
