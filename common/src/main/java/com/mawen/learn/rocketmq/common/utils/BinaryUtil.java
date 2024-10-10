package com.mawen.learn.rocketmq.common.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class BinaryUtil {

	public static String generateMd5(String bodyStr) {
		byte[] bytes = calculateMd5(bodyStr.getBytes(StandardCharsets.UTF_8));
		return Hex.encodeHexString(bytes, false);
	}

	public static String generateMd5(byte[] content) {
		byte[] bytes = calculateMd5(content);
		return Hex.encodeHexString(bytes, false);
	}

	public static byte[] calculateMd5(byte[] binaryData) {
		MessageDigest messageDigest = null;

		try {
			messageDigest = MessageDigest.getInstance("MD5");
		}
		catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("MD5 algorithm not found.");
		}

		messageDigest.update(binaryData);

		return messageDigest.digest();
	}

	public static boolean isAscii(byte[] subject) {
		if (subject == null) {
			return false;
		}

		for (byte b : subject) {
			if (b < 32 || b > 126) {
				return false;
			}
		}
		return true;
	}
}
