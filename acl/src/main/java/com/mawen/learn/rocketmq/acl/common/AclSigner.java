package com.mawen.learn.rocketmq.acl.common;

import java.awt.event.KeyEvent;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import org.apache.commons.codec.binary.Base64;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.checkerframework.checker.units.qual.K;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/2
 */
public class AclSigner {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_AUTHORIZE_LOGGER_NAME);
	private static final int CAL_SIGNATURE_FAILED = 10015;
	private static final String CAL_SIGNATURE_FAILED_MSG = "[%s:signature-failed] unable to calculate a request signature. error=%s";

	public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	public static final SigningAlgorithm DEFAULT_ALGORITHM = SigningAlgorithm.HmacSHA1;

	public static String calSignature(String data, String key) {
		return calSignature(data, key, DEFAULT_ALGORITHM, DEFAULT_CHARSET);
	}

	public static String calSignature(String data, String key, SigningAlgorithm algorithm, Charset charset) {
		return signAndBase64Encode(data, key, algorithm, charset);
	}

	private static String signAndBase64Encode(String data, String key, SigningAlgorithm algorithm, Charset charset) {
		try {
			byte[] signature = sign(data.getBytes(charset), key.getBytes(charset), algorithm);
			return new String(Base64.encodeBase64(signature), DEFAULT_CHARSET);
		}
		catch (Exception e) {
			String message = String.format(CAL_SIGNATURE_FAILED_MSG, CAL_SIGNATURE_FAILED, e.getMessage());
			log.error(message, e);
			throw new AclException("CAL_SIGNATURE_FAILED", CAL_SIGNATURE_FAILED, message, e);
		}
	}

	private static byte[] sign(byte[] data, byte[] key, SigningAlgorithm algorithm) {
		try {
			Mac mac = Mac.getInstance(algorithm.toString());
			mac.init(new SecretKeySpec(key, algorithm.toString()));
			return mac.doFinal(data);
		}
		catch (Exception e) {
			String message = String.format(CAL_SIGNATURE_FAILED_MSG, CAL_SIGNATURE_FAILED, e.getMessage());
			log.error(message, e);
			throw new AclException("CAL_SIGNATURE_FAILD", CAL_SIGNATURE_FAILED, message, e);
		}
	}

	public static String calSignature(byte[] data, String key) {
		return calSignature(data, key, DEFAULT_ALGORITHM, DEFAULT_CHARSET);
	}

	public static String calSignature(byte[] data, String key, SigningAlgorithm algorithm, Charset charset) {
		return signAndBase64Encode(data, key, algorithm, charset);
	}

	private static String signAndBase64Encode(byte[] data, String key, SigningAlgorithm algorithm, Charset charset) {
		try {
			byte[] signature = sign(data, key.getBytes(charset), algorithm);
			return new String(Base64.encodeBase64(signature), DEFAULT_CHARSET);
		}
		catch (Exception e) {
			String message = String.format(CAL_SIGNATURE_FAILED_MSG, CAL_SIGNATURE_FAILED, e.getMessage());
			log.error(message, e);
			throw new AclException("CAL_SIGNATURE_FAILD", CAL_SIGNATURE_FAILED, message, e);
		}
	}
}
