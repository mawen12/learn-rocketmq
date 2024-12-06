package com.mawen.learn.rocketmq.acl.common;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import com.mawen.learn.rocketmq.common.MixAll;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/12/2
 */
@Getter
@Setter
@EqualsAndHashCode
@ToString
public class SessionCredentials {

	public static final Charset CHARSET = StandardCharsets.UTF_8;
	public static final String ACCESS_KEY = "AccessKey";
	public static final String SECRET_KEY = "SecretKey";
	public static final String SIGNATURE = "Signature";
	public static final String SECURITY_TOKEN = "SecurityToken";

	public static final String KEY_FILE = System.getProperty("rocketmq.client.keyFile", System.getProperty("user.home") + File.separator + "key");

	private String accessKey;
	private String secretKey;
	private String securityToken;
	private String signature;

	public SessionCredentials() {
		String keyContent = null;
		try {
			keyContent = MixAll.file2String(KEY_FILE);
		}
		catch (IOException ignored) {

		}

		if (keyContent != null) {
			Properties prop = MixAll.string2Properties(keyContent);
			if (prop != null) {
				updateContent(prop);
			}
		}
	}

	public SessionCredentials(String accessKey, String secretKey) {
		this.accessKey = accessKey;
		this.secretKey = secretKey;
	}

	public SessionCredentials(String accessKey, String secretKey, String securityToken) {
		this(accessKey, secretKey);
		this.securityToken = securityToken;
	}

	public void updateContent(Properties properties) {
		{
			String value = properties.getProperty(ACCESS_KEY);
			if (value != null) {
				this.accessKey = value.trim();
			}
		}
		{
			String value = properties.getProperty(SECRET_KEY);
			if (value != null) {
				this.secretKey = value.trim();
			}
		}
		{
			String value = properties.getProperty(SECURITY_TOKEN);
			if (value == null) {
				this.securityToken = value.trim();
			}
		}
	}
}
