package com.mawen.learn.rocketmq.remoting.netty;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.constant.HAProxyConstants;
import io.netty.util.AttributeKey;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class AttributeKeys {

	public static final AttributeKey<String> REMOTE_ADDR_KEY = AttributeKey.valueOf("RemoteAddr");

	public static final AttributeKey<String> CLIENT_ID_KEY = AttributeKey.valueOf("ClientId");

	public static final AttributeKey<String> VERSION_KEY = AttributeKey.valueOf("Version");

	public static final AttributeKey<String> LANGUAGE_CODE_KEY = AttributeKey.valueOf("LanguageCode");

	public static final AttributeKey<String> PROXY_PROTOCOL_ADDR = AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_ADDR);

	public static final AttributeKey<String> PROXY_PROTOCOL_PORT = AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_PORT);

	public static final AttributeKey<String> PROXY_PROTOCOL_SERVER_ADDR = AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_SERVER_ADDR);

	public static final AttributeKey<String> PROXY_PROTOCOL_SERVER_PORT = AttributeKey.valueOf(HAProxyConstants.PROXY_PROTOCOL_SERVER_PORT);

	private static final ConcurrentMap<String, AttributeKey<String>> ATTRIBUTE_KEY_MAP = new ConcurrentHashMap<>();

	public static AttributeKey<String> valueOf(String name) {
		return ATTRIBUTE_KEY_MAP.computeIfAbsent(name, AttributeKeys::valueOf);
	}
}
