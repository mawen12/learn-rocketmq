package com.mawen.learn.rocketmq.common.constant;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class HAProxyConstants {

	public static final String CHANNEL_ID = "channel_id";
	public static final String PROXY_PROTOCOL_PREFIX = "proxy_protocol_";
	public static final String PROXY_PROTOCOL_ADDR = PROXY_PROTOCOL_PREFIX + "addr";
	public static final String PROXY_PROTOCOL_PORT = PROXY_PROTOCOL_PREFIX + "port";
	public static final String PROXY_PROTOCOL_SERVER_ADDR = PROXY_PROTOCOL_PREFIX + "server_addr";
	public static final String PROXY_PROTOCOL_SERVER_PORT = PROXY_PROTOCOL_PREFIX + "server_port";
	public static final String PROXY_PROTOCOL_TLV_PREFIX = PROXY_PROTOCOL_PREFIX + "tlv_0x";
}
