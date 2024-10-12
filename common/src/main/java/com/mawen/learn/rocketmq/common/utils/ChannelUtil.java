package com.mawen.learn.rocketmq.common.utils;


import java.net.InetAddress;
import java.net.InetSocketAddress;

import io.netty.channel.Channel;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class ChannelUtil {

	public static String getRemoteIp(Channel channel) {
		InetSocketAddress inetSocketAddress = (InetSocketAddress) channel.remoteAddress();
		if (inetSocketAddress == null) {
			return "";
		}

		InetAddress inetAddr = inetSocketAddress.getAddress();
		return inetAddr != null ? inetAddr.getHostAddress() : inetSocketAddress.getHostName();
	}
}
