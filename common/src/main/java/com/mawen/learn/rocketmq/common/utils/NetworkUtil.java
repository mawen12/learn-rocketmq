package com.mawen.learn.rocketmq.common.utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class NetworkUtil {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

	public static final String OS_NAME = System.getProperty("os.name");

	private static boolean isLinuxPlatform = false;

	private static boolean isWindowsPlatform = false;

	static {
		if (OS_NAME != null && OS_NAME.toLowerCase().contains("linux")) {
			isLinuxPlatform = true;
		}

		if (OS_NAME != null && OS_NAME.toLowerCase().contains("windows")) {
			isWindowsPlatform = true;
		}
	}

	public static boolean isWindowsPlatform() {
		return isWindowsPlatform;
	}

	public static boolean isLinuxPlatform() {
		return isLinuxPlatform;
	}

	public static Selector openSelector() throws IOException {
		Selector result = null;

		if (isLinuxPlatform()) {
			try {
				final Class<?> providerClazz = Class.forName("sun.nio.ch.EPollSelectorProvider");
				if (providerClazz != null) {
					try {
						Method method = providerClazz.getMethod("provider");
						if (method != null) {
							SelectorProvider selectorProvider = (SelectorProvider) method.invoke(null);
							if (selectorProvider != null) {
								result = selectorProvider.openSelector();
							}
						}
					}
					catch (Exception e) {
						log.warn("Open ePoll Selector for linux platform exception", e);
					}
				}
			}
			catch (Exception ignored) {

			}
		}

		if (result == null) {
			result = Selector.open();
		}

		return result;
	}

	public static String getLocalAddress() {
		try {
			Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
			List<String> ipv4Result = new ArrayList<>();
			List<String> ipv6Result = new ArrayList<>();

			while (enumeration.hasMoreElements()) {
				NetworkInterface nif = enumeration.nextElement();
				if (isBridge(nif) || nif.isVirtual() || nif.isPointToPoint() || !nif.isUp()) {
					continue;
				}

				Enumeration<InetAddress> en = nif.getInetAddresses();
				while (en.hasMoreElements()) {
					InetAddress address = en.nextElement();
					if (!address.isLoopbackAddress()) {
						if (address instanceof Inet6Address) {
							ipv6Result.add(normalizeHostAddress(address));
						}
						else {
							ipv4Result.add(normalizeHostAddress(address));
						}
					}
				}
			}

			if (!ipv4Result.isEmpty()) {
				for (String ip : ipv4Result) {
					if (ip.startsWith("127.0") || ip.startsWith("192.168") || ip.startsWith("0.")) {
						continue;
					}
					return ip;
				}
				return ipv4Result.get(ipv4Result.size() - 1);
			}
			else if (!ipv6Result.isEmpty()) {
				return ipv6Result.get(0);
			}

			InetAddress localHost = InetAddress.getLocalHost();
			return normalizeHostAddress(localHost);
		}
		catch (Exception e) {
			log.warn("Failed to obtain local address", e);
		}

		return null;
	}

	public static String normalizeHostAddress(InetAddress localHost) {
		if (localHost instanceof Inet6Address) {
			return "[" + localHost.getHostAddress() + "]";
		}
		return localHost.getHostAddress();
	}

	public static String convert2IpString(final String addr) {
		return socketAddress2String(string2SocketAddress(addr));
	}

	public static SocketAddress string2SocketAddress(String addr) {
		int split = addr.lastIndexOf(":");
		String host = addr.substring(0, split);
		String port = addr.substring(split + 1);
		return  new InetSocketAddress(host, Integer.parseInt(port));
	}

	public static String socketAddress2String(SocketAddress addr) {
		StringBuilder sb = new StringBuilder();
		InetSocketAddress inetSocketAddress = (InetSocketAddress) addr;

		sb.append(inetSocketAddress.getAddress().getHostAddress());
		sb.append(":");
		sb.append(inetSocketAddress.getPort());

		return sb.toString();
	}

	private static boolean isBridge(NetworkInterface networkInterface) {
		try {
			if (isLinuxPlatform()) {
				String interfaceName = networkInterface.getName();
				File file = new File("/sys/class/net/" + interfaceName + "/bridge");
				return file.exists();
			}
		}
		catch (SecurityException ignored) {

		}
		return false;
	}
}
