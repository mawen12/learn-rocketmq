package com.mawen.learn.rocketmq.remoting;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingConnectException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class RemotingHelper {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

	public static final String DEFAULT_CHARSET = "UTF-8";
	public static final String DEFAULT_CIDR_ALL = "0.0.0.0/0";

	public static final Map<Integer, String> REQUEST_CODE_MAP = new HashMap<Integer, String>() {{
		try {
			Field[] fields = RequestCode.class.getFields();
			for (Field field : fields) {
				if (field.getType() == int.class) {
					put((int) field.get(null), field.getName().toLowerCase());
				}
			}
		}
		catch (IllegalAccessException ignored) {

		}
	}};

	public static final Map<Integer, String> RESPONSE_CODE_MAP = new HashMap<Integer, String>() {{
		try {
			Field[] fields = ResponseCode.class.getFields();
			for (Field field : fields) {
				if (field.getType() == int.class) {
					put((int) field.get(null), field.getName().toLowerCase());
				}
			}
		}
		catch (IllegalAccessException ignored) {

		}
	}};

	public static <T> T getAttributeValue(AttributeKey<T> key, final Channel channel) {
		if (channel.hasAttr(key)) {
			Attribute<T> attr = channel.attr(key);
			return attr.get();
		}
		return null;
	}

	public static <T> void setPropertyToAttr(final Channel channel, AttributeKey<T> attributeKey, T value) {
		if (channel == null) {
			return;
		}

		channel.attr(attributeKey).set(value);
	}

	public static SocketAddress string2SocketAddress(final String addr) {
		int split = addr.lastIndexOf(":");
		String host = addr.substring(0, split);
		String port = addr.substring(split + 1);
		return new InetSocketAddress(host, Integer.parseInt(port));
	}

	public static RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis) throws RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException, InterruptedException, RemotingCommandException {
		long beginTime = System.currentTimeMillis();

		SocketAddress socketAddress = NetworkUtil.string2SocketAddress(addr);
		SocketChannel socketChannel = connection(socketAddress);
		if (socketChannel != null) {
			boolean sendRequestOK = false;

			try {
				socketChannel.configureBlocking(true);

				socketChannel.socket().setSoTimeout((int) timeoutMillis);

				ByteBuffer byteBufferRequest = request.encode();
				while (byteBufferRequest.hasRemaining()) {
					int length = socketChannel.write(byteBufferRequest);
					if (length > 0) {
						if (byteBufferRequest.hasRemaining()) {
							if (System.currentTimeMillis() - beginTime > timeoutMillis) {
								throw new RemotingSendRequestException(addr);
							}
						}
					}
					else {
						throw new RemotingSendRequestException(addr);
					}

					Thread.sleep(1);
				}

				sendRequestOK = true;

				ByteBuffer buffer = ByteBuffer.allocate(4);
				while (buffer.hasRemaining()) {
					int length = socketChannel.read(buffer);
					if (length > 0) {
						if (buffer.hasRemaining()) {
							if (System.currentTimeMillis() - beginTime > timeoutMillis) {
								throw new RemotingTimeoutException(addr, timeoutMillis);
							}
						}
					}
					else {
						throw new RemotingTimeoutException(addr, timeoutMillis);
					}

					Thread.sleep(1);
				}

				int size = buffer.getInt(0);
				ByteBuffer bodyBuffer = ByteBuffer.allocate(size);
				while (bodyBuffer.hasRemaining()) {
					int length = socketChannel.read(bodyBuffer);
					if (length > 0) {
						if (bodyBuffer.hasRemaining()) {
							if (System.currentTimeMillis() - beginTime > timeoutMillis) {
								throw new RemotingTimeoutException(addr, timeoutMillis);
							}
						}
					}
					else {
						throw new RemotingTimeoutException(addr, timeoutMillis);
					}

					Thread.sleep(1);
				}

				bodyBuffer.flip();
				return RemotingCommand.decode(bodyBuffer);
			}
			catch (IOException e) {
				log.error("invokeSync failure", e);

				if (sendRequestOK) {
					throw new RemotingTimeoutException(addr, timeoutMillis);
				}
				else {
					throw new RemotingSendRequestException(addr);
				}
			}
			finally {
				try {
					socketChannel.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		else {
			throw new RemotingConnectException(addr);
		}
	}

	public static String parseChannelRemoteAddr(final Channel channel) {
		if (channel == null) {
			return "";
		}

		String addr = getProxyProtocolAddress(channel);
		if (StringUtils.isNotBlank(addr)) {
			return addr;
		}

		Attribute<String> attr = channel.attr(AttributeKeys.REMOTE_ADDR_KEY);
		if (attr == null) {
			return parseChannelRemoteAddr0(channel);
		}

		addr = attr.get();
		if (addr == null) {
			addr = parseChannelRemoteAddr0(channel);
			attr.set(addr);
		}

		return addr;
	}

	private static String getProxyProtocolAddress(Channel channel) {
		if (!channel.hasAttr(AttributeKeys.PROXY_PROTOCOL_ADDR)) {
			return null;
		}

		String proxyProtocolAddr = getAttributeValue(AttributeKeys.PROXY_PROTOCOL_ADDR, channel);
		String proxyProtocolPort = getAttributeValue(AttributeKeys.PROXY_PROTOCOL_PORT, channel);
		if (StringUtils.isBlank(proxyProtocolAddr) || proxyProtocolPort == null) {
			return null;
		}

		return proxyProtocolAddr + ":" + proxyProtocolPort;
	}

	private static String parseChannelRemoteAddr0(final Channel channel) {
		return parseSocketAddr(channel.remoteAddress());
	}

	public static String parseChannelLocalAddr(final Channel channel) {
		return parseSocketAddr(channel.localAddress());
	}

	private static String parseSocketAddr(SocketAddress remote) {
		String addr = remote != null ? remote.toString() : "";

		if (addr.length() > 0) {
			int index = addr.lastIndexOf("/");
			if (index >= 0) {
				return addr.substring(index + 1);
			}
			return addr;
		}

		return "";
	}

	public static String parseHostFromAddress(String address) {
		if (address == null) {
			return "";
		}

		String[] split = address.split(":");
		if (split.length < 1) {
			return "";
		}
		return split[0];
	}

	public static String parseSocketAddressAddr(SocketAddress address) {

	}
}
