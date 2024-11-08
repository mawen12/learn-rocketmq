package com.mawen.learn.rocketmq.store.ha;

import java.nio.channels.SocketChannel;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/8
 */
public interface HAConnection {

	void start();

	void shutdown();

	void close();

	SocketChannel getSocketChannel();

	HAConnectionState getCurrentState();

	String getClientAddress();

	long getTransferredByteInSecond();

	long getTransferFromWhere();

	long getSlaveAckOffset();
}
