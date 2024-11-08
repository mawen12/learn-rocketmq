package com.mawen.learn.rocketmq.store.ha;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/8
 */
public enum HAConnectionState {

	READY,

	HANDSHAKE,

	TRANSFER,

	SUSPEND,

	SHUTDOWN;
}
