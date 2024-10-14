package com.mawen.learn.rocketmq.remoting.netty;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class NettyClientConfig {

	private int clientWorkThreads = NettySystemConfig.clientWorkerSize;

	private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();

	private int clientOnewaySemaphoreValue = NettySystemConfig.CLIENT_ONEWAY_SEMAPHORE_VALUE;

	private int clientAsyncSemaphoreValue = NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE;

	private int connectTimeoutMillis = NettySystemConfig.connectTimeoutMillis;

	private long channelNotActiveInterval = 60 * 1000;

	private int clientChannelMaxIdleTimeSeconds = NettySystemConfig.clientChannelMaxIdleTimeSeconds;

	private int clientSocketSndBufSize = NettySystemConfig.socketSndbufSize;

	private int clientSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

	private boolean clientPooledByteBufAllocatorEnable = false;

	private boolean clientCloseSocketIfTimeout = NettySystemConfig.clientCloseSocketIfTimeout;

	private boolean useTLS = Boolean.parseBoolean(System.getProperty(TLS_ENABLE))
}
