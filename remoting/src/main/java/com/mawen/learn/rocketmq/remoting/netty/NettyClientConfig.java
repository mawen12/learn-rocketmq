package com.mawen.learn.rocketmq.remoting.netty;

import com.mawen.learn.rocketmq.remoting.common.TlsMode;

import static com.mawen.learn.rocketmq.remoting.netty.TlsSystemConfig.*;

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

	private boolean useTLS = Boolean.parseBoolean(System.getProperty(TLS_ENABLE, String.valueOf(tlsMode == TlsMode.ENFORCING)));

	private String socksProxyConfig = "{}";

	private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;

	private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;

	private boolean disableCallbackExecutor = false;

	private boolean disableNettyWorkerGroup = false;

	private long maxReconnectionIntervalTimeSeconds = 60;

	private boolean enableReconnectForGoAway = true;

	private boolean enableTransparentRetry = true;

	public int getClientWorkThreads() {
		return clientWorkThreads;
	}

	public void setClientWorkThreads(int clientWorkThreads) {
		this.clientWorkThreads = clientWorkThreads;
	}

	public int getClientCallbackExecutorThreads() {
		return clientCallbackExecutorThreads;
	}

	public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
		this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
	}

	public int getClientOnewaySemaphoreValue() {
		return clientOnewaySemaphoreValue;
	}

	public void setClientOnewaySemaphoreValue(int clientOnewaySemaphoreValue) {
		this.clientOnewaySemaphoreValue = clientOnewaySemaphoreValue;
	}

	public int getClientAsyncSemaphoreValue() {
		return clientAsyncSemaphoreValue;
	}

	public void setClientAsyncSemaphoreValue(int clientAsyncSemaphoreValue) {
		this.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
	}

	public int getConnectTimeoutMillis() {
		return connectTimeoutMillis;
	}

	public void setConnectTimeoutMillis(int connectTimeoutMillis) {
		this.connectTimeoutMillis = connectTimeoutMillis;
	}

	public long getChannelNotActiveInterval() {
		return channelNotActiveInterval;
	}

	public void setChannelNotActiveInterval(long channelNotActiveInterval) {
		this.channelNotActiveInterval = channelNotActiveInterval;
	}

	public int getClientChannelMaxIdleTimeSeconds() {
		return clientChannelMaxIdleTimeSeconds;
	}

	public void setClientChannelMaxIdleTimeSeconds(int clientChannelMaxIdleTimeSeconds) {
		this.clientChannelMaxIdleTimeSeconds = clientChannelMaxIdleTimeSeconds;
	}

	public int getClientSocketSndBufSize() {
		return clientSocketSndBufSize;
	}

	public void setClientSocketSndBufSize(int clientSocketSndBufSize) {
		this.clientSocketSndBufSize = clientSocketSndBufSize;
	}

	public int getClientSocketRcvBufSize() {
		return clientSocketRcvBufSize;
	}

	public void setClientSocketRcvBufSize(int clientSocketRcvBufSize) {
		this.clientSocketRcvBufSize = clientSocketRcvBufSize;
	}

	public boolean isClientPooledByteBufAllocatorEnable() {
		return clientPooledByteBufAllocatorEnable;
	}

	public void setClientPooledByteBufAllocatorEnable(boolean clientPooledByteBufAllocatorEnable) {
		this.clientPooledByteBufAllocatorEnable = clientPooledByteBufAllocatorEnable;
	}

	public boolean isClientCloseSocketIfTimeout() {
		return clientCloseSocketIfTimeout;
	}

	public void setClientCloseSocketIfTimeout(boolean clientCloseSocketIfTimeout) {
		this.clientCloseSocketIfTimeout = clientCloseSocketIfTimeout;
	}

	public boolean isUseTLS() {
		return useTLS;
	}

	public void setUseTLS(boolean useTLS) {
		this.useTLS = useTLS;
	}

	public String getSocksProxyConfig() {
		return socksProxyConfig;
	}

	public void setSocksProxyConfig(String socksProxyConfig) {
		this.socksProxyConfig = socksProxyConfig;
	}

	public int getWriteBufferHighWaterMark() {
		return writeBufferHighWaterMark;
	}

	public void setWriteBufferHighWaterMark(int writeBufferHighWaterMark) {
		this.writeBufferHighWaterMark = writeBufferHighWaterMark;
	}

	public int getWriteBufferLowWaterMark() {
		return writeBufferLowWaterMark;
	}

	public void setWriteBufferLowWaterMark(int writeBufferLowWaterMark) {
		this.writeBufferLowWaterMark = writeBufferLowWaterMark;
	}

	public boolean isDisableCallbackExecutor() {
		return disableCallbackExecutor;
	}

	public void setDisableCallbackExecutor(boolean disableCallbackExecutor) {
		this.disableCallbackExecutor = disableCallbackExecutor;
	}

	public boolean isDisableNettyWorkerGroup() {
		return disableNettyWorkerGroup;
	}

	public void setDisableNettyWorkerGroup(boolean disableNettyWorkerGroup) {
		this.disableNettyWorkerGroup = disableNettyWorkerGroup;
	}

	public long getMaxReconnectionIntervalTimeSeconds() {
		return maxReconnectionIntervalTimeSeconds;
	}

	public void setMaxReconnectionIntervalTimeSeconds(long maxReconnectionIntervalTimeSeconds) {
		this.maxReconnectionIntervalTimeSeconds = maxReconnectionIntervalTimeSeconds;
	}

	public boolean isEnableReconnectForGoAway() {
		return enableReconnectForGoAway;
	}

	public void setEnableReconnectForGoAway(boolean enableReconnectForGoAway) {
		this.enableReconnectForGoAway = enableReconnectForGoAway;
	}

	public boolean isEnableTransparentRetry() {
		return enableTransparentRetry;
	}

	public void setEnableTransparentRetry(boolean enableTransparentRetry) {
		this.enableTransparentRetry = enableTransparentRetry;
	}
}
