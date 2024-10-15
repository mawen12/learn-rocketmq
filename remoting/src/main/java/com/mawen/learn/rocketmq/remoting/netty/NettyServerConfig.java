package com.mawen.learn.rocketmq.remoting.netty;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
public class NettyServerConfig implements Cloneable{

	private String bindAddress = "0.0.0.0";

	private int listenPort = 0;

	private int serverWorkerThreads = 8;

	private int serverCallbackExecutorThreads = 0;

	private int serverSelectorThreads = 3;

	private int serverOnewaySemaphoreValue = 256;

	private int serverAsyncSemaphoreValue = 64;

	private int serverChannelMaxIdleTimeSeconds = 120;

	private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;

	private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

	private int writeBufferHighWaterMark = NettySystemConfig.writeBufferHighWaterMark;

	private int writeBufferLowWaterMark = NettySystemConfig.writeBufferLowWaterMark;

	private int serverSocketBackLog = NettySystemConfig.socketBacklog;

	private boolean serverNettyWorkerGroupEnable = true;

	private boolean serverPooledByteBufAllocatorEnable = true;

	private boolean enableShutdownGracefully = false;

	private int shutdownWaitTimeSeconds = 30;

	private boolean useEpollNativeSelector = false;

	public String getBindAddress() {
		return bindAddress;
	}

	public void setBindAddress(String bindAddress) {
		this.bindAddress = bindAddress;
	}

	public int getListenPort() {
		return listenPort;
	}

	public void setListenPort(int listenPort) {
		this.listenPort = listenPort;
	}

	public int getServerWorkerThreads() {
		return serverWorkerThreads;
	}

	public void setServerWorkerThreads(int serverWorkerThreads) {
		this.serverWorkerThreads = serverWorkerThreads;
	}

	public int getServerCallbackExecutorThreads() {
		return serverCallbackExecutorThreads;
	}

	public void setServerCallbackExecutorThreads(int serverCallbackExecutorThreads) {
		this.serverCallbackExecutorThreads = serverCallbackExecutorThreads;
	}

	public int getServerSelectorThreads() {
		return serverSelectorThreads;
	}

	public void setServerSelectorThreads(int serverSelectorThreads) {
		this.serverSelectorThreads = serverSelectorThreads;
	}

	public int getServerOnewaySemaphoreValue() {
		return serverOnewaySemaphoreValue;
	}

	public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
		this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
	}

	public int getServerAsyncSemaphoreValue() {
		return serverAsyncSemaphoreValue;
	}

	public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
		this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
	}

	public int getServerChannelMaxIdleTimeSeconds() {
		return serverChannelMaxIdleTimeSeconds;
	}

	public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
		this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
	}

	public int getServerSocketSndBufSize() {
		return serverSocketSndBufSize;
	}

	public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
		this.serverSocketSndBufSize = serverSocketSndBufSize;
	}

	public int getServerSocketRcvBufSize() {
		return serverSocketRcvBufSize;
	}

	public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
		this.serverSocketRcvBufSize = serverSocketRcvBufSize;
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

	public int getServerSocketBackLog() {
		return serverSocketBackLog;
	}

	public void setServerSocketBackLog(int serverSocketBackLog) {
		this.serverSocketBackLog = serverSocketBackLog;
	}

	public boolean isServerNettyWorkerGroupEnable() {
		return serverNettyWorkerGroupEnable;
	}

	public void setServerNettyWorkerGroupEnable(boolean serverNettyWorkerGroupEnable) {
		this.serverNettyWorkerGroupEnable = serverNettyWorkerGroupEnable;
	}

	public boolean isServerPooledByteBufAllocatorEnable() {
		return serverPooledByteBufAllocatorEnable;
	}

	public void setServerPooledByteBufAllocatorEnable(boolean serverPooledByteBufAllocatorEnable) {
		this.serverPooledByteBufAllocatorEnable = serverPooledByteBufAllocatorEnable;
	}

	public boolean isEnableShutdownGracefully() {
		return enableShutdownGracefully;
	}

	public void setEnableShutdownGracefully(boolean enableShutdownGracefully) {
		this.enableShutdownGracefully = enableShutdownGracefully;
	}

	public int getShutdownWaitTimeSeconds() {
		return shutdownWaitTimeSeconds;
	}

	public void setShutdownWaitTimeSeconds(int shutdownWaitTimeSeconds) {
		this.shutdownWaitTimeSeconds = shutdownWaitTimeSeconds;
	}

	public boolean isUseEpollNativeSelector() {
		return useEpollNativeSelector;
	}

	public void setUseEpollNativeSelector(boolean useEpollNativeSelector) {
		this.useEpollNativeSelector = useEpollNativeSelector;
	}
}
