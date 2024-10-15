package com.mawen.learn.rocketmq.remoting;

import java.util.concurrent.ExecutorService;

import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.mawen.learn.rocketmq.remoting.netty.NettyRequestProcessor;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public interface RemotingServer extends RemotingService{

	void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);

	void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

	int localListenPort();

	Pair<NettyRequestProcessor, ExecutorService> getProcessor(final int requestCode);

	Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessor();

	RemotingServer newRemotingServer(int port);

	void removeRemotingServer(int port);

	RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException;

	void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingSendRequestException, RemotingTimeoutException;

	void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;
}
