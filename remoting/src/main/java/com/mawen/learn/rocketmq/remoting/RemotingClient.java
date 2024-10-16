package com.mawen.learn.rocketmq.remoting;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import com.mawen.learn.rocketmq.remoting.exception.RemotingConnectException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.mawen.learn.rocketmq.remoting.netty.NettyRequestProcessor;
import com.mawen.learn.rocketmq.remoting.netty.ResponseFuture;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public interface RemotingClient extends RemotingService {

	void updateNameServerAddressList(final List<String> addrs);

	List<String> getNameServerAddressList();

	List<String> getAvailableNameServerList();

	RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException;

	void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException;

	void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

	default CompletableFuture<RemotingCommand> invoke(final String addr, final RemotingCommand request, final long timeoutMillis) {
		CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
		try {
			invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
				@Override
				public void operationComplete(ResponseFuture responseFuture) {

				}

				@Override
				public void operationSucceed(RemotingCommand response) {
					future.complete(response);
				}

				@Override
				public void operationFail(Throwable throwable) {
					future.completeExceptionally(throwable);
				}
			});
		}
		catch (Throwable t) {
			future.completeExceptionally(t);
		}
		return future;
	}

	void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);

	void setCallbackExecutor(final ExecutorService callbackService);

	boolean isChannelWritable(final String addr);

	boolean isAddressReachable(final String addr);

	void closeChannels(final List<String> addrList);
}
