package com.mawen.learn.rocketmq.remoting.netty;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mawen.learn.rocketmq.remoting.InvokeCallback;
import com.mawen.learn.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.Channel;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/15
 */
public class ResponseFuture {

	private final Channel channel;

	private final int opaque;

	private final RemotingCommand request;

	private final long timeoutMillis;

	private final InvokeCallback invokeCallback;

	private final long beginTimestamp = System.currentTimeMillis();

	private final CountDownLatch countDownLatch = new CountDownLatch(1);

	private final SemaphoreReleaseOnlyOnce once;

	private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);

	private volatile RemotingCommand responseCommand;

	private volatile boolean sendRequestOK = true;

	private volatile Throwable cause;

	private volatile boolean interrupted = false;


	public ResponseFuture(Channel channel, int opaque, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback, SemaphoreReleaseOnlyOnce once) {
		this.channel = channel;
		this.opaque = opaque;
		this.request = request;
		this.timeoutMillis = timeoutMillis;
		this.invokeCallback = invokeCallback;
		this.once = once;
	}

	public void executeInvokeCallback() {
		if (this.invokeCallback != null) {
			if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
				RemotingCommand response = getResponseCommand();
				if (response != null) {
					invokeCallback.operationSucceed(response);
				}
				else {
					if (!isSendRequestOK()) {
						invokeCallback.operationFail(new RemotingSendRequestException(channel.remoteAddress().toString()));
					}
					else if (isTimeout()) {
						invokeCallback.operationFail(new RemotingTimeoutException(channel.remoteAddress().toString()));
					}
					else {
						invokeCallback.operationFail(new RemotingException(getRequestCommand().toString(), cause));
					}
				}
				invokeCallback.operationComplete(this);
			}
		}
	}

	public void interrupt() {
		this.interrupted = true;
		executeInvokeCallback();
	}

	public void release() {
		if (this.once != null) {
			this.once.release();
		}
	}

	public boolean isTimeout() {
		long diff = System.currentTimeMillis() - this.beginTimestamp;
		return diff > this.timeoutMillis;
	}

	public RemotingCommand waitResponse(long timeoutMillis) throws InterruptedException {
		this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
		return this.responseCommand;
	}

	public void putResponse(final RemotingCommand responseCommand) {
		this.responseCommand = responseCommand;
		this.countDownLatch.countDown();
	}

	public long getBeginTimestamp() {
		return beginTimestamp;
	}

	public CountDownLatch getCountDownLatch() {
		return countDownLatch;
	}

	public SemaphoreReleaseOnlyOnce getOnce() {
		return once;
	}

	public AtomicBoolean getExecuteCallbackOnlyOnce() {
		return executeCallbackOnlyOnce;
	}

	public RemotingCommand getResponseCommand() {
		return responseCommand;
	}

	public void setResponseCommand(RemotingCommand responseCommand) {
		this.responseCommand = responseCommand;
	}

	public boolean isSendRequestOK() {
		return sendRequestOK;
	}

	public void setSendRequestOK(boolean sendRequestOK) {
		this.sendRequestOK = sendRequestOK;
	}

	public Throwable getCause() {
		return cause;
	}

	public void setCause(Throwable cause) {
		this.cause = cause;
	}

	public boolean isInterrupted() {
		return interrupted;
	}

	public void setInterrupted(boolean interrupted) {
		this.interrupted = interrupted;
	}

	public RemotingCommand getRequestCommand() {
		return request;
	}

	public InvokeCallback getInvokeCallback() {
		return invokeCallback;
	}

	public long getTimeoutMillis() {
		return timeoutMillis;
	}

	public Channel getChannel() {
		return channel;
	}

	@Override
	public String toString() {
		return "ResponseFuture [responseCommand=" + responseCommand + ", sendRequestOK=" + sendRequestOK
				+ ", cause=" + cause + ", opaque=" + opaque + ", timeoutMillis=" + timeoutMillis
				+ ", invokeCallback=" + invokeCallback + ", beginTimestamp=" + beginTimestamp
				+ ", countDownLatch=" + countDownLatch + "]";
	}
}
