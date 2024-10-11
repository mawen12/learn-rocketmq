package com.mawen.learn.rocketmq.remoting;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public interface InvokeCallback {

	void operationComplete(final ResponseFuture responseFuture);

	default void operationSucceed(final RemotingCommand response) {

	}

	default void operationFail(final Throwable throwable) {

	}
}
