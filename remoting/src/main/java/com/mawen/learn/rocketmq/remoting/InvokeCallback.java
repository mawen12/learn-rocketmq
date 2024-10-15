package com.mawen.learn.rocketmq.remoting;

import com.mawen.learn.rocketmq.remoting.netty.ResponseFuture;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;

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
