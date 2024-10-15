package com.mawen.learn.rocketmq.remoting;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public interface RPCHook {

	void doBeforeRequest(final String remoteAddr, final RemotingCommand request);

	void doAfterResponse(final String remoteAddr, final RemotingCommand request, final RemotingCommand response);
}
