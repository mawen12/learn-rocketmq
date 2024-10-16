package com.mawen.learn.rocketmq.remoting.rpchook;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.protocol.RequestType;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class StreamTypeRPCHook implements RPCHook {

	@Override
	public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
		request.addExtField(MixAll.REQ_T, String.valueOf(RequestType.STREAM.getCode()));
	}

	@Override
	public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
	}
}
