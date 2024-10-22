package com.mawen.learn.rocketmq.client.rpchook;

import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/22
 */
@AllArgsConstructor
public class NamespaceRpcHook implements RPCHook {

	private final ClientConfig clientConfig;

	@Override
	public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
		if (StringUtils.isNotEmpty(clientConfig.getNamespaceV2())) {
			request.addExtField(MixAll.RPC_REQUEST_HEADER_NAMESPACED_FIELD, "true");
			request.addExtField(MixAll.RPC_REQUEST_HEADER_NAMESPACE_FIELD, clientConfig.getNamespaceV2());
		}
	}

	@Override
	public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

	}
}
