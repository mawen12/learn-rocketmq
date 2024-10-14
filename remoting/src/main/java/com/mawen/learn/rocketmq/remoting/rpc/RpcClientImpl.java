package com.mawen.learn.rocketmq.remoting.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.RemotingClient;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import io.netty.util.concurrent.Promise;

import static com.mawen.learn.rocketmq.remoting.protocol.RequestCode.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class RpcClientImpl implements RpcClient{

	private ClientMetadata cLientMetadata;

	private RemotingClient remotingClient;

	private List<RpcClientHook> clientHookList = new ArrayList<>();

	public RpcClientImpl(ClientMetadata cLientMetadata, RemotingClient remotingClient) {
		this.cLientMetadata = cLientMetadata;
		this.remotingClient = remotingClient;
	}

	@Override
	public Future<RpcResponse> invoke(RpcRequest request, long timeoutMs) throws RpcException {
		if (clientHookList.size() > 0) {
			for (RpcClientHook rpcClientHook : clientHookList) {
				RpcResponse response = rpcClientHook.beforeRequest(request);
				if (response != null) {
					return createResponseFuture().setSuccess(response);
				}
			}
		}

		String addr = getBrokerAddrByNameOrException(request.getHeader().brokerName);
		Promise<RpcResponse> rpcResponsePromise = null;
		try {
			switch (request.getCode()) {
				case PULL_MESSAGE:
					rpcResponsePromise = handlePullMessage(addr, request, timeoutMs);
					break;
				case GET_MIN_OFFSET:
					rpcResponsePromise = handleGetMinOffset(addr, request, timeoutMs);
					break;
				case GET_MAX_OFFSET:
					rpcResponsePromise = handleGetMaxOffset(addr, request, timeoutMs);
					break;
				case SEARCH_OFFSET_BY_TIMESTAMP:
					rpcResponsePromise = handleSearchOffset(addr, request, timeoutMs);
					break;
				case GET_EARLIEST_MSG_STORETIME:
					rpcResponsePromise = handleGetEarliestMsgStoreTime(addr, request, timeoutMs);
					break;
				case QUERY_CONSUMER_OFFSET:
					rpcResponsePromise = handleUpdateConsumerOffset(addr, request, timeoutMs);
					break;
				case GET_TOPIC_STATS_INFO:
					rpcResponsePromise = handleCommonBodyRequest(addr, request, timeoutMs);
					break;
				case GET_TOPIC_CONFIG:
					rpcResponsePromise = hanndleCommonBodyRequest(addr, request, timeoutMs);
					break;
				default:
					throw new RpcException(ResponseCode.REQUEST_CODE_NOT_SUPPORTED, "Unknown request code " + request.getCode());
			}
		}
		catch (RpcException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RpcException(ResponseCode.RPC_UNKNOWN, "error from remoting layer", e);
		}
		return rpcResponsePromise;
	}

	@Override
	public Future<RpcResponse> invoke(MessageQueue mq, RpcRequest request, long timeoutMs) throws RpcException {
		return null;
	}

	public void registerHook(RpcClientHook hook) {
		clientHookList.add(hook);
	}

	private String getBrokerAddrByNameOrException(String bname) throws RpcException {
		String addr = this.cLientMetadata.findMasterBrokerAddr(bname);
		if (addr == null) {
			throw new RpcException(ResponseCode.SYSTEM_ERROR, "cannot find addr for broker " + bname);
		}
		return addr;
	}

	private void processFailedResponse(String addr, RemotingCommand command, ResponseFuture responseFuture, Promise<RpcResponse> rpcResponsePromise) {

	}
}
