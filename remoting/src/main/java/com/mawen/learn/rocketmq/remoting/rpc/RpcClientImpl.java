package com.mawen.learn.rocketmq.remoting.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.InvokeCallback;
import com.mawen.learn.rocketmq.remoting.RemotingClient;
import com.mawen.learn.rocketmq.remoting.exception.RemotingConnectException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.mawen.learn.rocketmq.remoting.netty.ResponseFuture;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.admin.TopicStatsTable;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetEarliestStoretimeResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.UpdateConsumerOffsetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import io.netty.util.concurrent.ImmediateEventExecutor;
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
					rpcResponsePromise = handleCommonBodyRequest(addr, request, timeoutMs, TopicStatsTable.class);
					break;
				case GET_TOPIC_CONFIG:
					rpcResponsePromise = handleCommonBodyRequest(addr, request, timeoutMs, TopicConfigAndQueueMapping.class);
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
		String brokerName = cLientMetadata.getBrokerNameFromMessageQueue(mq);
		request.getHeader().setBrokerName(brokerName);
		return invoke(request, timeoutMs);
	}

	public void registerHook(RpcClientHook hook) {
		clientHookList.add(hook);
	}

	public Promise<RpcResponse> createResponseFuture() {
		return ImmediateEventExecutor.INSTANCE.newPromise();
	}

	public Promise<RpcResponse> handlePullMessage(String addr, RpcRequest rpcRequest, long timeoutMillis) throws Exception {
		RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);

		Promise<RpcResponse> responsePromise = createResponseFuture();

		InvokeCallback callback = new InvokeCallback() {
			@Override
			public void operationComplete(ResponseFuture responseFuture) {
			}

			@Override
			public void operationSucceed(RemotingCommand response) {
				try {
					switch (response.getCode()) {
						case ResponseCode.SUCCESS:
						case ResponseCode.PULL_NOT_FOUND:
						case ResponseCode.PULL_RETRY_IMMEDIATELY:
						case ResponseCode.PULL_OFFSET_MOVED:
							PullMessageRequestHeader responseHeader = response.decodeCommandCustomHeader(PullMessageRequestHeader.class);
							responsePromise.setSuccess(new RpcResponse(response.getCode(), responseHeader, response.getBody()));
						default:
							responsePromise.setSuccess(new RpcResponse(new RpcException(response.getCode(), "unexpected remote response code")));
					}
				}
				catch (Exception e) {
					String errorMessage = "process failed. addr: " + addr + ", timeoutMillis: " + timeoutMillis + ". Request: " + requestCommand;
					responsePromise.setSuccess(new RpcResponse(new RpcException(ResponseCode.RPC_UNKNOWN, errorMessage, e)));
				}
			}

			@Override
			public void operationFail(Throwable throwable) {
				String errorMessage = "process failed. addr: " + addr + ". Request: " + requestCommand;
				responsePromise.setSuccess(new RpcResponse(new RpcException(ResponseCode.RPC_UNKNOWN, errorMessage, throwable)));
			}
		};

		this.remotingClient.invokeAsync(addr, requestCommand, timeoutMillis, callback);

		return responsePromise;
	}

	public Promise<RpcResponse> handleGetMinOffset(String addr, RpcRequest rpcRequest, long timeoutMillis) throws Exception {
		Promise<RpcResponse> responsePromise = createResponseFuture();

		RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);

		RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);

		assert responseCommand != null;

		switch (responseCommand.getCode()) {
			case ResponseCode.SUCCESS:
				GetMinOffsetResponseHeader responseHeader = responseCommand.decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);
				responsePromise.setSuccess(new RpcResponse(responseCommand.getCode(), responseHeader, responseCommand.getBody()));
				break;
			default:
				responsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
		}

		return responsePromise;
	}

	public Promise<RpcResponse> handleGetMaxOffset(String addr, RpcRequest rpcRequest, long timeoutMillis) throws Exception {
		Promise<RpcResponse> responsePromise = createResponseFuture();

		RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);

		RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);

		assert responseCommand != null;

		switch (responseCommand.getCode()) {
			case ResponseCode.SUCCESS:
				GetMaxOffsetResponseHeader responseHeader = responseCommand.decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);
				responsePromise.setSuccess(new RpcResponse(responseCommand.getCode(), responseHeader, responseCommand.getBody()));
				break;
			default:
				responsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
		}

		return responsePromise;
	}

	public Promise<RpcResponse> handleSearchOffset(String addr, RpcRequest rpcRequest, long timeoutMillis) throws Exception {
		Promise<RpcResponse> responsePromise = createResponseFuture();

		RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);

		RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);

		assert responseCommand != null;

		switch (responseCommand.getCode()) {
			case ResponseCode.SUCCESS:
				SearchOffsetResponseHeader responseHeader = responseCommand.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
				responsePromise.setSuccess(new RpcResponse(responseCommand.getCode(), responseHeader, responseCommand.getBody()));
				break;
			default:
				responsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
		}

		return responsePromise;
	}

	public Promise<RpcResponse> handleGetEarliestMsgStoreTime(String addr, RpcRequest rpcRequest, long timeoutMillis) throws Exception {
		Promise<RpcResponse> responsePromise = createResponseFuture();

		RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);

		RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);

		assert responseCommand != null;

		switch (responseCommand.getCode()) {
			case ResponseCode.SUCCESS:
				GetEarliestStoretimeResponseHeader responseHeader = responseCommand.decodeCommandCustomHeader(GetEarliestStoretimeResponseHeader.class);
				responsePromise.setSuccess(new RpcResponse(responseCommand.getCode(), responseHeader, responseCommand.getBody()));
				break;
			default:
				responsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
		}

		return responsePromise;
	}

	public Promise<RpcResponse> handleUpdateConsumerOffset(String addr, RpcRequest rpcRequest, long timeoutMillis) throws Exception {
		Promise<RpcResponse> responsePromise = createResponseFuture();

		RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);

		RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);

		assert responseCommand != null;

		switch (responseCommand.getCode()) {
			case ResponseCode.SUCCESS:
				UpdateConsumerOffsetResponseHeader responseHeader = responseCommand.decodeCommandCustomHeader(UpdateConsumerOffsetResponseHeader.class);
				responsePromise.setSuccess(new RpcResponse(responseCommand.getCode(), responseHeader, responseCommand.getBody()));
				break;
			default:
				responsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
		}

		return responsePromise;
	}

	public Promise<RpcResponse> handleCommonBodyRequest(String addr, RpcRequest rpcRequest, long timeoutMillis, Class bodyClass) throws Exception {
		Promise<RpcResponse> responsePromise = createResponseFuture();

		RemotingCommand requestCommand = RpcClientUtils.createCommandForRpcRequest(rpcRequest);

		RemotingCommand responseCommand = this.remotingClient.invokeSync(addr, requestCommand, timeoutMillis);

		assert responseCommand != null;

		switch (responseCommand.getCode()) {
			case ResponseCode.SUCCESS:
				responsePromise.setSuccess(new RpcResponse(ResponseCode.SUCCESS, null, RemotingSerializable.decode(responseCommand.getBody(), bodyClass)));
				break;
			default:
				responsePromise.setSuccess(new RpcResponse(new RpcException(responseCommand.getCode(), "unknown remote error")));
		}

		return responsePromise;
	}

	private String getBrokerAddrByNameOrException(String bname) throws RpcException {
		String addr = this.cLientMetadata.findMasterBrokerAddr(bname);
		if (addr == null) {
			throw new RpcException(ResponseCode.SYSTEM_ERROR, "cannot find addr for broker " + bname);
		}
		return addr;
	}

	private void processFailedResponse(String addr, RemotingCommand command, ResponseFuture responseFuture, Promise<RpcResponse> rpcResponsePromise) {
		RemotingCommand responseCommand = responseFuture.getResponseCommand();
		if (responseCommand != null) {
			return;
		}

		int errorCode = ResponseCode.RPC_UNKNOWN;
		String errorMessage = null;

		if (!responseFuture.isSendRequestOK()) {
			errorCode = ResponseCode.RPC_SEND_TO_CHANNEL_FAILED;
			errorMessage = "send request failed to " + addr + ". Request: " + command;
		}
		else if (responseFuture.isTimeout()) {
			errorCode = ResponseCode.RPC_TIME_OUT;
			errorMessage = "wait response from " + addr + " timeout: " + responseFuture.getTimeoutMillis() + "ms. Request: " + command;
		}
		else {
			errorMessage = "unknown reason. addr: " + addr + ", timeoutMillis: " + responseFuture.getTimeoutMillis() + "ms. Request: " + command;
		}

		rpcResponsePromise.setSuccess(new RpcResponse(new RpcException(errorCode, errorMessage)));
	}
}
