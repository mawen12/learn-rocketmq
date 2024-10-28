package com.mawen.learn.rocketmq.client.impl.mqclient;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.consumer.AckCallback;
import com.mawen.learn.rocketmq.client.consumer.AckResult;
import com.mawen.learn.rocketmq.client.consumer.PopCallback;
import com.mawen.learn.rocketmq.client.consumer.PopResult;
import com.mawen.learn.rocketmq.client.consumer.PullCallback;
import com.mawen.learn.rocketmq.client.consumer.PullResult;
import com.mawen.learn.rocketmq.client.consumer.PullStatus;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.OffsetNotFoundException;
import com.mawen.learn.rocketmq.client.impl.ClientRemotingProcessor;
import com.mawen.learn.rocketmq.client.impl.CommunicationMode;
import com.mawen.learn.rocketmq.client.impl.MQClientAPIImpl;
import com.mawen.learn.rocketmq.client.impl.admin.MqClientAdminImpl;
import com.mawen.learn.rocketmq.client.impl.consumer.PopRequest;
import com.mawen.learn.rocketmq.client.impl.consumer.PullResultExt;
import com.mawen.learn.rocketmq.client.producer.SendResult;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageBatch;
import com.mawen.learn.rocketmq.common.message.MessageClientIDSetter;
import com.mawen.learn.rocketmq.common.message.MessageDecoder;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.InvokeCallback;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.netty.NettyClientConfig;
import com.mawen.learn.rocketmq.remoting.netty.ResponseFuture;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.LockBatchResponseBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import com.mawen.learn.rocketmq.remoting.protocol.header.AckMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.HeartbeatRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.LockBatchMqRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.NotificationRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.NotificationResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2;
import com.mawen.learn.rocketmq.remoting.protocol.header.UnlockBatchMqRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import io.netty.util.concurrent.CompleteFuture;
import lombok.Getter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/28
 */
@Getter
public class MQClientAPIExt extends MQClientAPIImpl {

	private static final Logger log = LoggerFactory.getLogger(MQClientAPIExt.class);

	private final ClientConfig clientConfig;
	private final MqClientAdminImpl mqClientAdmin;

	public MQClientAPIExt(ClientConfig clientConfig, NettyClientConfig nettyClientConfig, ClientRemotingProcessor clientRemotingProcessor, RPCHook rpcHook) {
		super(nettyClientConfig, clientRemotingProcessor, rpcHook, clientConfig);

		this.clientConfig = clientConfig;
		this.mqClientAdmin = new MqClientAdminImpl(getRemotingClient());
	}

	public boolean updateNameServerAddressList() {
		if (this.clientConfig.getNamesrvAddr() != null) {
			this.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
			log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
			return true;
		}
		return false;
	}

	public CompletableFuture<Void> sendHeartbeatOneway(String brokerAddr, HeartbeatData heartbeatData, long timeoutMillis) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		try {
			RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, new HeartbeatRequestHeader());
			request.setLanguage(clientConfig.getLanguage());
			request.setBody(heartbeatData.encode());

			this.getRemotingClient().invokeOneway(brokerAddr, request, timeoutMillis);
			future.complete(null);
		}
		catch (Throwable t) {
			future.completeExceptionally(t);
		}
		return future;
	}

	public CompletableFuture<Integer> sendHeartbeatAsync(String brokerAddr, HeartbeatData heartbeatData, long timeoutMillis) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, new HeartbeatRequestHeader());
		request.setLanguage(clientConfig.getLanguage());
		request.setBody(heartbeatData.encode());

		return this.getRemotingClient().invoke(brokerAddr, request, timeoutMillis).thenCompose(response -> {
			CompletableFuture<Integer> future = new CompletableFuture<>();
			if (response.isResponseSuccess()) {
				future.complete(response.getVersion());
			}
			else {
				future.completeExceptionally(new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr));
			}
			return future;
		});
	}

	public CompletableFuture<SendResult> sendMessageAsync(String brokerAddr, String brokerName, Message msg, SendMessageRequestHeader requestHeader, long timeoutMillis) {
		SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
		request.setBody(msg.getBody());

		return this.getRemotingClient().invoke(brokerAddr, request, timeoutMillis).thenCompose(response -> {
			CompletableFuture<SendResult> future = new CompletableFuture<>();
			try {
				future.complete(this.processSendResponse(brokerName, msg, response, brokerAddr));
			}
			catch (Exception e) {
				future.completeExceptionally(e);
			}
			return future;
		});
	}

	public CompletableFuture<SendResult> sendMessageAsync(String brokerAddr, String brokerName, List<? extends Message> msgList, SendMessageRequestHeader requestHeader, long timeoutMillis) {
		SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, requestHeaderV2);

		CompletableFuture<SendResult> future = new CompletableFuture<>();
		try {
			requestHeader.setBatch(true);
			MessageBatch msgBatch = MessageBatch.generateFromList(msgList);
			MessageClientIDSetter.setUniqID(msgBatch);
			byte[] body = msgBatch.encode();
			msgBatch.setBody(body);

			request.setBody(body);
			return this.getRemotingClient().invoke(brokerAddr, request, timeoutMillis).thenCompose(response -> {
				CompletableFuture<SendResult> future1 = new CompletableFuture<>();
				try {
					future1.complete(processSendResponse(brokerName, msgBatch, response, brokerAddr));
				}
				catch (Exception e) {
					future1.completeExceptionally(e);
				}
				return future1;
			});
		}
		catch (Throwable t) {
			future.completeExceptionally(t);
		}
		return future;
	}

	public CompletableFuture<RemotingCommand> sendMessageBackAsync(String brokerAddr, ConsumerSendMsgBackRequestHeader requestHeader, long timeoutMillis) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);
		return this.getRemotingClient().invoke(brokerAddr, request, timeoutMillis);
	}

	public CompletableFuture<PopResult> popMessageAsync(String brokerAddr, String brokerName, PopMessageRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<PopResult> future = new CompletableFuture<>();
		try {
			this.popMessageAsync(brokerName, brokerAddr, requestHeader, timeoutMillis, new PopCallback() {
				@Override
				public void onSuccess(PopResult popResult) {
					future.complete(popResult);
				}

				@Override
				public void onException(Throwable e) {
					future.completeExceptionally(e);
				}
			});
		}
		catch (Throwable t) {
			future.completeExceptionally(t);
		}
		return future;
	}

	public CompletableFuture<AckResult> ackMessageAsync(String brokerAddr, AckMessageRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<AckResult> future = new CompletableFuture<>();
		try {
			this.ackMessageAsync(brokerAddr, requestHeader, new AckCallback() {
				@Override
				public void onSuccess(AckResult ackResult) {
					future.complete(ackResult);
				}

				@Override
				public void onException(Throwable e) {
					future.completeExceptionally(e);
				}
			});
		}
		catch (Throwable t) {
			future.completeExceptionally(t);
		}
		return future;
	}

	public CompletableFuture<AckResult> batchAckMessageAsync(String brokerAddr, String topic, String consumerGroup, List<String> extraInfoList, long timeoutMillis) {
		CompletableFuture<AckResult> future = new CompletableFuture<>();
		try {
			this.batchAckMessageAsync(brokerAddr, timeoutMillis, new AckCallback() {
				@Override
				public void onSuccess(AckResult ackResult) {
					future.complete(ackResult);
				}

				@Override
				public void onException(Throwable e) {
					future.completeExceptionally(e);
				}
			});
		}
		catch (Throwable t) {
			future.completeExceptionally(t);
		}
		return future;
	}

	public CompletableFuture<AckResult> changeInvisibleTimeAsync(String brokerAddr, String brokerName, ChangeInvisibleTimeRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<AckResult> future = new CompletableFuture<>();
		try {
			this.changeInvisibleTimeAsync(brokerName, brokerAddr, requestHeader, timeoutMillis, new AckCallback() {
				@Override
				public void onSuccess(AckResult ackResult) {
					future.complete(ackResult);
				}

				@Override
				public void onException(Throwable e) {
					future.completeExceptionally(e);
				}
			});
		}
		catch (Throwable t) {
			future.completeExceptionally(t);
		}
		return future;
	}

	public CompletableFuture<PullResult> pullMessageAsync(String brokerAddr, PullMessageRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<PullResult> future = new CompletableFuture<>();
		try {
			this.pullMessage(brokerAddr, requestHeader, timeoutMillis, CommunicationMode.ASYNC, new PullCallback() {
				@Override
				public void onSuccess(PullResult pullResult) {
					if (pullResult instanceof PullResultExt) {
						PullResultExt pullResultExt = (PullResultExt) pullResult;
						if (PullStatus.FOUND.equals(pullResult.getPullStatus())) {
							List<MessageExt> messageExtList = MessageDecoder.decodeBatch(ByteBuffer.wrap(pullResultExt.getMessageBinary()), true, false, true);
							pullResult.setMsgFoundList(messageExtList);
						}
					}
					future.complete(pullResult);
				}

				@Override
				public void onException(Throwable e) {
					future.completeExceptionally(e);
				}
			});
		}
		catch (Throwable t) {
			future.completeExceptionally(t);
		}
		return future;
	}

	public CompletableFuture<Long> queryConsumerOffsetWithFuture(String brokerAddr, QueryConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);

		return this.getRemotingClient().invoke(brokerAddr, request, timeoutMillis).thenCompose(response -> {
			CompletableFuture<Long> future = new CompletableFuture<>();
			switch (response.getCode()) {
				case ResponseCode.SUCCESS:
					try {
						QueryConsumerOffsetResponseHeader responseHeader = response.decodeCommandCustomHeader(QueryConsumerOffsetResponseHeader.class);
						future.complete(responseHeader.getOffset());
					}
					catch (RemotingCommandException e) {
						future.completeExceptionally(e);
					}
					break;
				case ResponseCode.QUERY_NOT_FOUND:
					future.completeExceptionally(new OffsetNotFoundException(response.getCode(), response.getRemark(), brokerAddr));
					break;
				default:
					future.completeExceptionally(new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr));
					break;
			}
			return future;
		});
	}

	public CompletableFuture<Void> updateConsumeOffsetOneway(String brokerAddr, UpdateConsumerOffsetRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		try {
			RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);
			this.getRemotingClient().invoke(brokerAddr, request, timeoutMillis);
			future.complete(null);
		}
		catch (Throwable t) {
			future.completeExceptionally(t);
		}
		return future;
	}

	public CompletableFuture<List<String>> getConsumerListByGroupAsync(String brokerAddr, GetConsumerListByGroupRequestHeader requestHeader, long timeoutMillis) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);

		CompletableFuture<List<String>> future = new CompletableFuture<>();
		try {
			this.getRemotingClient().invokeAsync(brokerAddr, request, timeoutMillis, new InvokeCallback() {
				@Override
				public void operationComplete(ResponseFuture responseFuture) {

				}

				@Override
				public void operationSucceed(RemotingCommand response) {
					if (response.isResponseSuccess()) {
						switch (response.getCode()) {
							case ResponseCode.SUCCESS:
								if (response.getBody() != null) {
									GetConsumerListByGroupResponseBody body = GetConsumerListByGroupResponseBody.decode(response.getBody(), GetConsumerListByGroupResponseBody.class);
									future.complete(body.getConsumerIdList());
								}
								return;
							case ResponseCode.SYSTEM_ERROR:
								future.complete(Collections.emptyList());
								return;
							default:
								break;
						}
					}
					future.completeExceptionally(new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr));
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

	public CompletableFuture<Long> getMaxOffset(String brokerAddr, GetMaxOffsetRequestHeader requestHeader, long timeoutMillis) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);

		CompletableFuture<Long> future = new CompletableFuture<>();
		try {
			this.getRemotingClient().invokeAsync(brokerAddr, request, timeoutMillis, new InvokeCallback() {
				@Override
				public void operationComplete(ResponseFuture responseFuture) {
				}

				@Override
				public void operationSucceed(RemotingCommand response) {
					if (response.isResponseSuccess()) {
						try {
							GetMaxOffsetResponseHeader responseHeader = response.decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);
							future.complete(responseHeader.getOffset());
						}
						catch (Throwable t) {
							future.completeExceptionally(t);
						}
					}
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

	public CompletableFuture<Long> getMinOffset(String brokerAddr, GetMinOffsetRequestHeader requestHeader, long timeoutMillis) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, requestHeader);

		CompletableFuture<Long> future = new CompletableFuture<>();
		try {
			this.getRemotingClient().invokeAsync(brokerAddr, request, timeoutMillis, new InvokeCallback() {
				@Override
				public void operationComplete(ResponseFuture responseFuture) {
				}

				@Override
				public void operationSucceed(RemotingCommand response) {
					if (response.isResponseSuccess()) {
						try {
							GetMinOffsetResponseHeader responseHeader = response.decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);
							future.complete(responseHeader.getOffset());
						}
						catch (Throwable t) {
							future.completeExceptionally(t);
						}
					}
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

	public CompletableFuture<Long> searchOffset(String brokerAddr, SearchOffsetRequestHeader requestHeader, long timeoutMillis) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);

		return this.getRemotingClient().invoke(brokerAddr, request, timeoutMillis).thenCompose(response -> {
			CompletableFuture<Long> future = new CompletableFuture<>();
			if (response.isResponseSuccess()) {
				try {
					SearchOffsetResponseHeader responseHeader = response.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
					future.complete(responseHeader.getOffset());
				}
				catch (Throwable t) {
					future.completeExceptionally(t);
				}
			}
			else {
				future.completeExceptionally(new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr));
			}
		});
	}

	public CompletableFuture<Set<MessageQueue>> lockBatchMQWithFuture(String brokerAddr, LockBatchRequestBody requestBody, long timeoutMillis) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, new LockBatchMqRequestHeader());
		request.setBody(requestBody.encode());

		return this.getRemotingClient().invoke(brokerAddr, request, timeoutMillis).thenCompose(response -> {
			CompletableFuture<Set<MessageQueue>> future = new CompletableFuture<>();
			if (response.isResponseSuccess()) {
				try {
					LockBatchResponseBody body = LockBatchResponseBody.decode(response.getBody(), LockBatchResponseBody.class);
					future.complete(body.getLockOKMQSet());
				}
				catch (Throwable t) {
					future.completeExceptionally(t);
				}
			}
			else {
				future.completeExceptionally(new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr));
			}
			return future;
		});
	}

	public CompletableFuture<Void> unlockBatchMQOneway(String brokerAddr, UnlockBatchRequestBody requestBody, long timeoutMillis) {
		CompletableFuture<Void> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, new UnlockBatchMqRequestHeader());
		request.setBody(requestBody.encode());

		try {
			this.getRemotingClient().invokeOneway(brokerAddr, request, timeoutMillis);
			future.complete(null);
		}
		catch (Exception e) {
			future.completeExceptionally(e);
		}
		return future;
	}

	public CompletableFuture<Boolean> notification(String brokerAddr, NotificationRequestHeader requestHeader, long timeoutMillis) {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.NOTIFICATION, requestHeader);

		return this.getRemotingClient().invoke(brokerAddr, request, timeoutMillis).thenCompose(response -> {
			CompletableFuture<Boolean> future = new CompletableFuture<>();
			if (response.isResponseSuccess()) {
				try {
					NotificationResponseHeader responseHeader = response.decodeCommandCustomHeader(NotificationResponseHeader.class);
					future.complete(responseHeader.isHasMsg());
				}
				catch (Throwable t) {
					future.completeExceptionally(t);
				}
			}
			return future;
		});
	}

	public CompletableFuture<RemotingCommand> invoke(String brokerAddr, RemotingCommand request, long timeoutMillis) {
		return getRemotingClient().invoke(brokerAddr, request, timeoutMillis);
	}

	public CompletableFuture<Void> invokeOneway(String brokerAddr, RemotingCommand request, long timeoutMillis) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		try {
			this.getRemotingClient().invokeOneway(brokerAddr, request, timeoutMillis);
			future.complete(null);
		}
		catch (Exception e) {
			future.completeExceptionally(e);
		}
		return future;
	}
}
