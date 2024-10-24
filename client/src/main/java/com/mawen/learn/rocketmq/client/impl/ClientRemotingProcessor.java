package com.mawen.learn.rocketmq.client.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.client.impl.producer.MQProducerInner;
import com.mawen.learn.rocketmq.client.producer.RequestFutureHolder;
import com.mawen.learn.rocketmq.client.producer.RequestResponseFuture;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.compression.Compressor;
import com.mawen.learn.rocketmq.common.compression.CompressorFactory;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageAccessor;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageDecoder;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.sysflag.MessageSysFlag;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;
import com.mawen.learn.rocketmq.remoting.common.RemotingHelper;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.netty.NettyRequestProcessor;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.GetConsumerStatusBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.ResetOffsetBody;
import com.mawen.learn.rocketmq.remoting.protocol.header.CheckTransactionStateRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerStatsRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import io.netty.channel.ChannelHandlerContext;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/22
 */
@AllArgsConstructor
public class ClientRemotingProcessor implements NettyRequestProcessor {

	private final Logger log = LoggerFactory.getLogger(ClientRemotingProcessor.class);

	private final MQClientInstance mqClientFactory;

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
		switch (request.getCode()) {
			case RequestCode.CHECK_TRANSACTION_STATE:
				return this.checkTransactionState(ctx, request);
			case RequestCode.NOTIFY_CONSUMER_IDS_CHANGED:
				return this.notifyConsumerIdsChanged(ctx, request);
			case RequestCode.RESET_CONSUMER_CLIENT_OFFSET:
				return this.resetOffset(ctx, request);
			case RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT:
				return this.getConsumerStats(ctx, request);
			case RequestCode.GET_CONSUMER_RUNNING_INFO:
				return this.getConsumerRunningInfo(ctx, request);
			case RequestCode.CONSUME_MESSAGE_DIRECTLY:
				return this.consumeMessageDirectly(ctx, request);
			case RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT:
				return this.receiveReplayMessage(ctx, request);
			default:
				break;
		}
		return null;
	}

	@Override
	public boolean rejectRequest() {
		return false;
	}

	private RemotingCommand checkTransactionState(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
		CheckTransactionStateRequestHeader requestHeader = request.decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);

		ByteBuffer buffer = ByteBuffer.wrap(request.getBody());
		MessageExt messageExt = MessageDecoder.decode(buffer);
		if (messageExt != null) {
			if (StringUtils.isNotEmpty(this.mqClientFactory.getClientConfig().getNamespace())) {
				messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.mqClientFactory.getClientConfig().getNamespace()));
			}

			String transactionId = messageExt.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
			if (transactionId != null && !"".equals(transactionId)) {
				messageExt.setTransactionId(transactionId);
			}

			String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
			if (group != null) {
				MQProducerInner producer = this.mqClientFactory.selectProducer(group);
				if (producer != null) {
					String addr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
					producer.checkTransactionState(addr, messageExt, requestHeader);
				}
				else {
					log.debug("checkTransactionState, pick producer by group[{}] failed", group);
				}
			}
			else {
				log.warn("checkTransactionState, pick producer group failed");
			}
		}
		else {
			log.warn("checkTransactionState, decode message failed");
		}

		return null;
	}

	private RemotingCommand notifyConsumerIdsChanged(ChannelHandlerContext ctx, RemotingCommand request) {
		try {
			NotifyConsumerIdsChangedRequestHeader requestHeader = request.decodeCommandCustomHeader(NotifyConsumerIdsChangedRequestHeader.class);
			log.info("receive broker's notification[{}], the consumer group: {} changed, rebalance immediately", RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getConsumerGroup());
			this.mqClientFactory.rebalanceImmediately();
		}
		catch (Exception e) {
			log.error("notifyConsumerIdsChanged exception", UtilAll.exceptionSimpleDesc(e));
		}
		return null;
	}

	private RemotingCommand resetOffset(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
		ResetOffsetRequestHeader requestHeader = request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
		log.info("invoke reset offset operation from broker, brokerAddr={}, topic={}, group={}, timestamp={}",
				RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup(), requestHeader.getTimestamp());
		Map<MessageQueue, Long> offsetTable = new HashMap<>(4);
		if (request.getBody() != null) {
			ResetOffsetBody body = RemotingSerializable.decode(request.getBody(), ResetOffsetBody.class);
			offsetTable = body.getOffsetTable();
		}
		this.mqClientFactory.resetOffset(requestHeader.getTopic(), requestHeader.getGroup(), offsetTable);
		return null;
	}

	private RemotingCommand getConsumerStats(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
		RemotingCommand response = RemotingCommand.createResponseCommand(null);
		GetConsumerStatsRequestHeader requestHeader = request.decodeCommandCustomHeader(GetConsumerStatsRequestHeader.class);

		Map<MessageQueue, Long> offsetTable = this.mqClientFactory.getConsumerStatus(requestHeader.getTopic(), requestHeader.getGroup());
		GetConsumerStatusBody body = new GetConsumerStatusBody();
		body.setMessageQueueTable(offsetTable);

		response.setBody(body.encode());
		response.setCode(ResponseCode.SUCCESS);
		return response;
	}

	private RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
		RemotingCommand response = RemotingCommand.createResponseCommand(null);
		GetConsumerRunningInfoRequestHeader requestHeader = request.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);

		ConsumerRunningInfo consumerRunningInfo = this.mqClientFactory.consumerRunningInfo(requestHeader.getConsumerGroup());
		if (consumerRunningInfo != null) {
			if (requestHeader.isJstackEnable()) {
				Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
				String jstack = UtilAll.jstack(map);
				consumerRunningInfo.setJstack(jstack);
			}

			response.setCode(ResponseCode.SUCCESS);
			response.setBody(consumerRunningInfo.encode());
		}
		else {
			response.setCode(ResponseCode.SYSTEM_ERROR);
			response.setRemark(String.format("The Consumer Group <%s> not exist in this consumer", requestHeader.getConsumerGroup()));
		}

		return response;
	}

	private RemotingCommand consumeMessageDirectly(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
		RemotingCommand response = RemotingCommand.createResponseCommand(null);
		ConsumeMessageDirectlyResultRequestHeader requestHeader = request.decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class);

		MessageExt msg = MessageDecoder.clientDecode(ByteBuffer.wrap(request.getBody()), true);

		ConsumeMessageDirectlyResult result = this.mqClientFactory.consumeMessageDirectly(msg, requestHeader.getConsumerGroup(), requestHeader.getBrokerName());

		if (request != null) {
			response.setCode(ResponseCode.SUCCESS);
			response.setBody(result.encode());
		}
		else {
			response.setCode(ResponseCode.SYSTEM_ERROR);
			response.setRemark(String.format("The Consumer Group <%s> not exist in this consumer", requestHeader.getConsumerGroup()));
		}

		return response;
	}

	private RemotingCommand receiveReplayMessage(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
		RemotingCommand response = RemotingCommand.createResponseCommand(null);
		long receiveTime = System.currentTimeMillis();
		ReplyMessageRequestHeader requestHeader = request.decodeCommandCustomHeader(ReplyMessageRequestHeader.class);

		try {
			MessageExt msg = new MessageExt();
			msg.setTopic(requestHeader.getTopic());
			msg.setQueueId(requestHeader.getQueueId());
			msg.setStoreTimestamp(requestHeader.getStoreTimestamp());

			if (requestHeader.getBornHost() != null) {
				msg.setBornHost(NetworkUtil.string2SocketAddress(requestHeader.getBornHost()));
			}

			if (requestHeader.getStoreHost() != null) {
				msg.setStoreHost(NetworkUtil.string2SocketAddress(requestHeader.getStoreHost()));
			}

			byte[] body = request.getBody();
			int sysFlag = requestHeader.getSysFlag();
			if ((sysFlag & MessageSysFlag.COMPRESSED_FLAG) == MessageSysFlag.COMPRESSED_FLAG) {
				try {
					Compressor compressor = CompressorFactory.getCompressor(MessageSysFlag.getCompressionType(sysFlag));
					body = compressor.decompress(body);
				}
				catch (IOException e) {
					log.warn("err when uncompress constant", e);
				}
			}

			msg.setBody(body);
			msg.setFlag(requestHeader.getFlag());
			MessageAccessor.setProperties(msg, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
			MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REPLY_MESSAGE_ARRIVE_TIME, String.valueOf(receiveTime));
			msg.setBornTimestamp(requestHeader.getBornTimestamp());
			msg.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
			log.debug("receive reply message: {}", msg);

			processRelayMessage(msg);

			response.setCode(ResponseCode.SUCCESS);
			response.setRemark(null);
		}
		catch (Exception e) {
			log.warn("unknown err when receiveReplyMsg", e);
			response.setCode(ResponseCode.SYSTEM_ERROR);
			response.setRemark("process reply message fail");
		}

		return response;
	}

	private void processRelayMessage(MessageExt replyMsg) {
		String correlationId = replyMsg.getUserProperty(MessageConst.PROPERTY_CORRELATION_ID);
		RequestResponseFuture responseFuture = RequestFutureHolder.getInstance().getRequestFutureTable().get(correlationId);

		if (responseFuture != null) {
			responseFuture.putResponseMessage(replyMsg);

			RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
			if (responseFuture.getRequestCallback() != null) {
				responseFuture.getRequestCallback().onSuccess(replyMsg);
			}
		}
		else {
			log.warn("receive reply message, but not matched any request, CorrelationId: {}, reply from host: {}", correlationId, replyMsg.getBornHostString());
		}
	}
}
