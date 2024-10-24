package com.mawen.learn.rocketmq.client.impl.admin;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.mawen.learn.rocketmq.client.MQClientAdmin;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageDecoder;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.RemotingClient;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.admin.ConsumeStats;
import com.mawen.learn.rocketmq.remoting.protocol.admin.TopicStatsTable;
import com.mawen.learn.rocketmq.remoting.protocol.body.ClusterInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerConnection;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.GroupList;
import com.mawen.learn.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.QuerySubscriptionResponseBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.QueueTimeSpan;
import com.mawen.learn.rocketmq.remoting.protocol.body.ResetOffsetBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.TopicList;
import com.mawen.learn.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerStatsRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import com.mawen.learn.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@RequiredArgsConstructor
public class MqClientAdminImpl implements MQClientAdmin {

	private static final Logger log = LoggerFactory.getLogger(MqClientAdminImpl.class);

	private final RemotingClient remotingClient;

	@Override
	public CompletableFuture<List<MessageExt>> queryMessage(String address, boolean uniqueKeyFlag, boolean decompressBody, QueryMessageRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<List<MessageExt>> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
		request.addExtField(MixAll.UNIQUE_MSG_QUERY_FLAG, String.valueOf(uniqueKeyFlag));

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.getCode() == ResponseCode.SUCCESS) {
				List<MessageExt> wrappers = MessageDecoder.decodeBatch(ByteBuffer.wrap(response.getBody()), true, decompressBody, true);
				future.complete(filterMessages(wrappers, requestHeader.getTopic(), requestHeader.getKey(), uniqueKeyFlag));
			}
			else if (response.getCode() == ResponseCode.QUERY_NOT_FOUND) {
				future.complete(new ArrayList<>());
			}
			else {
				log.warn("queryMessage getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});

		return future;
	}

	@Override
	public CompletableFuture<TopicStatsTable> getTopicStatsInfo(String address, GetTopicStatsInfoRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<TopicStatsTable> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.getCode() == ResponseCode.SUCCESS) {
				TopicStatsTable result = TopicStatsTable.decode(response.getBody(), TopicStatsTable.class);
				future.complete(result);
			}
			else {
				log.warn("getTopicStatsInfo getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<List<QueueTimeSpan>> queryConsumeTimeSpan(String address, QueryConsumeTimeSpanRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<List<QueueTimeSpan>> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_TIME_SPAN, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.getCode() == ResponseCode.SUCCESS) {
				QueryConsumeTimeSpanBody body = QueryConsumeTimeSpanBody.decode(response.getBody(), QueryConsumeTimeSpanBody.class);
				future.complete(body.getConsumeTimeSpanSet());
			}
			else {
				log.warn("queryConsumeTimeSpan getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});

		return future;
	}

	@Override
	public CompletableFuture<Void> updateOrCreateTopic(String address, CreateTopicRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<Void> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.getCode() == ResponseCode.SUCCESS) {
				future.complete(null);
			}
			else {
				log.warn("updateOrCreateTopic getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<Void> updateOrCreateSubscriptionGroup(String address, SubscriptionGroupConfig config, long timeoutMillis) {
		CompletableFuture<Void> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);
		request.setBody(RemotingSerializable.encode(config));

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.getCode() == ResponseCode.SUCCESS) {
				future.complete(null);
			}
			else {
				log.warn("updateOrCreateSubscriptionGroup getResponseCommand failed, {} {}, config={}", response.getCode(), response.getRemark(), config);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<Void> deleteTopicInBroker(String address, DeleteTopicRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<Void> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.getCode() == ResponseCode.SUCCESS) {
				future.complete(null);
			}
			else {
				log.warn("deleteTopicInBroker getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<Void> deleteTopicInNameserver(String address, DeleteTopicFromNamesrvRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<Void> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.getCode() == ResponseCode.SUCCESS) {
				future.complete(null);
			}
			else {
				log.warn("deleteTopicInNameserver getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<Void> deleteKvConfig(String address, DeleteKVConfigRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<Void> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.isResponseSuccess()) {
				future.complete(null);
			}
			else {
				log.warn("deleteKvConfig getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<Void> deleteSubscriptionGroup(String address, DeleteSubscriptionGroupRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<Void> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.isResponseSuccess()) {
				future.complete(null);
			}
			else {
				log.warn("deleteSubscriptionGroup getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<Map<MessageQueue, Long>> invokeBrokerToResetOffset(String address, ResetOffsetRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<Map<MessageQueue, Long>> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.isResponseSuccess()) {
				ResetOffsetBody body = ResetOffsetBody.decode(response.getBody(), ResetOffsetBody.class);
				future.complete(body.getOffsetTable());
			}
			else {
				log.warn("invokeBrokerToResetOffset getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<MessageExt> viewMessage(String address, ViewMessageRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<MessageExt> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.isResponseSuccess()) {
				MessageExt ret = MessageDecoder.clientDecode(ByteBuffer.wrap(response.getBody()), true);
				future.complete(ret);
			}
			else {
				log.warn("viewMessage getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<ClusterInfo> getBrokerClusterInfo(String address, long timeoutMillis) {
		CompletableFuture<ClusterInfo> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.isResponseSuccess()) {
				future.complete(ClusterInfo.decode(response.getBody(), ClusterInfo.class));
			}
			else {
				log.warn("getBrokerClusterInfo getResponseCommand failed, {} {}", response.getCode(), response.getRemark());
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<ConsumerConnection> getConsumerConnectionList(String address, GetConsumerConnectionListRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<ConsumerConnection> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.isResponseSuccess()) {
				future.complete(ConsumerConnection.decode(response.getBody(), ConsumerConnection.class));
			}
			else {
				log.warn("getConsumerConnectionList getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<TopicList> queryTopicsByConsumer(String address, QueryTopicsByConsumerRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<TopicList> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPICS_BY_CONSUMER, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.isResponseSuccess()) {
				future.complete(TopicList.decode(response.getBody(), TopicList.class));
			}
			else {
				log.warn("queryTopicsByConsumer getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<SubscriptionData> querySubscriptionByConsumer(String address, QuerySubscriptionByConsumerRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<SubscriptionData> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_SUBSCRIPTION_BY_CONSUMER, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.isResponseSuccess()) {
				QuerySubscriptionResponseBody body = RemotingSerializable.decode(request.getBody(), QuerySubscriptionResponseBody.class);
				future.complete(body.getSubscriptionData());
			}
			else {
				log.warn("querySubscriptionByConsumer getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<ConsumeStats> getConsumeStats(String address, GetConsumerStatsRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<ConsumeStats> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.isResponseSuccess()) {
				future.complete(ConsumeStats.decode(response.getBody(), ConsumeStats.class));
			}
			else {
				log.warn("getConsumeStats getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<GroupList> queryTopicConsumeByWho(String address, QueryTopicConsumeByWhoRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<GroupList> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.isResponseSuccess()) {
				future.complete(RemotingSerializable.decode(response.getBody(), GroupList.class));
			}
			else {
				log.warn("queryTopicConsumeByWho getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<ConsumerRunningInfo> getConsumerRunningInfo(String address, GetConsumerRunningInfoRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<ConsumerRunningInfo> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.isResponseSuccess()) {
				future.complete(RemotingSerializable.decode(response.getBody(), ConsumerRunningInfo.class));
			}
			else {
				log.warn("getConsumerRunningInfo getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	@Override
	public CompletableFuture<ConsumeMessageDirectlyResult> consumeMessageDirectly(String address, ConsumeMessageDirectlyResultRequestHeader requestHeader, long timeoutMillis) {
		CompletableFuture<ConsumeMessageDirectlyResult> future = new CompletableFuture<>();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, requestHeader);

		remotingClient.invoke(address, request, timeoutMillis).thenAccept(response -> {
			if (response.isResponseSuccess()) {
				future.complete(RemotingSerializable.decode(response.getBody(), ConsumeMessageDirectlyResult.class));
			}
			else {
				log.warn("consumeMessageDirectly getResponseCommand failed, {} {}, header={}", response.getCode(), response.getRemark(), requestHeader);
				future.completeExceptionally(new MQClientException(response));
			}
		});
		return future;
	}

	private List<MessageExt> filterMessages(List<MessageExt> messages, String topic, String key, boolean uniqueKeyFlag) {
		List<MessageExt> matches = new ArrayList<>();
		if (uniqueKeyFlag) {
			matches.addAll(messages.stream()
					.filter(msg -> topic.equals(msg.getTopic()))
					.filter(msg -> key.equals(msg.getMsgId()))
					.collect(Collectors.toList()));
		}
		else {
			matches.addAll(messages.stream()
					.filter(msg -> topic.equals(msg.getTopic()))
					.filter(msg -> StringUtils.isNotBlank(msg.getKeys()))
					.filter(msg -> Arrays.asList(msg.getKeys().split(MessageConst.KEY_SEPARATOR)).contains(key))
					.collect(Collectors.toList()));
		}

		return matches;
	}
}
