package com.mawen.learn.rocketmq.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.admin.ConsumeStats;
import com.mawen.learn.rocketmq.remoting.protocol.admin.TopicStatsTable;
import com.mawen.learn.rocketmq.remoting.protocol.body.ClusterInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerConnection;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.GroupList;
import com.mawen.learn.rocketmq.remoting.protocol.body.QueueTimeSpan;
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

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public interface MQClientAdmin {

	CompletableFuture<List<MessageExt>> queryMessage(String address, boolean uniqueKeyFlag, boolean decompressBody, QueryMessageRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<TopicStatsTable> getTopicStatsInfo(String address, GetTopicStatsInfoRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<List<QueueTimeSpan>> queryConsumeTimeSpan(String address, QueryConsumeTimeSpanRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<Void> updateOrCreateTopic(String address, CreateTopicRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<Void> updateOrCreateSubscriptionGroup(String address, SubscriptionGroupConfig config, long timeoutMillis);

	CompletableFuture<Void> deleteTopicInBroker(String address, DeleteTopicRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<Void> deleteTopicInNameserver(String address, DeleteTopicFromNamesrvRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<Void> deleteKvConfig(String address, DeleteKVConfigRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<Void> deleteSubscriptionGroup(String address, DeleteSubscriptionGroupRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<Map<MessageQueue, Long>> invokeBrokerToResetOffset(String address, ResetOffsetRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<MessageExt> viewMessage(String address, ViewMessageRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<ClusterInfo> getBrokerClusterInfo(String address, long timeoutMillis);

	CompletableFuture<ConsumerConnection> getConsumerConnectionList(String address, GetConsumerConnectionListRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<TopicList> queryTopicsByConsumer(String address, QueryTopicsByConsumerRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<SubscriptionData> querySubscriptionByConsumer(String address, QuerySubscriptionByConsumerRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<ConsumeStats> getConsumeStats(String address, GetConsumerStatsRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<GroupList> queryTopicConsumeByWho(String address, QueryTopicConsumeByWhoRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<ConsumerRunningInfo> getConsumerRunningInfo(String address, GetConsumerRunningInfoRequestHeader requestHeader, long timeoutMillis);

	CompletableFuture<ConsumeMessageDirectlyResult> consumeMessageDirectly(String address, ConsumeMessageDirectlyResultRequestHeader requestHeader, long timeoutMillis);










}
