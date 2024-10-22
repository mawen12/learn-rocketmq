package com.mawen.learn.rocketmq.client.impl;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.fastjson.JSON;
import com.mawen.learn.rocketmq.client.ClientConfig;
import com.mawen.learn.rocketmq.client.Validators;
import com.mawen.learn.rocketmq.client.consumer.PopCallback;
import com.mawen.learn.rocketmq.client.consumer.PopResult;
import com.mawen.learn.rocketmq.client.consumer.PopStatus;
import com.mawen.learn.rocketmq.client.consumer.PullCallback;
import com.mawen.learn.rocketmq.client.consumer.PullResult;
import com.mawen.learn.rocketmq.client.consumer.PullStatus;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.exception.OffsetNotFoundException;
import com.mawen.learn.rocketmq.client.hook.SendMessageContext;
import com.mawen.learn.rocketmq.client.impl.consumer.PullResultExt;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.mawen.learn.rocketmq.client.impl.producer.TopicPublishInfo;
import com.mawen.learn.rocketmq.client.producer.SendCallback;
import com.mawen.learn.rocketmq.client.producer.SendResult;
import com.mawen.learn.rocketmq.client.producer.SendStatus;
import com.mawen.learn.rocketmq.client.rpchook.NamespaceRpcHook;
import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.MQVersion;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.PlainAccessConfig;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.attribute.AttributeParser;
import com.mawen.learn.rocketmq.common.constant.FileReadAheadMode;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageBatch;
import com.mawen.learn.rocketmq.common.message.MessageClientIDSetter;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageDecoder;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.message.MessageQueueAssignment;
import com.mawen.learn.rocketmq.common.message.MessageRequestMode;
import com.mawen.learn.rocketmq.common.namesrv.DefaultTopAddressing;
import com.mawen.learn.rocketmq.common.namesrv.NameServerUpdateCallback;
import com.mawen.learn.rocketmq.common.namesrv.TopAddressing;
import com.mawen.learn.rocketmq.common.queue.RoundQueue;
import com.mawen.learn.rocketmq.common.sysflag.PullSysFlag;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.remoting.ChannelEventListener;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.InvokeCallback;
import com.mawen.learn.rocketmq.remoting.RPCHook;
import com.mawen.learn.rocketmq.remoting.RemotingClient;
import com.mawen.learn.rocketmq.remoting.common.HeartbeatV2Result;
import com.mawen.learn.rocketmq.remoting.common.RemotingHelper;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingConnectException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.mawen.learn.rocketmq.remoting.netty.NettyClientConfig;
import com.mawen.learn.rocketmq.remoting.netty.NettyRemotingClient;
import com.mawen.learn.rocketmq.remoting.netty.ResponseFuture;
import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;
import com.mawen.learn.rocketmq.remoting.protocol.EpochEntry;
import com.mawen.learn.rocketmq.remoting.protocol.LanguageCode;
import com.mawen.learn.rocketmq.remoting.protocol.NamespaceUtil;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.admin.ConsumeStats;
import com.mawen.learn.rocketmq.remoting.protocol.admin.TopicStatsTable;
import com.mawen.learn.rocketmq.remoting.protocol.body.AclInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import com.mawen.learn.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.BrokerStatsData;
import com.mawen.learn.rocketmq.remoting.protocol.body.CheckClientRequestBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.ClusterAclVersionInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.ClusterInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeStatsList;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerConnection;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.EpochEntryCache;
import com.mawen.learn.rocketmq.remoting.protocol.body.GetConsumerStatusBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.GroupList;
import com.mawen.learn.rocketmq.remoting.protocol.body.HARuntimeInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.KVTable;
import com.mawen.learn.rocketmq.remoting.protocol.body.LockBatchRequestBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.LockBatchResponseBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.ProducerConnection;
import com.mawen.learn.rocketmq.remoting.protocol.body.ProducerTableInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.QueryAssignmentRequestBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.QueryAssignmentResponseBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.QueryCorrectionOffsetBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.QuerySubscriptionResponseBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.QueueTimeSpan;
import com.mawen.learn.rocketmq.remoting.protocol.body.ResetOffsetBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.SetMessageRequestModeRequestBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import com.mawen.learn.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import com.mawen.learn.rocketmq.remoting.protocol.body.TopicList;
import com.mawen.learn.rocketmq.remoting.protocol.body.UnlockBatchRequestBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.UserInfo;
import com.mawen.learn.rocketmq.remoting.protocol.header.AddBrokerRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.CloneGroupOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ConsumerSendMsgBackRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.CreateAccessConfigRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.CreateAclRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.CreateUserRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.DeleteAccessConfigRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.DeleteAclRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.DeleteSubscriptionGroupRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.DeleteUserRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.EndTransactionRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ExtraInfoUtil;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetAclRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetAllProducerInfoRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetBrokerAclConfigResponseBroker;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerConnectionListRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerListByGroupRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerListByGroupResponseBody;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerRunningInfoRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerStatsInBrokerHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerStatsRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetConsumerStatusRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetEarliestStoretimeResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetMaxOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetMinOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetProducerConnectionListRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetSubscriptionGroupConfigRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetTopicConfigRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetTopicStatsInfoRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.GetUserRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.HeartbeatRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ListAclsRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ListUsersRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.LockBatchMqRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.PopMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.PopMessageResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.PullMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.PullMessageResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryConsumeQueueRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryConsumerOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryCorrectionOffsetHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QuerySubscriptionByConsumerRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.QueryTopicsByConsumerRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.RemoveBrokerRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ResetMasterFlushOffsetHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ResetOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ResumeCheckHalfMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.SearchOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.SendMessageRequestHeaderV2;
import com.mawen.learn.rocketmq.remoting.protocol.header.SendMessageResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.UnlockBatchMqRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.UnregisterClientRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.UpdateAclRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.UpdateGlobalWhiteAddrConfigRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.UpdateGroupForbiddenRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.UpdateUserRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ViewBrokerStatsDataRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.ViewMessageRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.GetMetadataResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.AddWritePermOfBrokerResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.GetKVConfigRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.GetKVConfigResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.GetKVListByNamespaceRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.PutKVConfigRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.namesrv.WipeWriterPermOfBrokerRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.HeartbeatData;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.MessageModel;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import com.mawen.learn.rocketmq.remoting.protocol.route.TopicRouteData;
import com.mawen.learn.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import com.mawen.learn.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import com.mawen.learn.rocketmq.remoting.protocol.subscription.GroupForbidden;
import com.mawen.learn.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import com.mawen.learn.rocketmq.remoting.rpchook.DynamicalExtFieldRPCHook;
import com.mawen.learn.rocketmq.remoting.rpchook.StreamTypeRPCHook;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/21
 */
public class MQClientAPIImpl implements NameServerUpdateCallback {

	private static final Logger log = LoggerFactory.getLogger(MQClientAPIImpl.class);

	private static boolean sendSmartMsg = Boolean.parseBoolean(System.getProperty("com.mawen.learn.rocketmq.client.sendSmartMsg", "true"));

	static {
		System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
	}

	@Getter
	private final RemotingClient remotingClient;
	private final TopAddressing topAddressing;
	private final ClientRemotingProcessor clientRemotingProcessor;
	private String namesrvAddr;
	private ClientConfig clientConfig;

	public MQClientAPIImpl(final NettyClientConfig nettyClientConfig, final ClientRemotingProcessor clientRemotingProcessor, RPCHook rpcHook, final ClientConfig clientConfig) {
		this(nettyClientConfig, clientRemotingProcessor, rpcHook, clientConfig, null);
	}

	public MQClientAPIImpl(final NettyClientConfig nettyClientConfig, final ClientRemotingProcessor clientRemotingProcessor, RPCHook rpcHook, final ClientConfig clientConfig, final ChannelEventListener channelEventListener) {
		this.clientConfig = clientConfig;
		this.topAddressing = new DefaultTopAddressing(MixAll.getWSAddr(), clientConfig.getUnitName());
		this.topAddressing.registerChangeCallback(this);
		this.remotingClient = new NettyRemotingClient(nettyClientConfig, channelEventListener);
		this.clientRemotingProcessor = clientRemotingProcessor;
		this.remotingClient.registerRPCHook(new NamespaceRpcHook(clientConfig));

		if (clientConfig.isEnableStreamRequestType()) {
			this.remotingClient.registerRPCHook(new StreamTypeRPCHook());
		}

		this.remotingClient.registerRPCHook(rpcHook);
		this.remotingClient.registerRPCHook(new DynamicalExtFieldRPCHook());
		this.remotingClient.registerProcessor(RequestCode.CHECK_TRANSACTION_STATE, this.clientRemotingProcessor, null);
		this.remotingClient.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, this.clientRemotingProcessor, null);
		this.remotingClient.registerProcessor(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, this.clientRemotingProcessor, null);
		this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, this.clientRemotingProcessor, null);
		this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientRemotingProcessor, null);
		this.remotingClient.registerProcessor(RequestCode.CONSUME_MESSAGE_DIRECTLY, this.clientRemotingProcessor, null);
		this.remotingClient.registerProcessor(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, this.clientRemotingProcessor, null);
	}

	public List<String> getNameServerAddressList() {
		return this.remotingClient.getAvailableNameServerList();
	}

	public String fetchNameServerAddr() {
		try {
			String addrs = this.topAddressing.fetchNSAddr();
			if (!UtilAll.isBlank(addrs)) {
				if (!addrs.equals(this.namesrvAddr)) {
					log.info("name server address changed, old={}, new={}", this.namesrvAddr, addrs);
					this.updateNameServerAddressList(addrs);
					this.namesrvAddr = addrs;
					return namesrvAddr;
				}
			}
		}
		catch (Exception e) {
			log.error("fetchNameServerAddr exception", e);
		}
		return namesrvAddr;
	}

	@Override
	public String onNameServerAddressChange(String namesrvAddress) {
		if (namesrvAddress != null) {
			if (!namesrvAddress.equals(this.namesrvAddr)) {
				log.info("name server address changed, old={}, new={}", this.namesrvAddr, namesrvAddress);
				this.updateNameServerAddressList(namesrvAddress);
				this.namesrvAddr = namesrvAddress;
				return namesrvAddr;
			}
		}
		return namesrvAddr;
	}

	public void updateNameServerAddressList(final String addrs) {
		String[] addrArray = addrs.split(",");
		List<String> list = Arrays.asList(addrArray);
		this.remotingClient.updateNameServerAddressList(list);
	}

	public void start() {
		this.remotingClient.start();
	}

	public void shutdown() {
		this.remotingClient.shutdown();
	}

	public Set<MessageQueueAssignment> queryAssignment(final String addr, final String topic, final String consumerGroup, final String clientId, final String strategyName, final MessageModel messageModel, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		QueryAssignmentRequestBody body = new QueryAssignmentRequestBody();
		body.setTopic(topic);
		body.setConsumerGroup(consumerGroup);
		body.setClientId(clientId);
		body.setMessageModel(messageModel);
		body.setStrategyName(strategyName);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_ASSIGNMENT, null);
		request.setBody(body.encode());

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		switch (request.getCode()) {
			case ResponseCode.SUCCESS: {
				QueryAssignmentResponseBody responseBody = QueryAssignmentResponseBody.decode(response.getBody(), QueryAssignmentResponseBody.class);
				return responseBody.getMessageQueueAssignments();
			}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public void createSubscriptionGroup(final String addr, final SubscriptionGroupConfig config, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);

		byte[] body = RemotingSerializable.encode(config);
		request.setBody(body);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}
		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public void createTopic(final String addr, final String defaultTopic, final TopicConfig topicConfig, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		Validators.checkTopicConfig(topicConfig);

		CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
		requestHeader.setTopic(topicConfig.getTopicName());
		requestHeader.setDefaultTopic(defaultTopic);
		requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
		requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
		requestHeader.setPerm(topicConfig.getPerm());
		requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
		requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
		requestHeader.setOrder(topicConfig.isOrder());
		requestHeader.setAttributes(AttributeParser.parseToString(topicConfig.getAttributes()));

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public void createPlainAccessConfig(final String addr, final PlainAccessConfig plainAccessConfig, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		CreateAccessConfigRequestHeader requestHeader = new CreateAccessConfigRequestHeader();
		requestHeader.setAccessKey(plainAccessConfig.getAccessKey());
		requestHeader.setSecretKey(plainAccessConfig.getSecretKey());
		requestHeader.setAdmin(plainAccessConfig.isAdmin());
		requestHeader.setDefaultGroupPerm(plainAccessConfig.getDefaultGroupPerm());
		requestHeader.setDefaultTopicPerm(plainAccessConfig.getDefaultTopicPerm());
		requestHeader.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());
		requestHeader.setTopicPerms(UtilAll.join(plainAccessConfig.getTopicPerms(), ","));
		requestHeader.setGroupPerms(UtilAll.join(plainAccessConfig.getGroupPerms(), ","));

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_ACL_CONFIG, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public void deleteAccessConfig(final String addr, final String accessKey, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		DeleteAccessConfigRequestHeader requestHeader = new DeleteAccessConfigRequestHeader();
		requestHeader.setAccessKey(accessKey);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_ACL_CONFIG, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public void updateGlobalWhiteAddrsConfig(final String addr, final String globalWhiteAddrs, final String aclFileFullPath, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		UpdateGlobalWhiteAddrConfigRequestHeader requestHeader = new UpdateGlobalWhiteAddrConfigRequestHeader();
		requestHeader.setGlobalWhiteAddrs(globalWhiteAddrs);
		requestHeader.setAclFileFullPath(aclFileFullPath);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public ClusterAclVersionInfo getBrokerClusterAclInfo(final String addr, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException, RemotingCommandException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_ACL_INFO, null);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				GetBrokerAclConfigResponseBroker responseHeader = response.decodeCommandCustomHeader(GetBrokerAclConfigResponseBroker.class);

				ClusterAclVersionInfo versionInfo = new ClusterAclVersionInfo();
				versionInfo.setClusterName(responseHeader.getClusterName());
				versionInfo.setBrokerName(responseHeader.getBrokerName());
				versionInfo.setBrokerAddr(responseHeader.getBrokerAddr());
				versionInfo.setAclConfigDataVersion(DataVersion.fromJson(responseHeader.getVersion(), DataVersion.class));

				Map<String, Object> dataVersionMap = JSON.parseObject(responseHeader.getAllAclFileVersion(), HashMap.class);
				Map<String, DataVersion> allAclConfigDataVersion = new HashMap<>(dataVersionMap.size(), 1);
				for (Map.Entry<String, Object> entry : dataVersionMap.entrySet()) {
					allAclConfigDataVersion.put(entry.getKey(), DataVersion.fromJson(JSON.toJSONString(entry.getValue()), DataVersion.class));
				}
				versionInfo.setAllAclConfigDataVersion(allAclConfigDataVersion);
				return versionInfo;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public SendResult sendMessage(final String addr, final String brokerName, final Message msg, final SendMessageRequestHeader requestHeader, final long timeoutMillis, final CommunicationMode communicationMode, final SendMessageContext context, final DefaultMQProducerImpl producer) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException, RemotingCommandException, MQBrokerException {
		return sendMessage(addr, brokerName, msg, requestHeader, timeoutMillis, communicationMode, null, null, null, 0, context, producer);
	}

	public SendResult sendMessage(final String addr, final String brokerName, final Message msg, final SendMessageRequestHeader requestHeader, final long timeoutMillis,
			final CommunicationMode communicationMode, final SendCallback sendCallback, final TopicPublishInfo topicPublishInfo, final MQClientInstance instance,
			final int retryTimesWhenSendFailed, final SendMessageContext context, final DefaultMQProducerImpl producer) throws RemotingTooMuchRequestException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException, MQBrokerException {
		long beginStartTime = System.currentTimeMillis();

		RemotingCommand request = null;
		String msgType = msg.getProperty(MessageConst.PROPERTY_MESSAGE_TYPE);
		boolean isReply = msgType != null && msgType.equals(MixAll.REPLY_MESSAGE_FLAG);
		if (isReply) {
			if (sendSmartMsg) {
				SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
				request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE_V2, requestHeaderV2);
			}
			else {
				request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE, requestHeader);
			}
		}
		else {
			if (sendSmartMsg || msg instanceof MessageBatch) {
				SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
				request = RemotingCommand.createRequestCommand(msg instanceof Message ? RequestCode.SEND_BATCH_MESSAGE : RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
			}
			else {
				request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
			}
		}
		request.setBody(msg.getBody());

		switch (communicationMode) {
			case ONEWAY:
				this.remotingClient.invokeOneway(addr, request, timeoutMillis);
				return null;
			case ASYNC:
				final AtomicInteger times = new AtomicInteger();
				long costTimeAsync = System.currentTimeMillis() - beginStartTime;
				if (timeoutMillis < costTimeAsync) {
					throw new RemotingTooMuchRequestException("sendMessage call timeout");
				}
				this.sendMessageAsync(addr, brokerName, msg, timeoutMillis - costTimeAsync, request, sendCallback, topicPublishInfo, instance, retryTimesWhenSendFailed, times, context, producer);
				return null;
			case SYNC:
				long costTimeSync = System.currentTimeMillis() - beginStartTime;
				if (timeoutMillis < costTimeSync) {
					throw new RemotingTooMuchRequestException("sendMessage call timeout");
				}
				return this.sendMessageSync(addr, brokerName, msg, timeoutMillis - costTimeSync, request);
			default:
				assert false;
				break;
		}

		return null;
	}

	private SendResult sendMessageSync(final String addr, final String brokerName, final Message msg, final long timeoutMillis, final RemotingCommand request) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException, MQBrokerException {
		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
		assert response != null;
		return this.processSendResponse(brokerName, msg, response, addr);
	}

	void execRpcHooksAfterRequest(ResponseFuture responseFuture) {
		if (this.remotingClient instanceof NettyRemotingClient) {
			NettyRemotingClient remotingClient = (NettyRemotingClient) this.remotingClient;
			RemotingCommand response = responseFuture.getResponseCommand();
			remotingClient.doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(responseFuture.getChannel()), responseFuture.getRequestCommand(), response);
		}
	}

	private void sendMessageAsync(final String addr, final String brokerName, final Message msg, final long timeoutMillis, final RemotingCommand request, final SendCallback sendCallback, final TopicPublishInfo topicPublishInfo, final MQClientInstance instance, final int retryTimesWhenSendFailed, final AtomicInteger times, final SendMessageContext context, final DefaultMQProducerImpl producer) {
		final long beginStartTime = System.currentTimeMillis();

		try {
			this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
				@Override
				public void operationComplete(ResponseFuture responseFuture) {

				}

				@Override
				public void operationSucceed(RemotingCommand response) {
					long cost = System.currentTimeMillis() - beginStartTime;
					if (sendCallback == null) {
						try {
							SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response, addr);
							if (context != null && sendResult != null) {
								context.setSendResult(sendResult);
								context.getProducer().executeSendMessageHookAfter(context);
							}
						}
						catch (Throwable ignored) {}

						producer.updateFaultItem(brokerName, System.currentTimeMillis() - beginStartTime, false, true);
						return;
					}

					try {
						SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response, addr);
						assert sendResult != null;
						if (context != null) {
							context.setSendResult(sendResult);
							context.getProducer().executeSendMessageHookAfter(context);
						}

						try {
							sendCallback.onSuccess(sendResult);
						}
						catch (Throwable ignored) {}

						producer.updateFaultItem(brokerName, System.currentTimeMillis() - beginStartTime, false, true);
					}
					catch (Exception e) {
						producer.updateFaultItem(beginStartTime, System.currentTimeMillis() - beginStartTime, true, false);
						onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance, retryTimesWhenSendFailed, times, e, context, false, producer);
					}
				}

				@Override
				public void operationFail(Throwable throwable) {
					producer.updateFaultItem(brokerName, System.currentTimeMillis() - beginStartTime, true, true);
					long cost = System.currentTimeMillis() - beginStartTime;
					MQClientException ex;
					if (throwable instanceof RemotingSendRequestException) {
						ex = new MQClientException("send request failed", throwable);
					}
					else if (throwable instanceof RemotingTimeoutException) {
						ex = new MQClientException("wait response timeout, cost=" + cost, throwable);
					}
					else {
						ex = new MQClientException("unknown reason", throwable);
					}
					onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance, retryTimesWhenSendFailed, times, ex, context, true, producer);
				}
			});
		}
		catch (Exception e) {
			long cost = System.currentTimeMillis() - beginStartTime;
			producer.updateFaultItem(brokerName, cost, true, false);
			onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance, retryTimesWhenSendFailed, timeoutMillis, e, context, true, producer);
		}
	}

	private void onExceptionImpl(final String brokerName, final Message msg, final long timeoutMillis,
			final RemotingCommand request, final SendCallback sendCallback, final TopicPublishInfo topicPublishInfo,
			final MQClientInstance instance, final int timesTotal, final AtomicInteger curTimes, final Exception e,
			final SendMessageContext context, final boolean needRetry, final DefaultMQProducerImpl producer) {
		int tmp = curTimes.incrementAndGet();
		if (needRetry && tmp <= timesTotal) {
			String retryBrokerName = brokerName;
			if (topicPublishInfo != null) {
				MessageQueue mqChosen = producer.selectOneMessageQueue(topicPublishInfo, brokerName, false);
				retryBrokerName = instance.getBrokerNameFromMessageQueue(mqChosen);
			}
			String addr = instance.findBrokerAddressInPublish(retryBrokerName);
			log.warn("async send msg by retry {} times. topic={}, brokerAddr={}, brokerName={}", tmp, msg.getTopic(), addr, retryBrokerName, e);
			request.setOpaque(RemotingCommand.createNewRequestId());
			sendMessageAsync(addr, retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, context, producer);
		}
		else {
			if (context != null) {
				context.setException(e);
				context.getProducer().executeSendMessageHookAfter(context);
			}

			try {
				sendCallback.onException(e);
			}
			catch (Exception ignored) {}
		}
	}

	protected SendResult processSendResponse(final String brokerName, final Message msg, final RemotingCommand response, final String addr) throws MQBrokerException, RemotingCommandException {
		SendStatus sendStatus;
		switch (response.getCode()) {
			case ResponseCode.FLUSH_DISK_TIMEOUT: {
				sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
				break;
			}
			case ResponseCode.FLUSH_SLAVE_TIMEOUT: {
				sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
				break;
			}
			case ResponseCode.SLAVE_NOT_AVAILABLE: {
				sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
				break;
			}
			case ResponseCode.SUCCESS: {
				sendStatus = SendStatus.SEND_OK;
				break;
			}
			default:
				throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
		}

		SendMessageResponseHeader responseHeader = response.decodeCommandCustomHeader(SendMessageResponseHeader.class);

		String topic = msg.getTopic();
		if (StringUtils.isNotEmpty(this.clientConfig.getNamespace())) {
			topic = NamespaceUtil.withoutNamespace(topic, this.clientConfig.getNamespace());
		}

		MessageQueue messageQueue = new MessageQueue(topic, brokerName, responseHeader.getQueueId());

		String uniqMsgId = MessageClientIDSetter.getUniqID(msg);
		if (msg instanceof MessageBatch && responseHeader.getBatchUniqId() != null) {
			StringBuilder sb = new StringBuilder();
			for (Message message : (MessageBatch) msg) {
				sb.append(sb.length() == 0 ? "" : ",")
						.append(MessageClientIDSetter.getUniqID(message));
			}
			uniqMsgId = sb.toString();
		}

		SendResult sendResult = new SendResult(sendStatus, uniqMsgId, responseHeader.getMsgId(), messageQueue, responseHeader.getQueueOffset());
		sendResult.setTransactionId(responseHeader.getTransactionId());

		String regionId = response.getExtFields().get(MessageConst.PROPERTY_MSG_REGION);
		if (regionId == null || regionId.isEmpty()) {
			regionId = MixAll.DEFAULT_TRACE_REGION_ID;
		}
		sendResult.setRegionId(regionId);

		String traceOn = response.getExtFields().get(MessageConst.PROPERTY_TRACE_SWITCH);
		sendResult.setTraceOn(!Boolean.FALSE.toString().equals(traceOn));

		return sendResult;
	}

	public PullResult pullMessage(final String addr, final PullMessageRequestHeader requestHeader, final long timeoutMillis, final CommunicationMode communicationMode, final PullCallback pullCallback) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException, RemotingTooMuchRequestException {
		RemotingCommand request;
		if (PullSysFlag.hasLitePullFlag(requestHeader.getSysFlag())) {
			request = RemotingCommand.createRequestCommand(RequestCode.LITE_PULL_MESSAGE, requestHeader);
		}
		else {
			request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);
		}

		switch (communicationMode) {
			case ONEWAY:
				assert false;
				return null;
			case ASYNC:
				this.pullMessageAsync(addr, request, timeoutMillis, pullCallback);
				return null;
			case SYNC:
				return this.pullMessageSync(addr, request, timeoutMillis);
			default:
				assert false;
				break;
		}

		return null;
	}

	private void pullMessageAsync(final String addr, final RemotingCommand request, final long timeoutMillis, final PullCallback pullCallback) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException {
		this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
			@Override
			public void operationComplete(ResponseFuture responseFuture) {
			}

			@Override
			public void operationSucceed(RemotingCommand response) {
				try {
					PullResult pullResult = MQClientAPIImpl.this.processPullResponse(request, addr);
					pullCallback.onSuccess(pullResult);
				}
				catch (Exception e) {
					pullCallback.onException(e);
				}
			}

			@Override
			public void operationFail(Throwable throwable) {
				pullCallback.onException(throwable);
			}
		});
	}

	private PullResult pullMessageSync(final String addr, final RemotingCommand request, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException, MQBrokerException {
		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
		assert response != null;
		return this.processPullResponse(response, addr);
	}

	private PullResult processPullResponse(final RemotingCommand response, final String addr) throws MQBrokerException, RemotingCommandException {
		PullStatus pullStatus = PullStatus.NO_NEW_MSG;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				pullStatus = PullStatus.FOUND;
				break;
			case ResponseCode.PULL_NOT_FOUND:
				pullStatus = PullStatus.NO_NEW_MSG;
				break;
			case ResponseCode.PULL_RETRY_IMMEDIATELY:
				pullStatus = PullStatus.NO_MATCHED_MSG;
				break;
			case ResponseCode.PULL_OFFSET_MOVED:
				pullStatus = PullStatus.OFFSET_ILLEGAL;
				break;

			default:
				throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
		}

		PullMessageResponseHeader responseHeader = response.decodeCommandCustomHeader(PullMessageResponseHeader.class);

		return new PullResultExt(pullStatus, responseHeader.getNextBeginOffset(), responseHeader.getMinOffset(), responseHeader.getMaxOffset(), null, responseHeader.getSuggestWhichBrokerId(), response.getBody(), responseHeader.getOffsetDelta());
	}

	public void popMessageExt(final String brokerName, final String addr, final PopMessageRequestHeader requestHeader, final long timeoutMillis, final PopCallback popCallback) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.POP_MESSAGE, requestHeader);

		this.remotingClient.invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
			@Override
			public void operationComplete(ResponseFuture responseFuture) {

			}

			@Override
			public void operationSucceed(RemotingCommand response) {
				try {
					PopResult popResult = MQClientAPIImpl.this.processPopMessage(brokerName, response, requestHeader.getTopic(), requestHeader);
					popCallback.onSuccess(popResult);
				}
				catch (Exception e) {
					popCallback.onException(e);
				}
			}

			@Override
			public void operationFail(Throwable throwable) {
				popCallback.onException(throwable);
			}
		});
	}

	private PopResult processPopMessage(final String brokerName, final RemotingCommand response, String topic, CommandCustomHeader requestHeader) throws MQBrokerException, RemotingCommandException {
		PopStatus popStatus = PopStatus.NO_NEW_MSG;
		List<MessageExt> msgFoundList = null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				popStatus = PopStatus.FOUND;
				ByteBuffer buffer = ByteBuffer.wrap(response.getBody());
				msgFoundList = MessageDecoder.decodeBatch(buffer, clientConfig.isDecodeReadBody(), clientConfig.isDecodeDecompressBody(), true);
				break;
			case ResponseCode.POLLING_FULL:
				popStatus = PopStatus.POLLING_FULL;
				break;
			case ResponseCode.POLLING_TIMEOUT:
				popStatus = PopStatus.POLLING_NOT_FOUND;
				break;
			case ResponseCode.PULL_NOT_FOUND:
				popStatus = PopStatus.POLLING_NOT_FOUND;
				break;
			default:
				throw new MQBrokerException(response.getCode(), response.getRemark());
		}

		PopResult popResult = new PopResult(popStatus, msgFoundList);
		PopMessageResponseHeader responseHeader = response.decodeCommandCustomHeader(PopMessageResponseHeader.class);
		popResult.setRestNum(responseHeader.getRestNum());

		if (popStatus != PopStatus.FOUND) {
			return popResult;
		}

		Map<String, Long> startOffsetInfo = null;
		Map<String, List<Long>> msgOffsetInfo = null;
		Map<String, Integer> orderCountInfo = null;

		if (requestHeader instanceof PopMessageRequestHeader) {
			popResult.setInvisibleTime(responseHeader.getInvisibleTime());
			popResult.setPopTime(responseHeader.getPopTime());
			startOffsetInfo = ExtraInfoUtil.parseStartOffsetInfo(responseHeader.getStartOffsetInfo());
			msgOffsetInfo = ExtraInfoUtil.parseMsgOffsetInfo(responseHeader.getMsgOffsetInfo());
			orderCountInfo = ExtraInfoUtil.parseOrderCountInfo(responseHeader.getOrderCountInfo());
		}

		Map<String, List<Long>> sortMap = buildQueueOffsetSortMap(topic, msgFoundList);
		Map<String, String> map = new HashMap<>(5);

		for (MessageExt messageExt : msgFoundList) {
			if (requestHeader instanceof PopMessageRequestHeader) {
				if (startOffsetInfo == null) {
					String key = messageExt.getTopic() + messageExt.getQueueId();
					if (!map.containsKey(key)) {
						map.put(key, ExtraInfoUtil.buildExtraInfo(messageExt.getQueueOffset(), responseHeader.getPopTime(), responseHeader.getInvisibleTime(), responseHeader.getReviveQid(), messageExt.getTopic(), brokerName, messageExt.getQueueId()));
					}
					messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, map.get(key) + MessageConst.KEY_SEPARATOR + messageExt.getQueueOffset());
				}
				else {
					if (messageExt.getProperty(MessageConst.PROPERTY_POP_CK) == null) {
						final String queueIdKey;
						final String queueOffsetKey;
						final int index;
						final Long msgQueueOffset;

						if (MixAll.isLmq(topic) && messageExt.getReconsumeTimes() == 0 && StringUtils.isNotEmpty(messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))) {
							messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);

							String[] queues = messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH).split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
							String[] queueOffsets = messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET).split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);

							long offset = Long.parseLong(queueOffsets[ArrayUtils.indexOf(queues, topic)]);

							queueIdKey = ExtraInfoUtil.getStartOffsetInfoMapKey(topic, MixAll.LMQ_QUEUE_ID);
							queueOffsetKey = ExtraInfoUtil.getQueueOffsetMapKey(topic, MixAll.LMQ_QUEUE_ID, offset);
							index = sortMap.get(queueIdKey).indexOf(offset);
							msgQueueOffset = msgOffsetInfo.get(queueIdKey).get(index);

							if (msgQueueOffset != offset) {
								log.warn("Queue offset[{}] of msg is strange, not equal to ge stored in msg, {}", msgQueueOffset, messageExt);
							}
							messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, ExtraInfoUtil.buildExtraInfo(startOffsetInfo.get(queueIdKey), responseHeader.getPopTime(), responseHeader.getInvisibleTime(), responseHeader.getReviveQid(), topic, brokerName, 0, msgQueueOffset));
						}
						else {
							queueIdKey = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(), messageExt.getQueueId());
							queueOffsetKey = ExtraInfoUtil.getQueueOffsetMapKey(messageExt.getTopic(), messageExt.getQueueId(), messageExt.getQueueOffset());
							index = sortMap.get(queueIdKey).indexOf(messageExt.getQueueOffset());
							msgQueueOffset = msgOffsetInfo.get(queueIdKey).get(index);

							if (msgQueueOffset != messageExt.getQueueOffset()) {
								log.warn("Queue offset[{}] of msg is strange, not equal to the stored in msg, {}", msgQueueOffset, messageExt);
							}
							messageExt.getProperties().put(MessageConst.PROPERTY_POP_CK, ExtraInfoUtil.buildExtraInfo(startOffsetInfo.get(queueIdKey), responseHeader.getPopTime(), responseHeader.getInvisibleTime(), responseHeader.getReviveQid(), messageExt.getTopic(), brokerName, messageExt.getQueueId(), msgQueueOffset));
						}

						if (((PopMessageRequestHeader) requestHeader).isOrder() && orderCountInfo != null) {
							Integer count = orderCountInfo.get(queueOffsetKey);
							if (count == null) {
								count = orderCountInfo.get(queueIdKey);
							}
							if (count != null && count > 0) {
								messageExt.setReconsumeTimes(count);
							}
						}
					}
				}
				messageExt.getProperties().computeIfAbsent(MessageConst.PROPERTY_FIRST_POP_TIME, k -> String.valueOf(responseHeader.getPopTime()));
			}
			messageExt.setBrokerName(brokerName);
			messageExt.setTopic(NamespaceUtil.withoutNamespace(topic, this.clientConfig.getNamespace()));
		}
		return popResult;
	}

	private static Map<String, List<Long>> buildQueueOffsetSortMap(String topic, List<MessageExt> msgFoundList) {
		Map<String, List<Long>> sortMap = new HashMap<>(16);
		for (MessageExt messageExt : msgFoundList) {
			final String key;
			if (MixAll.isLmq(topic) && messageExt.getReconsumeTimes() == 0 && StringUtils.isNotEmpty(messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))) {
				String[] queues = messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH).split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
				String[] queueOffsets = messageExt.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET).split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
				key = ExtraInfoUtil.getStartOffsetInfoMapKey(topic, MixAll.LMQ_QUEUE_ID);
				sortMap.putIfAbsent(key, new ArrayList<>(4));
				sortMap.get(key).add(Long.parseLong(queueOffsets[ArrayUtils.indexOf(queues, topic)]));
				continue;
			}

			key = ExtraInfoUtil.getStartOffsetInfoMapKey(messageExt.getTopic(), messageExt.getProperty(MessageConst.PROPERTY_POP_CK), messageExt.getQueueId());
			if (!sortMap.containsKey(key)) {
				sortMap.put(key, new ArrayList<>(4));
			}
			sortMap.get(key).add(messageExt.getQueueOffset());
		}
		return sortMap;
	}

	public MessageExt viewMessage(final String addr, final String topic, final long phyoffset, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		ViewMessageRequestHeader requestHeader = new ViewMessageRequestHeader();
		requestHeader.setTopic(topic);
		requestHeader.setOffset(phyoffset);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				ByteBuffer buffer = ByteBuffer.wrap(response.getBody());
				MessageExt messageExt = MessageDecoder.clientDecode(buffer, true);
				if (StringUtils.isNotEmpty(this.clientConfig.getNamespace())) {
					messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.clientConfig.getNamespace()));
				}
				return messageExt;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public long searchOffset(final String addr, final String topic, final int queueId, final long timestamp, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException, MQBrokerException {
		SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
		requestHeader.setTopic(topic);
		requestHeader.setQueueId(queueId);
		requestHeader.setTimestamp(timestamp);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				SearchOffsetResponseHeader responseHeader = response.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
				return responseHeader.getOffset();
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public long searchOffset(final String addr, final MessageQueue messageQueue, final long timestamp, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, RemotingCommandException, MQBrokerException, InterruptedException {
		return searchOffset(addr, messageQueue, timestamp, BoundaryType.LOWER, timeoutMillis);
	}

	public long searchOffset(final String addr, final MessageQueue messageQueue, final long timestamp, final BoundaryType boundaryType, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException, MQBrokerException {
		SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
		requestHeader.setTopic(messageQueue.getTopic());
		requestHeader.setQueueId(messageQueue.getQueueId());
		requestHeader.setBrokerName(messageQueue.getBrokerName());
		requestHeader.setTimestamp(timestamp);
		requestHeader.setBoundaryType(boundaryType);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				SearchOffsetResponseHeader responseHeader = response.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
				return responseHeader.getOffset();
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public long getMaxOffset(final String addr, final MessageQueue messageQueue, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException, MQBrokerException {
		GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
		requestHeader.setTopic(messageQueue.getTopic());
		requestHeader.setQueueId(messageQueue.getQueueId());
		requestHeader.setBrokerName(messageQueue.getBrokerName());

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				GetMaxOffsetResponseHeader responseHeader = response.decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);
				return responseHeader.getOffset();
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), request.getRemark(), addr);
	}

	public long getMinOffset(final String addr, final MessageQueue messageQueue, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException, MQBrokerException {
		GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
		requestHeader.setTopic(messageQueue.getTopic());
		requestHeader.setQueueId(messageQueue.getQueueId());
		requestHeader.setBrokerName(messageQueue.getBrokerName());

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				GetMinOffsetResponseHeader responseHeader = response.decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);
				return responseHeader.getOffset();
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public List<String> getConsumerIdListByGroup(final String addr, final String consumerGroup, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
		GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
		requestHeader.setConsumerGroup(consumerGroup);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (request.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					GetConsumerListByGroupResponseBody body = GetConsumerListByGroupResponseBody.decode(response.getBody(), GetConsumerListByGroupResponseBody.class);
					return body.getConsumerIdList();
				}
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public long getEarliestMsgStoretime(final String addr, final MessageQueue messageQueue, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException, MQBrokerException {
		GetEarliestMsgStoretimeRequestHeader requestHeader = new GetEarliestMsgStoretimeRequestHeader();
		requestHeader.setTopic(messageQueue.getTopic());
		requestHeader.setQueueId(messageQueue.getQueueId());
		requestHeader.setBrokerName(messageQueue.getBrokerName());

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				GetEarliestStoretimeResponseHeader responseHeader = response.decodeCommandCustomHeader(GetEarliestStoretimeResponseHeader.class);
				return responseHeader.getTimestamp();
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public long queryConsumerOffset(final String addr, final QueryConsumerOffsetRequestHeader requestHeader, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException, MQBrokerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				QueryConsumerOffsetResponseHeader responseHeader = response.decodeCommandCustomHeader(QueryConsumerOffsetResponseHeader.class);
				return responseHeader.getOffset();
			case ResponseCode.QUERY_NOT_FOUND:
				throw new OffsetNotFoundException(response.getCode(), response.getRemark(), addr);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), request.getRemark(), addr);
	}

	public void updateConsumerOffset(final String addr, final UpdateConsumerOffsetRequestHeader requestHeader, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public void updateConsumerOffsetOneway(final String addr, final UpdateConsumerOffsetRequestHeader requestHeader, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);

		this.remotingClient.invokeOneway(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
	}

	public int sendHeartbeat(final String addr, final HeartbeatData heartbeatData, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, new HeartbeatRequestHeader());
		request.setLanguage(clientConfig.getLanguage());
		request.setBody(heartbeatData.encode());

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return response.getVersion();
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public HeartbeatV2Result sendHeartbeatV2(final String addr, final HeartbeatData heartbeatData, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, new HeartbeatRequestHeader());

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getExtFields() != null) {
					return new HeartbeatV2Result(response.getVersion(), Boolean.parseBoolean(response.getExtFields().get(MixAll.IS_SUB_CHANGE)), Boolean.parseBoolean(response.getExtFields().get(MixAll.IS_SUPPORT_HEART_BEAT_V2)));
				}
				return new HeartbeatV2Result(response.getVersion(), false, false);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public void unregisterClient(final String addr, final String clientID, final String producerGroup, final String consumerGroup, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
		UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
		requestHeader.setClientID(clientID);
		requestHeader.setProducerGroup(producerGroup);
		requestHeader.setConsumerGroup(consumerGroup);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public void endTransactionOneway(final String addr, final EndTransactionRequestHeader requestHeader, final String remakr, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);
		request.setRemark(remakr);

		this.remotingClient.invokeOneway(addr, request, timeoutMillis);
	}

	public void queryMessage(final String addr, final QueryMessageRequestHeader requestHeader, final long timeoutMillis, final InvokeCallback invokeCallback, final Boolean isUnqiueKey) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
		request.addExtField(MixAll.UNIQUE_MSG_QUERY_FLAG, isUnqiueKey.toString());

		this.remotingClient.invokeAsync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis, invokeCallback);
	}

	public boolean registerClient(final String addr, final HeartbeatData heartbeatData, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, new HeartbeatRequestHeader());
		request.setBody(heartbeatData.encode());

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		return response.getCode() == ResponseCode.SUCCESS;
	}

	public void consumeSendMessageBack(final String addr, final String brokerName, final MessageExt msg, final String consumerGroup, final int delayLevel, final long timeoutLevel, final int maxConsumeRetryTimes) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
		ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
		requestHeader.setGroup(consumerGroup);
		requestHeader.setOriginTopic(msg.getTopic());
		requestHeader.setOffset(msg.getCommitLogOffset());
		requestHeader.setDelayLevel(delayLevel);
		requestHeader.setOriginMsgId(msg.getMsgId());
		requestHeader.setMaxReconsumeTimes(maxConsumeRetryTimes);
		requestHeader.setBrokerName(brokerName);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutLevel);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public Set<MessageQueue> lockBatchMQ(final String addr, final LockBatchRequestBody requestBody, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, new LockBatchMqRequestHeader());
		request.setBody(requestBody.encode());

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody(), LockBatchResponseBody.class);
				return responseBody.getLockOKMQSet();
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public void unlockBatchMQ(final String addr, final UnlockBatchRequestBody requestBody, final long timeoutMillis, final boolean oneway) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException, MQBrokerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, new UnlockBatchMqRequestHeader());
		request.setBody(requestBody.encode());

		if (oneway) {
			this.remotingClient.invokeOneway(addr, request, timeoutMillis);
		}
		else {
			RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
			switch (response.getCode()) {
				case ResponseCode.SUCCESS:
					return;
				default:
					break;
			}

			throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
		}
	}

	public TopicStatsTable getTopicStatsInfo(final String addr, final String topic, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetTopicStatsInfoRequestHeader requestHeader = new GetTopicStatsInfoRequestHeader();
		requestHeader.setTopic(topic);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return TopicStatsTable.decode(response.getBody(), TopicStatsTable.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public ConsumeStats getConsumeStats(final String addr, final String consumerGroup, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
		return getConsumeStats(addr, consumerGroup, null, timeoutMillis);
	}

	public ConsumeStats getConsumeStats(final String addr, final String consumerGroup, final String topic, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetConsumerStatsRequestHeader requestHeader = new GetConsumerStatsRequestHeader();
		requestHeader.setConsumerGroup(consumerGroup);
		requestHeader.setTopic(topic);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
		
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return ConsumeStats.decode(response.getBody(), ConsumeStats.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public ProducerConnection getProducerConnectionList(final String addr, final String producerGroup, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
		GetProducerConnectionListRequestHeader requestHeader = new GetProducerConnectionListRequestHeader();
		requestHeader.setProducerGroup(producerGroup);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return ProducerConnection.decode(response.getBody(), ProducerConnection.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public ProducerTableInfo getAllProducerInfo(final String addr, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetAllProducerInfoRequestHeader requestHeader = new GetAllProducerInfoRequestHeader();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_PRODUCER_INFO, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return ProducerTableInfo.decode(response.getBody(), ProducerTableInfo.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public ConsumerConnection getConsumerConnection(final String addr, final String consumerGroup, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetConsumerConnectionListRequestHeader requestHeader = new GetConsumerConnectionListRequestHeader();
		requestHeader.setConsumerGroup(consumerGroup);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return ConsumerConnection.decode(response.getBody(), ConsumerConnection.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public KVTable getBrokerRuntimeInfo(final String addr, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_RUNTIME_INFO, null);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return KVTable.decode(response.getBody(), KVTable.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public void addBroker(final String addr, final String brokerConfigPath, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		AddBrokerRequestHeader requestHeader = new AddBrokerRequestHeader();
		requestHeader.setConfigPath(brokerConfigPath);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ADD_BROKER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public void removeBroker(final String addr, final String clusterName, String brokerName, long brokerId, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemoveBrokerRequestHeader requestHeader = new RemoveBrokerRequestHeader();
		requestHeader.setBrokerClusterName(clusterName);
		requestHeader.setBrokerName(brokerName);
		requestHeader.setBrokerId(brokerId);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REMOVE_BROKER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public void updateBrokerConfig(final String addr, final Properties properties, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, UnsupportedEncodingException {
		Validators.checkBrokerConfig(properties);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);

		String str = MixAll.properties2String(properties);
		if (str != null && str.length() > 0) {
			request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));

			RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

			switch (response.getCode()) {
				case ResponseCode.SUCCESS:
					return;
				default:
					break;
			}

			throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
		}
	}

	public Properties getBrokerConfig(final String addr, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException, UnsupportedEncodingException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONFIG, null);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET));
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public void updateColdDataFlowCtrGroupConfig(final String addr, final Properties properties, final long timeoutMillis) throws MQBrokerException, UnsupportedEncodingException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_COLD_DATA_FLOW_CTR_CONFIG, null);

		String str = MixAll.properties2String(properties);
		if (str != null && str.length() > 0) {
			request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));

			RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

			switch (response.getCode()) {
				case ResponseCode.SUCCESS:
					return;
				default:
					break;
			}

			throw new MQBrokerException(response.getCode(), response.getRemark());
		}
	}

	public void removeColdDataFlowCtrGroupConfig(final String addr, final String consumerGroup, final long timeoutMillis) throws UnsupportedEncodingException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REMOVE_COLD_DATA_FLOW_CTR_CONFIG, null);

		if (consumerGroup != null && consumerGroup.length() > 0) {
			request.setBody(consumerGroup.getBytes(MixAll.DEFAULT_CHARSET));

			RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

			switch (response.getCode()) {
				case ResponseCode.SUCCESS:
					return;
				default:
					break;
			}

			throw new MQBrokerException(response.getCode(), request.getRemark());
		}
	}

	public String getColdDataFlowCtrInfo(final String addr, final long timeoutMillis) throws MQBrokerException, UnsupportedEncodingException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_COLD_DATA_FLOW_CTR_CONFIG, null);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null && response.getBody().length > 0) {
					return new String(response.getBody(), MixAll.DEFAULT_CHARSET);
				}
				return null;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public String setCommitLogReadAheadMode(final String addr, final String mode, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SET_COMMITLOG_READ_MODE, null);

		Map<String, String> extFields = new HashMap<>();
		extFields.put(FileReadAheadMode.READ_AHEAD_MODE, mode);
		request.setExtFields(extFields);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getRemark() != null && response.getRemark().length() > 0) {
					return response.getRemark();
				}
				return null;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public ClusterInfo getBrokerClusterInfo(final String addr, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);

		RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return ClusterInfo.decode(response.getBody(), ClusterInfo.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public TopicRouteData getDefaultTopicRouteInfoFromNameServer(final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
		return getTopicRouteInfoFromNameServer(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC, timeoutMillis, false);
	}

	public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
		return getTopicRouteInfoFromNameServer(topic, timeoutMillis, true);
	}

	public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis, boolean allowTopicNotExist) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
		requestHeader.setTopic(topic);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.TOPIC_NOT_EXIST:
				if (allowTopicNotExist) {
					log.warn("get topic [{}] RouteInfoFromNameServer is not exist value", topic);
				}
				break;
			case ResponseCode.SUCCESS:
				byte[] body = response.getBody();
				if (body != null) {
					return TopicRouteData.decode(body, TopicRouteData.class);
				}
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public TopicList getTopicListFromNameServer(final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, null);

		RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				byte[] body = response.getBody();
				if (body != null) {
					return TopicList.decode(body, TopicList.class);
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public int wipeWritePermOfBroker(final String namesrvAddr, String brokerName, final long timeoutMillis) throws MQBrokerException, RemotingCommandException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		WipeWriterPermOfBrokerRequestHeader requestHeader = new WipeWriterPermOfBrokerRequestHeader();
		requestHeader.setBrokerName(brokerName);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.WIPE_WRITE_PERM_OF_BROKER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				WipeWritePermOfBrokerResponseHeader responseHeader = response.decodeCommandCustomHeader(WipeWritePermOfBrokerResponseHeader.class);
				return responseHeader.getWipeTopicCount();
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public int addWritePermofBroker(final String namesrvAddr, String brokerName, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException, MQClientException {
		AddWritePermOfBrokerRequestHeader requestHeader = new AddWritePermOfBrokerRequestHeader();
		requestHeader.setBrokerName(brokerName);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ADD_WRITE_PERM_OF_BROKER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				WipeWritePermOfBrokerResponseHeader responseHeader = response.decodeCommandCustomHeader(WipeWritePermOfBrokerResponseHeader.class);
				return responseHeader.getWipeTopicCount();
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public int addWritePermOfBroker(final String namesrvAddr, String brokerName, final long timeoutMillis) throws MQClientException, RemotingCommandException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		AddWritePermOfBrokerRequestHeader requestHeader = new AddWritePermOfBrokerRequestHeader();
		requestHeader.setBrokerName(brokerName);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.ADD_WRITE_PERM_OF_BROKER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				AddWritePermOfBrokerResponseHeader responseHeader = response.decodeCommandCustomHeader(AddWritePermOfBrokerResponseHeader.class);
				return responseHeader.getAddTopicCount();
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public void deleteTopicInBroker(final String addr, final String topic, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
		requestHeader.setTopic(topic);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public void deleteTopicInNameServer(final String addr, final String topic, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		DeleteTopicFromNamesrvRequestHeader requestHeader = new DeleteTopicFromNamesrvRequestHeader();
		requestHeader.setTopic(topic);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public void deleteSubscriptionGroup(final String addr, final String groupName, final boolean removeOffset, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		DeleteSubscriptionGroupRequestHeader requestHeader = new DeleteSubscriptionGroupRequestHeader();
		requestHeader.setGroupName(groupName);
		requestHeader.setCleanOffset(removeOffset);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public String getKVConfigValue(final String namespace, final String key, final long timeoutMillis) throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
		GetKVConfigRequestHeader requestHeader = new GetKVConfigRequestHeader();
		requestHeader.setNamespace(namespace);
		requestHeader.setKey(key);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				GetKVConfigResponseHeader responseHeader = response.decodeCommandCustomHeader(GetKVConfigResponseHeader.class);
				return responseHeader.getValue();
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public void putKVConfigValue(final String namespace, final String key, final String value, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		PutKVConfigRequestHeader requestHeader = new PutKVConfigRequestHeader();
		requestHeader.setNamespace(namespace);
		requestHeader.setKey(key);
		requestHeader.setValue(value);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG, requestHeader);

		List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
		if (nameServerAddressList != null) {
			RemotingCommand errResponse = null;
			for (String namesrvAddr : nameServerAddressList) {
				RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);

				assert response != null;
				switch (response.getCode()) {
					case ResponseCode.SUCCESS:
						break;
					default:
						errResponse = response;
				}
			}

			if (errResponse != null) {
				throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
			}
		}
	}

	public void deleteKVConfigValue(final String namespace, final String key, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		DeleteKVConfigRequestHeader requestHeader = new DeleteKVConfigRequestHeader();
		requestHeader.setNamespace(namespace);
		requestHeader.setKey(key);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG, requestHeader);

		List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
		if (nameServerAddressList != null) {
			RemotingCommand errResponse = null;
			for (String namesrvAddr : nameServerAddressList) {
				RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);

				assert response != null;
				switch (response.getCode()) {
					case ResponseCode.SUCCESS:
						break;
					default:
						errResponse = response;
				}
			}

			if (errResponse != null) {
				throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
			}
		}
	}

	public KVTable getKVListByNamespace(final String namespace, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetKVListByNamespaceRequestHeader requestHeader = new GetKVListByNamespaceRequestHeader();
		requestHeader.setNamespace(namespace);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KVLIST_BY_NAMESPACE, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return KVTable.decode(response.getBody(), KVTable.class);
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public Map<MessageQueue, Long> invokeBrokerToResetOffset(final String addr, final String topic, final String group, final long timestamp, final boolean isForce, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
		return invokeBrokerToResetOffset(addr, topic, group, timestamp, isForce, timeoutMillis, false);
	}

	public Map<MessageQueue, Long> invokeBrokerToResetOffset(final String addr, final String topic, final String group, final long timestamp, int queueId, Long offset, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
		requestHeader.setTopic(topic);
		requestHeader.setGroup(group);
		requestHeader.setQueueId(queueId);
		requestHeader.setTimestamp(timestamp);
		requestHeader.setOffset(offset);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					return ResetOffsetBody.decode(response.getBody(), ResetOffsetBody.class).getOffsetTable();
				}
				break;
			case ResponseCode.TOPIC_NOT_EXIST:
			case ResponseCode.SUBSCRIPTION_NOT_EXIST:
			case ResponseCode.SYSTEM_ERROR:
				log.warn("Invoke broker to reset offset error code={}, remark={}", response.getCode(), response.getRemark());
				break;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public Map<MessageQueue, Long> invokeBrokerToResetOffset(final String addr, final String topic, final String group, final long timestamp, final boolean isForce, final long timeoutMillis, boolean isC) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
		requestHeader.setTopic(topic);
		requestHeader.setGroup(group);
		requestHeader.setTimestamp(timestamp);
		requestHeader.setForce(isForce);
		requestHeader.setOffset(-1L);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
		if (isC) {
			request.setLanguage(LanguageCode.CPP);
		}

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					ResetOffsetBody body = ResetOffsetBody.decode(response.getBody(), ResetOffsetBody.class);
					return body.getOffsetTable();
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public Map<String, Map<MessageQueue, Long>> invokeBrokerToGetConsumerStatus(final String addr, final String topic, final String grop, final String clientAddr, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
		requestHeader.setTopic(topic);
		requestHeader.setGroup(grop);
		requestHeader.setClientAddr(clientAddr);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (request.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					GetConsumerStatusBody body = GetConsumerStatusBody.decode(response.getBody(), GetConsumerStatusBody.class);
					return body.getConsumerTable();
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public GroupList queryTopicConsumeByWho(final String addr, final String topic, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		QueryTopicConsumeByWhoRequestHeader requestHeader = new QueryTopicConsumeByWhoRequestHeader();
		requestHeader.setTopic(topic);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return GroupList.decode(response.getBody(), GroupList.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public TopicList queryTopicsByConsumer(final String addr, final String group, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		QueryTopicsByConsumerRequestHeader requestHeader = new QueryTopicsByConsumerRequestHeader();
		requestHeader.setGroup(group);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPICS_BY_CONSUMER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return TopicList.decode(response.getBody(), TopicList.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public SubscriptionData querySubscriptionByConsumer(final String addr, final String group, final String topic, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		QuerySubscriptionByConsumerRequestHeader requestHeader = new QuerySubscriptionByConsumerRequestHeader();
		requestHeader.setGroup(group);
		requestHeader.setTopic(topic);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_SUBSCRIPTION_BY_CONSUMER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return QuerySubscriptionResponseBody.decode(response.getBody(), QuerySubscriptionResponseBody.class).getSubscriptionData();
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public List<QueueTimeSpan> queryConsumeTimeSpan(final String addr, final String topic, final String group, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
		QueryConsumeTimeSpanRequestHeader requestHeader = new QueryConsumeTimeSpanRequestHeader();
		requestHeader.setTopic(topic);
		requestHeader.setGroup(group);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_TIME_SPAN, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return QueryConsumeTimeSpanBody.decode(response.getBody(), QueryConsumeTimeSpanBody.class).getConsumeTimeSpanSet();
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public TopicList getTopicsByCluster(final String cluster, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetTopicsByClusterRequestHeader requestHeader = new GetTopicsByClusterRequestHeader();
		requestHeader.setCluster(cluster);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPICS_BY_CLUSTER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					return TopicList.decode(response.getBody(), TopicList.class);
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public TopicList getSystemTopicList(final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS, null);

		RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
					if (topicList.getTopicList() != null && !topicList.getTopicList().isEmpty() && UtilAll.isBlank(topicList.getBrokerAddr())) {
						TopicList tmp = getSystemTopicListFromBroker(topicList.getBrokerAddr(), timeoutMillis);
						if (tmp.getTopicList() != null && !tmp.getTopicList().isEmpty()) {
							topicList.getTopicList().addAll(tmp.getTopicList());
						}
					}

					return topicList;
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public TopicList getSystemTopicListFromBroker(final String addr, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, null);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					return TopicList.decode(response.getBody(), TopicList.class);
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public boolean cleanExpiredConsumeQueue(final String addr, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE, null);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return true;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public boolean deleteExpiredCommitLog(final String addr, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_EXPIRED_COMMITLOG, null);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return true;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public boolean cleanUnusedTopicByAddr(final String addr, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_UNUSED_TOPIC, null);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return true;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public ConsumerRunningInfo getConsumerRunningInfo(final String addr, final String consumerGroup, final String clientId, final boolean jstack, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
		requestHeader.setConsumerGroup(consumerGroup);
		requestHeader.setClientId(clientId);
		requestHeader.setJstackEnable(jstack);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					return ConsumerRunningInfo.decode(response.getBody(), ConsumerRunningInfo.class);
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public ConsumeMessageDirectlyResult consumeMessageDirectly(final String addr, final String consumerGroup, final String clientId, final String topic, final String msgId, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		ConsumeMessageDirectlyResultRequestHeader requestHeader = new ConsumeMessageDirectlyResultRequestHeader();
		requestHeader.setTopic(topic);
		requestHeader.setConsumerGroup(consumerGroup);
		requestHeader.setClientId(clientId);
		requestHeader.setMsgId(msgId);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					return ConsumeMessageDirectlyResult.decode(response.getBody(), ConsumeMessageDirectlyResult.class);
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public Map<Integer, Long> queryCorrectionOffset(final String addr, final String topic, final String group, Set<String> filterGroup, long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		QueryCorrectionOffsetHeader requestHeader = new QueryCorrectionOffsetHeader();
		requestHeader.setCompareGroup(group);
		requestHeader.setTopic(topic);
		if (filterGroup != null) {
			StringBuilder sb = new StringBuilder();
			String splitor = "";
			for (String s : filterGroup) {
				sb.append(s).append(splitor);
				splitor = ",";
			}
			requestHeader.setFilterGroups(sb.toString());
		}

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CORRECTION_OFFSET, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return QueryCorrectionOffsetBody.decode(response.getBody(), QueryCorrectionOffsetBody.class).getCorrectionOffsets();
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public TopicList getUnitTopicList(final boolean containRetry, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_UNIT_TOPIC_LIST, null);

		RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
					if (!containRetry) {
						Iterator<String> iterator = topicList.getTopicList().iterator();
						while (iterator.hasNext()) {
							String topic = iterator.next();
							if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
								iterator.remove();
							}
						}
					}

					return topicList;
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public TopicList getHasUnitSubTopicList(final boolean containRetry, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, null);

		RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
					if (!containRetry) {
						Iterator<String> iterator = topicList.getTopicList().iterator();
						while (iterator.hasNext()) {
							String topic = iterator.next();
							if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
								iterator.remove();
							}
						}
					}
					return topicList;
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public TopicList getHasUnitSubUnUnitTopicList(final boolean containRetry, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST, null);

		RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					TopicList topicList = TopicList.decode(response.getBody(), TopicList.class);
					if (!containRetry) {
						Iterator<String> iterator = topicList.getTopicList().iterator();
						while (iterator.hasNext()) {
							String topic = iterator.next();
							if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
								iterator.remove();
							}
						}
					}
					return topicList;
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public void cloneGroupOFfset(final String addr, final String srcGroup, final String destGroup, final String topic, final boolean isOffline, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		CloneGroupOffsetRequestHeader requestHeader = new CloneGroupOffsetRequestHeader();
		requestHeader.setSrcGroup(srcGroup);
		requestHeader.setDestGroup(destGroup);
		requestHeader.setTopic(topic);
		requestHeader.setOffline(isOffline);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLONE_GROUP_OFFSET, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public BrokerStatsData viewBrokerStatsData(final String brokerAddr, final String statsName, final String statsKey, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		ViewBrokerStatsDataRequestHeader requestHeader = new ViewBrokerStatsDataRequestHeader();
		requestHeader.setStatsKey(statsKey);
		requestHeader.setStatsName(statsName);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_BROKER_STATS_DATA, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					return BrokerStatsData.decode(response.getBody(), BrokerStatsData.class);
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public Collection<String> getClusterList(String topic, long timeoutMillis) {
		return Collections.EMPTY_SET;
	}

	public ConsumeStatsList fetchConsumeStatsInBroker(final String addr, final boolean isOrder, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetConsumerStatsInBrokerHeader requestHeader = new GetConsumerStatsInBrokerHeader();
		requestHeader.setOrder(isOrder);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONSUME_STATS, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				if (response.getBody() != null) {
					return ConsumeStatsList.decode(response.getBody(), ConsumeStatsList.class);
				}
			default:
				break;
		}

		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public SubscriptionGroupWrapper getAllSubscriptionGroup(final String addr, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return SubscriptionGroupWrapper.decode(response.getBody(), SubscriptionGroupWrapper.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public SubscriptionGroupConfig getSubscriptionGroupConfig(final String addr, final String group, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetSubscriptionGroupConfigRequestHeader requestHeader = new GetSubscriptionGroupConfigRequestHeader();
		requestHeader.setGroup(group);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_SUBSCRIPTIONGROUP_CONFIG, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return RemotingSerializable.decode(response.getBody(), SubscriptionGroupConfig.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public TopicConfigSerializeWrapper getAllTopicConfig(final String addr, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return RemotingSerializable.decode(response.getBody(), TopicConfigSerializeWrapper.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), request.getRemark(), addr);
	}

	public void upateNameServerConfig(final Properties properties, final List<String> nameServers, final long timeoutMillis) throws UnsupportedEncodingException, MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		String str = MixAll.properties2String(properties);
		if (str == null || str.length() < 1) {
			return;
		}

		List<String> invokeNameServers = nameServers == null || nameServers.isEmpty() ? this.remotingClient.getNameServerAddressList() : nameServers;
		if (invokeNameServers == null || invokeNameServers.isEmpty()) {
			return;
		}

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_NAMESRV_CONFIG, null);
		request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));

		RemotingCommand errResponse = null;
		for (String nameServer : invokeNameServers) {
			RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);

			assert response != null;
			switch (response.getCode()) {
				case ResponseCode.SUCCESS:
					break;
				default:
					errResponse = response;
			}
		}

		if (errResponse != null) {
			throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
		}
	}

	public Map<String, Properties> getNameServerConfig(final List<String> nameServers, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, UnsupportedEncodingException {
		List<String> invokedNameServers = nameServers == null || nameServers.isEmpty() ? this.remotingClient.getNameServerAddressList() : nameServers;
		if (invokedNameServers == null || invokedNameServers.isEmpty()) {
			return null;
		}

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_NAMESRV_CONFIG, null);

		Map<String, Properties> configMap = new HashMap<>(4);
		for (String nameServer : invokedNameServers) {
			RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);

			assert response != null;
			if (ResponseCode.SUCCESS == response.getCode()) {
				configMap.put(nameServer, MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET)));
			}
			else {
				throw new MQClientException(response.getCode(), response.getRemark());
			}
		}

		return configMap;
	}

	public QueryConsumeQueueResponseBody queryConsumeQueue(final String addr, final String topic, final int queueId, final long index, final int count, final String consumerGroup, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		QueryConsumeQueueRequestHeader requestHeader = new QueryConsumeQueueRequestHeader();
		requestHeader.setTopic(topic);
		requestHeader.setQueueId(queueId);
		requestHeader.setIndex(index);
		requestHeader.setCount(count);
		requestHeader.setConsumerGroup(consumerGroup);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_QUEUE, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		if (ResponseCode.SUCCESS == response.getCode()) {
			return QueryConsumeQueueResponseBody.decode(response.getBody(), QueryConsumeQueueResponseBody.class);
		}
		throw new MQClientException(response.getCode(), response.getRemark());
	}

	public void checkClientInBroker(final String addr, final String consumerGroup, final String clientId, final SubscriptionData subscriptionData, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_CLIENT_CONFIG, null);

		CheckClientRequestBody requestBody = new CheckClientRequestBody();
		requestBody.setClientId(clientId);
		requestBody.setGroup(consumerGroup);
		requestBody.setSubscriptionData(subscriptionData);

		request.setBody(requestBody.encode());

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		if (ResponseCode.SUCCESS != response.getCode()) {
			throw new MQClientException(response.getCode(), request.getRemark());
		}
	}

	public boolean resumeCheckHalfMessage(final String addr, String topic, String msgId, final long timeoutMillis) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		ResumeCheckHalfMessageRequestHeader requestHeader = new ResumeCheckHalfMessageRequestHeader();
		requestHeader.setMsgId(msgId);
		requestHeader.setTopic(topic);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESUME_CHECK_HALF_MESSAGE, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return true;
			default:
				log.warn("Failed to resume half message chekc logic. Remark={}", response.getRemark());
				return false;
		}
	}

	public void setMessageRequestMode(final String addr, final String topic, final String consumerGroup, final MessageRequestMode requestMode, final int popShareQueueNum, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SET_MESSAGE_REQUEST_MODE, null);

		SetMessageRequestModeRequestBody requestBody = new SetMessageRequestModeRequestBody();
		requestBody.setTopic(topic);
		requestBody.setConsumerGroup(consumerGroup);
		requestBody.setMode(requestMode);
		requestBody.setPopShareQueueNum(popShareQueueNum);

		request.setBody(requestBody.encode());

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		if (ResponseCode.SUCCESS != response.getCode()) {
			throw new MQClientException(response.getCode(), response.getRemark());
		}
	}

	public TopicConfigAndQueueMapping getTopicConfig(final String addr, String topic, long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetTopicConfigRequestHeader requestHeader = new GetTopicConfigRequestHeader();
		requestHeader.setTopic(topic);
		requestHeader.setLo(true);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_CONFIG, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return RemotingSerializable.decode(response.getBody(), TopicConfigAndQueueMapping.class);
			case ResponseCode.TOPIC_NOT_EXIST:
				break;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public void createStaticTopic(final String addr, final String defaultTopic, final TopicConfig topicConfig, final TopicQueueMappingDetail topicQueueMappingDetail,
			boolean force, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
		requestHeader.setTopic(topicConfig.getTopicName());
		requestHeader.setDefaultTopic(defaultTopic);
		requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
		requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
		requestHeader.setPerm(topicConfig.getPerm());
		requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
		requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
		requestHeader.setOrder(topicConfig.isOrder());
		requestHeader.setForce(force);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}
		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public GroupForbidden updateAndgetGroupForbidden(String addr, UpdateGroupForbiddenRequestHeader requestHeader, long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_GET_GROUP_FORBIDDEN, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return RemotingSerializable.decode(response.getBody(), GroupForbidden.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public void resetMasterFlushOffset(final String addr, final long masterFlushOffset) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		ResetMasterFlushOffsetHeader requestHeader = new ResetMasterFlushOffsetHeader();
		requestHeader.setMasterFlushOffset(masterFlushOffset);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESET_MASTER_FLUSH_OFFSET, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public HARuntimeInfo getBrokerHAStatus(final String addr, final long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_HA_STATUS, null);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return RemotingSerializable.decode(response.getBody(), HARuntimeInfo.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public GetMetadataResponseHeader getControllerMetaData(final String controllerAddress) throws RemotingCommandException, MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_METADATA_INFO, null);

		RemotingCommand response = this.remotingClient.invokeSync(controllerAddress, request, 3000);

		assert response != null;
		if (response.getCode() == ResponseCode.SUCCESS) {
			return response.decodeCommandCustomHeader(GetMetadataResponseHeader.class);
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public BrokerReplicasInfo getInSyncStateData(final String controllerAddress, final List<String> brokers) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException {
		GetMetadataResponseHeader controllerMetaData = getControllerMetaData(controllerAddress);

		assert controllerMetaData != null;
		assert controllerMetaData.getControllerLeaderAddress() != null;
		String leaderAddress = controllerMetaData.getControllerLeaderAddress();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_GET_SYNC_STATE_DATA, null);
		request.setBody(RemotingSerializable.encode(brokers));

		RemotingCommand response = this.remotingClient.invokeSync(leaderAddress, request, 3000);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return RemotingSerializable.decode(response.getBody(), BrokerReplicasInfo.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), leaderAddress);
	}

	public EpochEntryCache getBrokerEpochCache(String brokerAddr) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_EPOCH_CACHE, null);

		RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, 3000);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return RemotingSerializable.decode(response.getBody(), EpochEntryCache.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr);
	}

	public Map<String, Properties> getControllerConfig(final List<String> controllerServers, final long timeoutMillis) throws UnsupportedEncodingException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
		List<String> invokeControllerServers = controllerServers == null || controllerServers.isEmpty() ? this.remotingClient.getNameServerAddressList() : controllerServers;
		if (invokeControllerServers == null || invokeControllerServers.isEmpty()) {
			return null;
		}

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONTROLLER_CONFIG, null);

		Map<String, Properties> configMap = new HashMap<>(4);
		for (String controller : invokeControllerServers) {
			RemotingCommand response = this.remotingClient.invokeSync(controller, request, timeoutMillis);

			assert response != null;

			if (ResponseCode.SUCCESS == response.getCode()) {
				configMap.put(controller, MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET)));
			}
			else {
				throw new MQClientException(response.getCode(), response.getRemark());
			}
		}

		return configMap;
	}

	public void updateControllerConfig(final Properties properties, final List<String> controllers, final long timeoutMillis) throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, UnsupportedEncodingException {
		String str = MixAll.properties2String(properties);
		if (str.length() < 1 || controllers == null || controllers.isEmpty()) {
			return;
		}

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONTROLLER_CONFIG, null);
		request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));

		RemotingCommand errResp = null;
		for (String controller : controllers) {
			RemotingCommand response = this.remotingClient.invokeSync(controller, request, timeoutMillis);

			assert response != null;
			switch (response.getCode()) {
				case ResponseCode.SUCCESS:
					break;
				default:
					errResp = response;
			}

			if (errResp != null) {
				throw new MQClientException(errResp.getCode(), errResp.getRemark());
			}
		}
	}

	public Pair<ElectMasterResponseHeader, BrokerMemberGroup> electMaster(String controllerAddr, String clusterName, String brokerName, Long brokerId) throws MQBrokerException, RemotingCommandException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetMetadataResponseHeader controllerMetaData = this.getControllerMetaData(controllerAddr);
		assert controllerMetaData != null;
		assert controllerMetaData.getControllerLeaderAddress() != null;
		String leaderAddress = controllerMetaData.getControllerLeaderAddress();

		ElectMasterRequestHeader requestHeader = ElectMasterRequestHeader.ofAdminTrigger(clusterName, brokerName, brokerId);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONTROLLER_ELECT_MASTER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(leaderAddress, request, 3000);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				BrokerMemberGroup brokerMemberGroup = RemotingSerializable.decode(response.getBody(), BrokerMemberGroup.class);
				ElectMasterResponseHeader responseHeader = response.decodeCommandCustomHeader(ElectMasterResponseHeader.class);
				return new Pair<>(responseHeader, brokerMemberGroup);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), leaderAddress);
	}

	public void cleanControllerBrokerData(String controllerAddr, String clusterName, String brokerName, String brokerControllerIdsToClean, boolean isCleanLivingBroker) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingCommandException {
		GetMetadataResponseHeader controllerMetaData = this.getControllerMetaData(controllerAddr);
		assert controllerMetaData != null;
		assert controllerMetaData.getControllerLeaderAddress() != null;
		String leaderAddress = controllerMetaData.getControllerLeaderAddress();

		CleanControllerBrokerDataRequestHeader requestHeader = new CleanControllerBrokerDataRequestHeader(clusterName, brokerName, brokerControllerIdsToClean, isCleanLivingBroker);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_BROKER_DATA, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(leaderAddress, request, 3000);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), leaderAddress);
	}

	public void createUser(String addr, UserInfo userInfo, long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		CreateUserRequestHeader requestHeader = new CreateUserRequestHeader(userInfo.getUsername());

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_CREATE_USER, requestHeader);
		request.setBody(RemotingSerializable.encode(userInfo));

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public void updateUser(String addr, UserInfo userInfo, long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		UpdateUserRequestHeader requestHeader = new UpdateUserRequestHeader();

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_UPDATE_USER, requestHeader);
		request.setBody(RemotingSerializable.encode(userInfo));

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public void deleteUser(String addr, String username, long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		DeleteUserRequestHeader requestHeader = new DeleteUserRequestHeader();
		requestHeader.setUsername(username);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_DELETE_USER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public UserInfo getUser(String addr, String username, long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetUserRequestHeader requestHeader = new GetUserRequestHeader();
		requestHeader.setUsername(username);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_GET_USER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return RemotingSerializable.decode(response.getBody(), UserInfo.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public List<UserInfo> listUser(String addr, String filter, long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		ListUsersRequestHeader requestHeader = new ListUsersRequestHeader(filter);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_LIST_USER, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return RemotingSerializable.decodeList(response.getBody(), UserInfo.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public void createAcl(String addr, AclInfo aclInfo, long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		CreateAclRequestHeader requestHeader = new CreateAclRequestHeader(aclInfo.getSubject());

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_CREATE_ACL, requestHeader);
		request.setBody(RemotingSerializable.encode(aclInfo));

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public void updateAcl(String addr, AclInfo aclInfo, long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		UpdateAclRequestHeader requestHeader = new UpdateAclRequestHeader(aclInfo.getSubject());

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_UPDATE_ACL, requestHeader);
		request.setBody(RemotingSerializable.encode(aclInfo));

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public void deleteAcl(String addr, String subject, String resource, long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		DeleteAclRequestHeader requestHeader = new DeleteAclRequestHeader(subject, resource);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_DELETE_ACL, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return;
			default:
				break;
		}
		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public AclInfo getAcl(String addr, String subject, long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		GetAclRequestHeader requestHeader = new GetAclRequestHeader(subject);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_GET_ACL, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return RemotingSerializable.decode(response.getBody(), AclInfo.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}

	public List<AclInfo> listAcl(String addr, String subjectFilter, String resoueceFilter, long timeoutMillis) throws MQBrokerException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
		ListAclsRequestHeader requestHeader = new ListAclsRequestHeader(subjectFilter, resoueceFilter);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.AUTH_LIST_ACL, requestHeader);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
		assert response != null;
		switch (response.getCode()) {
			case ResponseCode.SUCCESS:
				return RemotingSerializable.decodeList(response.getBody(), AclInfo.class);
			default:
				break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
	}


}