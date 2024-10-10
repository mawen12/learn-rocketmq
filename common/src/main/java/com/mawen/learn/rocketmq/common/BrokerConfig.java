package com.mawen.learn.rocketmq.common;

import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.common.annotation.ImportantField;
import com.mawen.learn.rocketmq.common.constant.PermName;
import com.mawen.learn.rocketmq.common.message.MessageRequestMode;
import com.mawen.learn.rocketmq.common.metrics.MetricsExporterType;
import com.mawen.learn.rocketmq.common.topic.TopicValidator;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class BrokerConfig extends BrokerIdentity {

	private static final int PROCESSOR_NUMBER = Runtime.getRuntime().availableProcessors();

	private String brokerConfigPath;

	private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

	@ImportantField
	private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));

	@ImportantField
	private int listPort = 6888;

	@ImportantField
	private String brokerIP1 = NetworkUtil.getLocalAddress();
	private String brokerIP2 = NetworkUtil.getLocalAddress();

	@ImportantField
	private boolean recoveryConcurrently = false;

	private int brokerPermission = PermName.PERM_READ | PermName.PERM_WRITE;
	private int defaultTopicQueueNums = 8;

	@ImportantField
	private boolean autoCreateTopicEnable = true;

	private boolean clusterTopicEnable = true;

	private boolean brokerTopicEnable = true;

	@ImportantField
	private boolean autoCreateSubscriptionGroup = true;

	private String messageStorePluginIn = "";

	@ImportantField
	private String msgTraceTopicName = TopicValidator.RMQ_SYS_TRACE_TOPIC;

	@ImportantField
	private boolean traceTopicEnable = false;

	private int sendMessageThreadPoolNums = Math.min(PROCESSOR_NUMBER, 4);
	private int putMessageFutureThreadPoolNums = Math.min(PROCESSOR_NUMBER, 4);
	private int pullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
	private int litePullMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
	private int ackMessageThreadPoolNums = 3;
	private int processReplyMessageThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
	private int queryMessageThreadPoolNums = 8 + PROCESSOR_NUMBER;

	private int adminBrokerThreadPoolNums = 16;
	private int clientManageThreadPoolNums = 32;
	private int consumerManageThreadPoolNums = 32;
	private int loadBalanceProcessorThreadPoolNums = 32;
	private int heartbeatThreadPoolNums = Math.min(PROCESSOR_NUMBER, 32);
	private int recoverThreadPoolNums = 32;

	private int endTransactionThreadPoolNums = Math.max(8 + PROCESSOR_NUMBER * 2, sendMessageThreadPoolNums * 4);

	private int flushConsumerOffsetInterval = 1000 * 5;

	private int flushConsumerOffsetHistoryInterval = 1000 * 60;

	@ImportantField
	private boolean rejectTransactionMessage = false;

	@ImportantField
	private boolean fetchNameSrvAddrByDnsLookup = false;

	@ImportantField
	private boolean fetchNamesrvAddrByAddressServer = false;

	private int sendThreadPoolQueueCapacity = 10000;
	private int putThreadPoolQueueCapacity = 10000;
	private int pullThreadPoolQueueCapacity = 100000;
	private int litePullThreadPoolQueueCapacity = 100000;
	private int ackThreadPoolQueueCapacity = 100000;
	private int replyThreadPoolQueueCapacity = 10000;
	private int queryThreadPoolQueueCapacity = 20000;
	private int clientManagerThreadPoolQueueCapacity = 1000000;
	private int consumerManagerThreadPoolQueueCapacity = 1000000;
	private int heartbeatThreadPoolQueueCapacity = 50000;
	private int endTransactionPoolQueueCapacity = 100000;
	private int adminBrokerThreadPoolQueueCapacity = 10000;
	private int loadBalanceThreadPoolQueueCapacity = 100000;

	private boolean longPollingEnable = true;

	private long shortPollingTimeMillis = 1000;

	private boolean notifyConsumerIdsChangedEnable = true;

	private boolean highSpeedMode = false;

	private int commercialBaseCount = 1;

	private int commercialSizePerMsg = 4 * 1024;

	private boolean accountStatsEnable = true;

	private boolean accountStatsPrintZeroValues = true;

	private boolean transferMsgByHeap = true;

	private String regionId = MixAll.DEFAULT_TRACE_REGION_ID;

	private int registerBrokerTimeoutMillis = 24000;

	private int sendHeartbeatTimeoutMillis = 1000;

	private boolean slaveReadEnable = false;

	private boolean disableConsumeIfConsumerReadSlowly = false;

	private long consumerFallbehindThreshold = 16 * 1024 * 1024 * 1024;

	private boolean brokerFastFailureEnable = true;
	private long waitTimeMillisInSendQueue = 200;
	private long waitTimeMillisInPullQueue = 5 * 1000;
	private long waitTimeMillisInLitePullQueue = 5 * 1000;
	private long waitTimeMillisInHeartbeatQueue = 31 * 1000;
	private long waitTimeMillisInTransactionQueue = 3 * 1000;
	private long waitTimeMillisInAckQueue = 3000;

	private long startAcceptSendRequestTimestamp = 0L;

	private boolean traceOn = true;

	private boolean enableCalcFilterBitMap = false;

	private boolean rejectPullConsumerEnable = false;

	private int expectConsumerNumUseFilter = 32;

	private int maxErrorRateOfBloomFilter = 20;

	private long filterDataCleanTimeSpan = 24 * 3600 * 1000;

	private boolean filterSupportRetry = false;

	private boolean enablePropertyFilter = false;

	private boolean compressedRegister = false;

	private boolean forceRegister = true;

	private int registerNameServerPeriod = 30 * 1000;

	private int brokerHeartbeatInterval = 1000;

	private long brokerNotActiveTimeoutMillis = 10 * 1000;

	private boolean enableNetworkFlowControl = false;

	private boolean enableBroadcastOffsetStore = true;

	private long broadcastOffsetExpireSecond = 2 * 60;

	private long broadcastOffsetExpireMaxSecond = 5 * 60;

	private int popPollingSize = 1024;
	private int popPollingMapSize = 100000;
	private long maxPopPollingSize = 100000;
	private int reviveQueueNum = 8;
	private long reviveInterval = 1000;
	private long reviveMaxSlow = 3;
	private long reviveScanTime = 10000;
	private boolean enableSkipLongAwaitingAck = false;
	private long reviveAckWaitMs = TimeUnit.MINUTES.toMillis(3);
	private boolean enablePopLog = false;
	private boolean enablePopBufferMerge = false;
	private int popCkStayBufferTime = 10 * 1000;
	private int popCkStayBufferTimeout = 3 * 1000;
	private int popCkMaxBufferSize = 200000;
	private int popCkOffsetMaxQueueSize = 20000;
	private boolean enablePopBatchAck = false;
	private boolean enableNotifyAfterPopOrderLockRelease = true;
	private boolean initPopOffsetByCheckMsgInMem = true;
	private boolean retrieveMessageFromPopRetryTopicV1 = true;
	private boolean enableRetryTopicV2 = false;

	private boolean realTimeNotifyConsumerChange = true;

	private boolean litePullMessageEnable = true;

	private int syncBrokerMemberGroupPeriod = 1000;

	private long loadBalancePollNameServerInterval = 30 * 1000;

	private int cleanOfflineBrokerInterval = 30 * 1000;

	private boolean serverLoadBalancerEnable = true;

	private MessageRequestMode defaultMessageRequestMode = MessageRequestMode.PULL;

	private int defaultPopShareQueueNum = -1;

	@ImportantField
	private long transactionTimeOut = 6 * 1000;

	@ImportantField
	private int transactionCheckMax = 15;

	@ImportantField
	private long transactionCheckInterval = 30 * 1000;

	private long transactionMetricFlushInterval = 3 * 1000;

	private int transactionOpMsgMaxSize = 4096;

	private int transactionOpBatchInterval = 3000;

	@ImportantField
	private boolean aclEnable = false;

	private boolean storeReplyMessageEnable = true;

	private boolean enableDetailStat = true;

	private boolean autoDeleteUnusedStats = false;

	private boolean isolateLogEnable = false;

	private long forwardTimeout = 3 * 1000;

	private boolean enableSlaveActingMaster = false;

	private boolean enableRemoteEscape = false;

	private boolean skipPerOnline = false;

	private boolean asyncSendEnable = true;

	private boolean useServerSideResetOffset = true;

	private long consumerOffsetUpdateVersionStep = 500;

	private long delayOffsetUpdateVersionStep = 200;

	private boolean lockInStrictMode=  false;

	private boolean compatibleWithOldNameSrv = true;

	private boolean enableControllerMode = false;

	private String controllerAddr = "";

	private boolean fetchControllerAddrByDnsLookup = false;

	private long syncBrokerMetadataPeriod = 5 * 1000;

	private long checkSyncStateSetPeriod = 5 * 1000;

	private long syncControllerMetadataPeriod = 10 * 1000;

	private long controllerHeartBeatTimeoutMillis = 10 * 1000;

	private boolean validateSystemTopicWhenUpdateTopic = true;

	private int brokerElectionPriority = Integer.MAX_VALUE;

	private boolean useStaticSubscription = false;

	private MetricsExporterType metricsExporterType = MetricsExporterType.DISABLE;

	private int metricsOtelCardinalityLimit = 50 * 1000;
	private String metricsGrpcExporterTarget = "";
	private String metricsGrpcExporterHeader = "";
	private long metricGrpcExporterTimeoutInMillis = 3 * 1000;
	private long metricGrpcExporterIntervalInMillis = 60 * 1000;
	private long metricLoggingExporterIntervalInMillis = 10 * 1000;

	private int metricsPromExporterPort = 5557;
	private String metricsPromExporterHost = "";

	private String metricsLabel = "";

	private boolean metricsInDelta = false;

	private long channelExpiredTimeout = 120 * 1000;
	private long subscriptionExpiredTimeout = 10 * 60 * 1000;

	private boolean estimateAccumulation = true;

	private boolean coldStrStrategyEnable = false;
	private boolean usePIDColdCtrStrategy = true;
	private long cgColdReadThreshold = 3 * 1024 * 1024;
	private long globalColdReadThreshold = 100 * 1024 * 1024;

	private long fetchNamesrvAddrInterval = 10 * 1000;

	private boolean popResponseReturnActualRetryTopic = false;

	private boolean enableSingleTopicRegister = false;

	private boolean enableMaxedMessageType = false;

	private boolean enableSplitRegistration = false;

	private long popInflighMessageThreshold = 10000;

	private boolean enablePopMessageThreshold = false;

	private int splitRegistrationSize = 800;

	private String configBlackList = "configBlackList;brokerConfigPath";

	public String getBrokerConfigPath() {
		return brokerConfigPath;
	}

	public void setBrokerConfigPath(String brokerConfigPath) {
		this.brokerConfigPath = brokerConfigPath;
	}

	public String getRocketmqHome() {
		return rocketmqHome;
	}

	public void setRocketmqHome(String rocketmqHome) {
		this.rocketmqHome = rocketmqHome;
	}

	public String getNamesrvAddr() {
		return namesrvAddr;
	}

	public void setNamesrvAddr(String namesrvAddr) {
		this.namesrvAddr = namesrvAddr;
	}

	public int getListPort() {
		return listPort;
	}

	public void setListPort(int listPort) {
		this.listPort = listPort;
	}

	public String getBrokerIP1() {
		return brokerIP1;
	}

	public void setBrokerIP1(String brokerIP1) {
		this.brokerIP1 = brokerIP1;
	}

	public String getBrokerIP2() {
		return brokerIP2;
	}

	public void setBrokerIP2(String brokerIP2) {
		this.brokerIP2 = brokerIP2;
	}

	public boolean isRecoveryConcurrently() {
		return recoveryConcurrently;
	}

	public void setRecoveryConcurrently(boolean recoveryConcurrently) {
		this.recoveryConcurrently = recoveryConcurrently;
	}

	public int getBrokerPermission() {
		return brokerPermission;
	}

	public void setBrokerPermission(int brokerPermission) {
		this.brokerPermission = brokerPermission;
	}

	public int getDefaultTopicQueueNums() {
		return defaultTopicQueueNums;
	}

	public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
		this.defaultTopicQueueNums = defaultTopicQueueNums;
	}

	public boolean isAutoCreateTopicEnable() {
		return autoCreateTopicEnable;
	}

	public void setAutoCreateTopicEnable(boolean autoCreateTopicEnable) {
		this.autoCreateTopicEnable = autoCreateTopicEnable;
	}

	public boolean isClusterTopicEnable() {
		return clusterTopicEnable;
	}

	public void setClusterTopicEnable(boolean clusterTopicEnable) {
		this.clusterTopicEnable = clusterTopicEnable;
	}

	public boolean isBrokerTopicEnable() {
		return brokerTopicEnable;
	}

	public void setBrokerTopicEnable(boolean brokerTopicEnable) {
		this.brokerTopicEnable = brokerTopicEnable;
	}

	public boolean isAutoCreateSubscriptionGroup() {
		return autoCreateSubscriptionGroup;
	}

	public void setAutoCreateSubscriptionGroup(boolean autoCreateSubscriptionGroup) {
		this.autoCreateSubscriptionGroup = autoCreateSubscriptionGroup;
	}

	public String getMessageStorePluginIn() {
		return messageStorePluginIn;
	}

	public void setMessageStorePluginIn(String messageStorePluginIn) {
		this.messageStorePluginIn = messageStorePluginIn;
	}

	public String getMsgTraceTopicName() {
		return msgTraceTopicName;
	}

	public void setMsgTraceTopicName(String msgTraceTopicName) {
		this.msgTraceTopicName = msgTraceTopicName;
	}

	public boolean isTraceTopicEnable() {
		return traceTopicEnable;
	}

	public void setTraceTopicEnable(boolean traceTopicEnable) {
		this.traceTopicEnable = traceTopicEnable;
	}

	public int getSendMessageThreadPoolNums() {
		return sendMessageThreadPoolNums;
	}

	public void setSendMessageThreadPoolNums(int sendMessageThreadPoolNums) {
		this.sendMessageThreadPoolNums = sendMessageThreadPoolNums;
	}

	public int getPutMessageFutureThreadPoolNums() {
		return putMessageFutureThreadPoolNums;
	}

	public void setPutMessageFutureThreadPoolNums(int putMessageFutureThreadPoolNums) {
		this.putMessageFutureThreadPoolNums = putMessageFutureThreadPoolNums;
	}

	public int getPullMessageThreadPoolNums() {
		return pullMessageThreadPoolNums;
	}

	public void setPullMessageThreadPoolNums(int pullMessageThreadPoolNums) {
		this.pullMessageThreadPoolNums = pullMessageThreadPoolNums;
	}

	public int getLitePullMessageThreadPoolNums() {
		return litePullMessageThreadPoolNums;
	}

	public void setLitePullMessageThreadPoolNums(int litePullMessageThreadPoolNums) {
		this.litePullMessageThreadPoolNums = litePullMessageThreadPoolNums;
	}

	public int getAckMessageThreadPoolNums() {
		return ackMessageThreadPoolNums;
	}

	public void setAckMessageThreadPoolNums(int ackMessageThreadPoolNums) {
		this.ackMessageThreadPoolNums = ackMessageThreadPoolNums;
	}

	public int getProcessReplyMessageThreadPoolNums() {
		return processReplyMessageThreadPoolNums;
	}

	public void setProcessReplyMessageThreadPoolNums(int processReplyMessageThreadPoolNums) {
		this.processReplyMessageThreadPoolNums = processReplyMessageThreadPoolNums;
	}

	public int getQueryMessageThreadPoolNums() {
		return queryMessageThreadPoolNums;
	}

	public void setQueryMessageThreadPoolNums(int queryMessageThreadPoolNums) {
		this.queryMessageThreadPoolNums = queryMessageThreadPoolNums;
	}

	public int getAdminBrokerThreadPoolNums() {
		return adminBrokerThreadPoolNums;
	}

	public void setAdminBrokerThreadPoolNums(int adminBrokerThreadPoolNums) {
		this.adminBrokerThreadPoolNums = adminBrokerThreadPoolNums;
	}

	public int getClientManageThreadPoolNums() {
		return clientManageThreadPoolNums;
	}

	public void setClientManageThreadPoolNums(int clientManageThreadPoolNums) {
		this.clientManageThreadPoolNums = clientManageThreadPoolNums;
	}

	public int getConsumerManageThreadPoolNums() {
		return consumerManageThreadPoolNums;
	}

	public void setConsumerManageThreadPoolNums(int consumerManageThreadPoolNums) {
		this.consumerManageThreadPoolNums = consumerManageThreadPoolNums;
	}

	public int getLoadBalanceProcessorThreadPoolNums() {
		return loadBalanceProcessorThreadPoolNums;
	}

	public void setLoadBalanceProcessorThreadPoolNums(int loadBalanceProcessorThreadPoolNums) {
		this.loadBalanceProcessorThreadPoolNums = loadBalanceProcessorThreadPoolNums;
	}

	public int getHeartbeatThreadPoolNums() {
		return heartbeatThreadPoolNums;
	}

	public void setHeartbeatThreadPoolNums(int heartbeatThreadPoolNums) {
		this.heartbeatThreadPoolNums = heartbeatThreadPoolNums;
	}

	public int getRecoverThreadPoolNums() {
		return recoverThreadPoolNums;
	}

	public void setRecoverThreadPoolNums(int recoverThreadPoolNums) {
		this.recoverThreadPoolNums = recoverThreadPoolNums;
	}

	public int getEndTransactionThreadPoolNums() {
		return endTransactionThreadPoolNums;
	}

	public void setEndTransactionThreadPoolNums(int endTransactionThreadPoolNums) {
		this.endTransactionThreadPoolNums = endTransactionThreadPoolNums;
	}

	public int getFlushConsumerOffsetInterval() {
		return flushConsumerOffsetInterval;
	}

	public void setFlushConsumerOffsetInterval(int flushConsumerOffsetInterval) {
		this.flushConsumerOffsetInterval = flushConsumerOffsetInterval;
	}

	public int getFlushConsumerOffsetHistoryInterval() {
		return flushConsumerOffsetHistoryInterval;
	}

	public void setFlushConsumerOffsetHistoryInterval(int flushConsumerOffsetHistoryInterval) {
		this.flushConsumerOffsetHistoryInterval = flushConsumerOffsetHistoryInterval;
	}

	public boolean isRejectTransactionMessage() {
		return rejectTransactionMessage;
	}

	public void setRejectTransactionMessage(boolean rejectTransactionMessage) {
		this.rejectTransactionMessage = rejectTransactionMessage;
	}

	public boolean isFetchNameSrvAddrByDnsLookup() {
		return fetchNameSrvAddrByDnsLookup;
	}

	public void setFetchNameSrvAddrByDnsLookup(boolean fetchNameSrvAddrByDnsLookup) {
		this.fetchNameSrvAddrByDnsLookup = fetchNameSrvAddrByDnsLookup;
	}

	public boolean isFetchNamesrvAddrByAddressServer() {
		return fetchNamesrvAddrByAddressServer;
	}

	public void setFetchNamesrvAddrByAddressServer(boolean fetchNamesrvAddrByAddressServer) {
		this.fetchNamesrvAddrByAddressServer = fetchNamesrvAddrByAddressServer;
	}

	public int getSendThreadPoolQueueCapacity() {
		return sendThreadPoolQueueCapacity;
	}

	public void setSendThreadPoolQueueCapacity(int sendThreadPoolQueueCapacity) {
		this.sendThreadPoolQueueCapacity = sendThreadPoolQueueCapacity;
	}

	public int getPutThreadPoolQueueCapacity() {
		return putThreadPoolQueueCapacity;
	}

	public void setPutThreadPoolQueueCapacity(int putThreadPoolQueueCapacity) {
		this.putThreadPoolQueueCapacity = putThreadPoolQueueCapacity;
	}

	public int getPullThreadPoolQueueCapacity() {
		return pullThreadPoolQueueCapacity;
	}

	public void setPullThreadPoolQueueCapacity(int pullThreadPoolQueueCapacity) {
		this.pullThreadPoolQueueCapacity = pullThreadPoolQueueCapacity;
	}

	public int getLitePullThreadPoolQueueCapacity() {
		return litePullThreadPoolQueueCapacity;
	}

	public void setLitePullThreadPoolQueueCapacity(int litePullThreadPoolQueueCapacity) {
		this.litePullThreadPoolQueueCapacity = litePullThreadPoolQueueCapacity;
	}

	public int getAckThreadPoolQueueCapacity() {
		return ackThreadPoolQueueCapacity;
	}

	public void setAckThreadPoolQueueCapacity(int ackThreadPoolQueueCapacity) {
		this.ackThreadPoolQueueCapacity = ackThreadPoolQueueCapacity;
	}

	public int getReplyThreadPoolQueueCapacity() {
		return replyThreadPoolQueueCapacity;
	}

	public void setReplyThreadPoolQueueCapacity(int replyThreadPoolQueueCapacity) {
		this.replyThreadPoolQueueCapacity = replyThreadPoolQueueCapacity;
	}

	public int getQueryThreadPoolQueueCapacity() {
		return queryThreadPoolQueueCapacity;
	}

	public void setQueryThreadPoolQueueCapacity(int queryThreadPoolQueueCapacity) {
		this.queryThreadPoolQueueCapacity = queryThreadPoolQueueCapacity;
	}

	public int getClientManagerThreadPoolQueueCapacity() {
		return clientManagerThreadPoolQueueCapacity;
	}

	public void setClientManagerThreadPoolQueueCapacity(int clientManagerThreadPoolQueueCapacity) {
		this.clientManagerThreadPoolQueueCapacity = clientManagerThreadPoolQueueCapacity;
	}

	public int getConsumerManagerThreadPoolQueueCapacity() {
		return consumerManagerThreadPoolQueueCapacity;
	}

	public void setConsumerManagerThreadPoolQueueCapacity(int consumerManagerThreadPoolQueueCapacity) {
		this.consumerManagerThreadPoolQueueCapacity = consumerManagerThreadPoolQueueCapacity;
	}

	public int getHeartbeatThreadPoolQueueCapacity() {
		return heartbeatThreadPoolQueueCapacity;
	}

	public void setHeartbeatThreadPoolQueueCapacity(int heartbeatThreadPoolQueueCapacity) {
		this.heartbeatThreadPoolQueueCapacity = heartbeatThreadPoolQueueCapacity;
	}

	public int getEndTransactionPoolQueueCapacity() {
		return endTransactionPoolQueueCapacity;
	}

	public void setEndTransactionPoolQueueCapacity(int endTransactionPoolQueueCapacity) {
		this.endTransactionPoolQueueCapacity = endTransactionPoolQueueCapacity;
	}

	public int getAdminBrokerThreadPoolQueueCapacity() {
		return adminBrokerThreadPoolQueueCapacity;
	}

	public void setAdminBrokerThreadPoolQueueCapacity(int adminBrokerThreadPoolQueueCapacity) {
		this.adminBrokerThreadPoolQueueCapacity = adminBrokerThreadPoolQueueCapacity;
	}

	public int getLoadBalanceThreadPoolQueueCapacity() {
		return loadBalanceThreadPoolQueueCapacity;
	}

	public void setLoadBalanceThreadPoolQueueCapacity(int loadBalanceThreadPoolQueueCapacity) {
		this.loadBalanceThreadPoolQueueCapacity = loadBalanceThreadPoolQueueCapacity;
	}

	public boolean isLongPollingEnable() {
		return longPollingEnable;
	}

	public void setLongPollingEnable(boolean longPollingEnable) {
		this.longPollingEnable = longPollingEnable;
	}

	public long getShortPollingTimeMillis() {
		return shortPollingTimeMillis;
	}

	public void setShortPollingTimeMillis(long shortPollingTimeMillis) {
		this.shortPollingTimeMillis = shortPollingTimeMillis;
	}

	public boolean isNotifyConsumerIdsChangedEnable() {
		return notifyConsumerIdsChangedEnable;
	}

	public void setNotifyConsumerIdsChangedEnable(boolean notifyConsumerIdsChangedEnable) {
		this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
	}

	public boolean isHighSpeedMode() {
		return highSpeedMode;
	}

	public void setHighSpeedMode(boolean highSpeedMode) {
		this.highSpeedMode = highSpeedMode;
	}

	public int getCommercialBaseCount() {
		return commercialBaseCount;
	}

	public void setCommercialBaseCount(int commercialBaseCount) {
		this.commercialBaseCount = commercialBaseCount;
	}

	public int getCommercialSizePerMsg() {
		return commercialSizePerMsg;
	}

	public void setCommercialSizePerMsg(int commercialSizePerMsg) {
		this.commercialSizePerMsg = commercialSizePerMsg;
	}

	public boolean isAccountStatsEnable() {
		return accountStatsEnable;
	}

	public void setAccountStatsEnable(boolean accountStatsEnable) {
		this.accountStatsEnable = accountStatsEnable;
	}

	public boolean isAccountStatsPrintZeroValues() {
		return accountStatsPrintZeroValues;
	}

	public void setAccountStatsPrintZeroValues(boolean accountStatsPrintZeroValues) {
		this.accountStatsPrintZeroValues = accountStatsPrintZeroValues;
	}

	public boolean isTransferMsgByHeap() {
		return transferMsgByHeap;
	}

	public void setTransferMsgByHeap(boolean transferMsgByHeap) {
		this.transferMsgByHeap = transferMsgByHeap;
	}

	public String getRegionId() {
		return regionId;
	}

	public void setRegionId(String regionId) {
		this.regionId = regionId;
	}

	public int getRegisterBrokerTimeoutMillis() {
		return registerBrokerTimeoutMillis;
	}

	public void setRegisterBrokerTimeoutMillis(int registerBrokerTimeoutMillis) {
		this.registerBrokerTimeoutMillis = registerBrokerTimeoutMillis;
	}

	public int getSendHeartbeatTimeoutMillis() {
		return sendHeartbeatTimeoutMillis;
	}

	public void setSendHeartbeatTimeoutMillis(int sendHeartbeatTimeoutMillis) {
		this.sendHeartbeatTimeoutMillis = sendHeartbeatTimeoutMillis;
	}

	public boolean isSlaveReadEnable() {
		return slaveReadEnable;
	}

	public void setSlaveReadEnable(boolean slaveReadEnable) {
		this.slaveReadEnable = slaveReadEnable;
	}

	public boolean isDisableConsumeIfConsumerReadSlowly() {
		return disableConsumeIfConsumerReadSlowly;
	}

	public void setDisableConsumeIfConsumerReadSlowly(boolean disableConsumeIfConsumerReadSlowly) {
		this.disableConsumeIfConsumerReadSlowly = disableConsumeIfConsumerReadSlowly;
	}

	public long getConsumerFallbehindThreshold() {
		return consumerFallbehindThreshold;
	}

	public void setConsumerFallbehindThreshold(long consumerFallbehindThreshold) {
		this.consumerFallbehindThreshold = consumerFallbehindThreshold;
	}

	public boolean isBrokerFastFailureEnable() {
		return brokerFastFailureEnable;
	}

	public void setBrokerFastFailureEnable(boolean brokerFastFailureEnable) {
		this.brokerFastFailureEnable = brokerFastFailureEnable;
	}

	public long getWaitTimeMillisInSendQueue() {
		return waitTimeMillisInSendQueue;
	}

	public void setWaitTimeMillisInSendQueue(long waitTimeMillisInSendQueue) {
		this.waitTimeMillisInSendQueue = waitTimeMillisInSendQueue;
	}

	public long getWaitTimeMillisInPullQueue() {
		return waitTimeMillisInPullQueue;
	}

	public void setWaitTimeMillisInPullQueue(long waitTimeMillisInPullQueue) {
		this.waitTimeMillisInPullQueue = waitTimeMillisInPullQueue;
	}

	public long getWaitTimeMillisInLitePullQueue() {
		return waitTimeMillisInLitePullQueue;
	}

	public void setWaitTimeMillisInLitePullQueue(long waitTimeMillisInLitePullQueue) {
		this.waitTimeMillisInLitePullQueue = waitTimeMillisInLitePullQueue;
	}

	public long getWaitTimeMillisInHeartbeatQueue() {
		return waitTimeMillisInHeartbeatQueue;
	}

	public void setWaitTimeMillisInHeartbeatQueue(long waitTimeMillisInHeartbeatQueue) {
		this.waitTimeMillisInHeartbeatQueue = waitTimeMillisInHeartbeatQueue;
	}

	public long getWaitTimeMillisInTransactionQueue() {
		return waitTimeMillisInTransactionQueue;
	}

	public void setWaitTimeMillisInTransactionQueue(long waitTimeMillisInTransactionQueue) {
		this.waitTimeMillisInTransactionQueue = waitTimeMillisInTransactionQueue;
	}

	public long getWaitTimeMillisInAckQueue() {
		return waitTimeMillisInAckQueue;
	}

	public void setWaitTimeMillisInAckQueue(long waitTimeMillisInAckQueue) {
		this.waitTimeMillisInAckQueue = waitTimeMillisInAckQueue;
	}

	public long getStartAcceptSendRequestTimestamp() {
		return startAcceptSendRequestTimestamp;
	}

	public void setStartAcceptSendRequestTimestamp(long startAcceptSendRequestTimestamp) {
		this.startAcceptSendRequestTimestamp = startAcceptSendRequestTimestamp;
	}

	public boolean isTraceOn() {
		return traceOn;
	}

	public void setTraceOn(boolean traceOn) {
		this.traceOn = traceOn;
	}

	public boolean isEnableCalcFilterBitMap() {
		return enableCalcFilterBitMap;
	}

	public void setEnableCalcFilterBitMap(boolean enableCalcFilterBitMap) {
		this.enableCalcFilterBitMap = enableCalcFilterBitMap;
	}

	public boolean isRejectPullConsumerEnable() {
		return rejectPullConsumerEnable;
	}

	public void setRejectPullConsumerEnable(boolean rejectPullConsumerEnable) {
		this.rejectPullConsumerEnable = rejectPullConsumerEnable;
	}

	public int getExpectConsumerNumUseFilter() {
		return expectConsumerNumUseFilter;
	}

	public void setExpectConsumerNumUseFilter(int expectConsumerNumUseFilter) {
		this.expectConsumerNumUseFilter = expectConsumerNumUseFilter;
	}

	public int getMaxErrorRateOfBloomFilter() {
		return maxErrorRateOfBloomFilter;
	}

	public void setMaxErrorRateOfBloomFilter(int maxErrorRateOfBloomFilter) {
		this.maxErrorRateOfBloomFilter = maxErrorRateOfBloomFilter;
	}

	public long getFilterDataCleanTimeSpan() {
		return filterDataCleanTimeSpan;
	}

	public void setFilterDataCleanTimeSpan(long filterDataCleanTimeSpan) {
		this.filterDataCleanTimeSpan = filterDataCleanTimeSpan;
	}

	public boolean isFilterSupportRetry() {
		return filterSupportRetry;
	}

	public void setFilterSupportRetry(boolean filterSupportRetry) {
		this.filterSupportRetry = filterSupportRetry;
	}

	public boolean isEnablePropertyFilter() {
		return enablePropertyFilter;
	}

	public void setEnablePropertyFilter(boolean enablePropertyFilter) {
		this.enablePropertyFilter = enablePropertyFilter;
	}

	public boolean isCompressedRegister() {
		return compressedRegister;
	}

	public void setCompressedRegister(boolean compressedRegister) {
		this.compressedRegister = compressedRegister;
	}

	public boolean isForceRegister() {
		return forceRegister;
	}

	public void setForceRegister(boolean forceRegister) {
		this.forceRegister = forceRegister;
	}

	public int getRegisterNameServerPeriod() {
		return registerNameServerPeriod;
	}

	public void setRegisterNameServerPeriod(int registerNameServerPeriod) {
		this.registerNameServerPeriod = registerNameServerPeriod;
	}

	public int getBrokerHeartbeatInterval() {
		return brokerHeartbeatInterval;
	}

	public void setBrokerHeartbeatInterval(int brokerHeartbeatInterval) {
		this.brokerHeartbeatInterval = brokerHeartbeatInterval;
	}

	public long getBrokerNotActiveTimeoutMillis() {
		return brokerNotActiveTimeoutMillis;
	}

	public void setBrokerNotActiveTimeoutMillis(long brokerNotActiveTimeoutMillis) {
		this.brokerNotActiveTimeoutMillis = brokerNotActiveTimeoutMillis;
	}

	public boolean isEnableNetworkFlowControl() {
		return enableNetworkFlowControl;
	}

	public void setEnableNetworkFlowControl(boolean enableNetworkFlowControl) {
		this.enableNetworkFlowControl = enableNetworkFlowControl;
	}

	public boolean isEnableBroadcastOffsetStore() {
		return enableBroadcastOffsetStore;
	}

	public void setEnableBroadcastOffsetStore(boolean enableBroadcastOffsetStore) {
		this.enableBroadcastOffsetStore = enableBroadcastOffsetStore;
	}

	public long getBroadcastOffsetExpireSecond() {
		return broadcastOffsetExpireSecond;
	}

	public void setBroadcastOffsetExpireSecond(long broadcastOffsetExpireSecond) {
		this.broadcastOffsetExpireSecond = broadcastOffsetExpireSecond;
	}

	public long getBroadcastOffsetExpireMaxSecond() {
		return broadcastOffsetExpireMaxSecond;
	}

	public void setBroadcastOffsetExpireMaxSecond(long broadcastOffsetExpireMaxSecond) {
		this.broadcastOffsetExpireMaxSecond = broadcastOffsetExpireMaxSecond;
	}

	public int getPopPollingSize() {
		return popPollingSize;
	}

	public void setPopPollingSize(int popPollingSize) {
		this.popPollingSize = popPollingSize;
	}

	public int getPopPollingMapSize() {
		return popPollingMapSize;
	}

	public void setPopPollingMapSize(int popPollingMapSize) {
		this.popPollingMapSize = popPollingMapSize;
	}

	public long getMaxPopPollingSize() {
		return maxPopPollingSize;
	}

	public void setMaxPopPollingSize(long maxPopPollingSize) {
		this.maxPopPollingSize = maxPopPollingSize;
	}

	public int getReviveQueueNum() {
		return reviveQueueNum;
	}

	public void setReviveQueueNum(int reviveQueueNum) {
		this.reviveQueueNum = reviveQueueNum;
	}

	public long getReviveInterval() {
		return reviveInterval;
	}

	public void setReviveInterval(long reviveInterval) {
		this.reviveInterval = reviveInterval;
	}

	public long getReviveMaxSlow() {
		return reviveMaxSlow;
	}

	public void setReviveMaxSlow(long reviveMaxSlow) {
		this.reviveMaxSlow = reviveMaxSlow;
	}

	public long getReviveScanTime() {
		return reviveScanTime;
	}

	public void setReviveScanTime(long reviveScanTime) {
		this.reviveScanTime = reviveScanTime;
	}

	public boolean isEnableSkipLongAwaitingAck() {
		return enableSkipLongAwaitingAck;
	}

	public void setEnableSkipLongAwaitingAck(boolean enableSkipLongAwaitingAck) {
		this.enableSkipLongAwaitingAck = enableSkipLongAwaitingAck;
	}

	public long getReviveAckWaitMs() {
		return reviveAckWaitMs;
	}

	public void setReviveAckWaitMs(long reviveAckWaitMs) {
		this.reviveAckWaitMs = reviveAckWaitMs;
	}

	public boolean isEnablePopLog() {
		return enablePopLog;
	}

	public void setEnablePopLog(boolean enablePopLog) {
		this.enablePopLog = enablePopLog;
	}

	public boolean isEnablePopBufferMerge() {
		return enablePopBufferMerge;
	}

	public void setEnablePopBufferMerge(boolean enablePopBufferMerge) {
		this.enablePopBufferMerge = enablePopBufferMerge;
	}

	public int getPopCkStayBufferTime() {
		return popCkStayBufferTime;
	}

	public void setPopCkStayBufferTime(int popCkStayBufferTime) {
		this.popCkStayBufferTime = popCkStayBufferTime;
	}

	public int getPopCkStayBufferTimeout() {
		return popCkStayBufferTimeout;
	}

	public void setPopCkStayBufferTimeout(int popCkStayBufferTimeout) {
		this.popCkStayBufferTimeout = popCkStayBufferTimeout;
	}

	public int getPopCkMaxBufferSize() {
		return popCkMaxBufferSize;
	}

	public void setPopCkMaxBufferSize(int popCkMaxBufferSize) {
		this.popCkMaxBufferSize = popCkMaxBufferSize;
	}

	public int getPopCkOffsetMaxQueueSize() {
		return popCkOffsetMaxQueueSize;
	}

	public void setPopCkOffsetMaxQueueSize(int popCkOffsetMaxQueueSize) {
		this.popCkOffsetMaxQueueSize = popCkOffsetMaxQueueSize;
	}

	public boolean isEnablePopBatchAck() {
		return enablePopBatchAck;
	}

	public void setEnablePopBatchAck(boolean enablePopBatchAck) {
		this.enablePopBatchAck = enablePopBatchAck;
	}

	public boolean isEnableNotifyAfterPopOrderLockRelease() {
		return enableNotifyAfterPopOrderLockRelease;
	}

	public void setEnableNotifyAfterPopOrderLockRelease(boolean enableNotifyAfterPopOrderLockRelease) {
		this.enableNotifyAfterPopOrderLockRelease = enableNotifyAfterPopOrderLockRelease;
	}

	public boolean isInitPopOffsetByCheckMsgInMem() {
		return initPopOffsetByCheckMsgInMem;
	}

	public void setInitPopOffsetByCheckMsgInMem(boolean initPopOffsetByCheckMsgInMem) {
		this.initPopOffsetByCheckMsgInMem = initPopOffsetByCheckMsgInMem;
	}

	public boolean isRetrieveMessageFromPopRetryTopicV1() {
		return retrieveMessageFromPopRetryTopicV1;
	}

	public void setRetrieveMessageFromPopRetryTopicV1(boolean retrieveMessageFromPopRetryTopicV1) {
		this.retrieveMessageFromPopRetryTopicV1 = retrieveMessageFromPopRetryTopicV1;
	}

	public boolean isEnableRetryTopicV2() {
		return enableRetryTopicV2;
	}

	public void setEnableRetryTopicV2(boolean enableRetryTopicV2) {
		this.enableRetryTopicV2 = enableRetryTopicV2;
	}

	public boolean isRealTimeNotifyConsumerChange() {
		return realTimeNotifyConsumerChange;
	}

	public void setRealTimeNotifyConsumerChange(boolean realTimeNotifyConsumerChange) {
		this.realTimeNotifyConsumerChange = realTimeNotifyConsumerChange;
	}

	public boolean isLitePullMessageEnable() {
		return litePullMessageEnable;
	}

	public void setLitePullMessageEnable(boolean litePullMessageEnable) {
		this.litePullMessageEnable = litePullMessageEnable;
	}

	public int getSyncBrokerMemberGroupPeriod() {
		return syncBrokerMemberGroupPeriod;
	}

	public void setSyncBrokerMemberGroupPeriod(int syncBrokerMemberGroupPeriod) {
		this.syncBrokerMemberGroupPeriod = syncBrokerMemberGroupPeriod;
	}

	public long getLoadBalancePollNameServerInterval() {
		return loadBalancePollNameServerInterval;
	}

	public void setLoadBalancePollNameServerInterval(long loadBalancePollNameServerInterval) {
		this.loadBalancePollNameServerInterval = loadBalancePollNameServerInterval;
	}

	public int getCleanOfflineBrokerInterval() {
		return cleanOfflineBrokerInterval;
	}

	public void setCleanOfflineBrokerInterval(int cleanOfflineBrokerInterval) {
		this.cleanOfflineBrokerInterval = cleanOfflineBrokerInterval;
	}

	public boolean isServerLoadBalancerEnable() {
		return serverLoadBalancerEnable;
	}

	public void setServerLoadBalancerEnable(boolean serverLoadBalancerEnable) {
		this.serverLoadBalancerEnable = serverLoadBalancerEnable;
	}

	public MessageRequestMode getDefaultMessageRequestMode() {
		return defaultMessageRequestMode;
	}

	public void setDefaultMessageRequestMode(MessageRequestMode defaultMessageRequestMode) {
		this.defaultMessageRequestMode = defaultMessageRequestMode;
	}

	public int getDefaultPopShareQueueNum() {
		return defaultPopShareQueueNum;
	}

	public void setDefaultPopShareQueueNum(int defaultPopShareQueueNum) {
		this.defaultPopShareQueueNum = defaultPopShareQueueNum;
	}

	public long getTransactionTimeOut() {
		return transactionTimeOut;
	}

	public void setTransactionTimeOut(long transactionTimeOut) {
		this.transactionTimeOut = transactionTimeOut;
	}

	public int getTransactionCheckMax() {
		return transactionCheckMax;
	}

	public void setTransactionCheckMax(int transactionCheckMax) {
		this.transactionCheckMax = transactionCheckMax;
	}

	public long getTransactionCheckInterval() {
		return transactionCheckInterval;
	}

	public void setTransactionCheckInterval(long transactionCheckInterval) {
		this.transactionCheckInterval = transactionCheckInterval;
	}

	public long getTransactionMetricFlushInterval() {
		return transactionMetricFlushInterval;
	}

	public void setTransactionMetricFlushInterval(long transactionMetricFlushInterval) {
		this.transactionMetricFlushInterval = transactionMetricFlushInterval;
	}

	public int getTransactionOpMsgMaxSize() {
		return transactionOpMsgMaxSize;
	}

	public void setTransactionOpMsgMaxSize(int transactionOpMsgMaxSize) {
		this.transactionOpMsgMaxSize = transactionOpMsgMaxSize;
	}

	public int getTransactionOpBatchInterval() {
		return transactionOpBatchInterval;
	}

	public void setTransactionOpBatchInterval(int transactionOpBatchInterval) {
		this.transactionOpBatchInterval = transactionOpBatchInterval;
	}

	public boolean isAclEnable() {
		return aclEnable;
	}

	public void setAclEnable(boolean aclEnable) {
		this.aclEnable = aclEnable;
	}

	public boolean isStoreReplyMessageEnable() {
		return storeReplyMessageEnable;
	}

	public void setStoreReplyMessageEnable(boolean storeReplyMessageEnable) {
		this.storeReplyMessageEnable = storeReplyMessageEnable;
	}

	public boolean isEnableDetailStat() {
		return enableDetailStat;
	}

	public void setEnableDetailStat(boolean enableDetailStat) {
		this.enableDetailStat = enableDetailStat;
	}

	public boolean isAutoDeleteUnusedStats() {
		return autoDeleteUnusedStats;
	}

	public void setAutoDeleteUnusedStats(boolean autoDeleteUnusedStats) {
		this.autoDeleteUnusedStats = autoDeleteUnusedStats;
	}

	public boolean isIsolateLogEnable() {
		return isolateLogEnable;
	}

	public void setIsolateLogEnable(boolean isolateLogEnable) {
		this.isolateLogEnable = isolateLogEnable;
	}

	public long getForwardTimeout() {
		return forwardTimeout;
	}

	public void setForwardTimeout(long forwardTimeout) {
		this.forwardTimeout = forwardTimeout;
	}

	public boolean isEnableSlaveActingMaster() {
		return enableSlaveActingMaster;
	}

	public void setEnableSlaveActingMaster(boolean enableSlaveActingMaster) {
		this.enableSlaveActingMaster = enableSlaveActingMaster;
	}

	public boolean isEnableRemoteEscape() {
		return enableRemoteEscape;
	}

	public void setEnableRemoteEscape(boolean enableRemoteEscape) {
		this.enableRemoteEscape = enableRemoteEscape;
	}

	public boolean isSkipPerOnline() {
		return skipPerOnline;
	}

	public void setSkipPerOnline(boolean skipPerOnline) {
		this.skipPerOnline = skipPerOnline;
	}

	public boolean isAsyncSendEnable() {
		return asyncSendEnable;
	}

	public void setAsyncSendEnable(boolean asyncSendEnable) {
		this.asyncSendEnable = asyncSendEnable;
	}

	public boolean isUseServerSideResetOffset() {
		return useServerSideResetOffset;
	}

	public void setUseServerSideResetOffset(boolean useServerSideResetOffset) {
		this.useServerSideResetOffset = useServerSideResetOffset;
	}

	public long getConsumerOffsetUpdateVersionStep() {
		return consumerOffsetUpdateVersionStep;
	}

	public void setConsumerOffsetUpdateVersionStep(long consumerOffsetUpdateVersionStep) {
		this.consumerOffsetUpdateVersionStep = consumerOffsetUpdateVersionStep;
	}

	public long getDelayOffsetUpdateVersionStep() {
		return delayOffsetUpdateVersionStep;
	}

	public void setDelayOffsetUpdateVersionStep(long delayOffsetUpdateVersionStep) {
		this.delayOffsetUpdateVersionStep = delayOffsetUpdateVersionStep;
	}

	public boolean isLockInStrictMode() {
		return lockInStrictMode;
	}

	public void setLockInStrictMode(boolean lockInStrictMode) {
		this.lockInStrictMode = lockInStrictMode;
	}

	public boolean isCompatibleWithOldNameSrv() {
		return compatibleWithOldNameSrv;
	}

	public void setCompatibleWithOldNameSrv(boolean compatibleWithOldNameSrv) {
		this.compatibleWithOldNameSrv = compatibleWithOldNameSrv;
	}

	public boolean isEnableControllerMode() {
		return enableControllerMode;
	}

	public void setEnableControllerMode(boolean enableControllerMode) {
		this.enableControllerMode = enableControllerMode;
	}

	public String getControllerAddr() {
		return controllerAddr;
	}

	public void setControllerAddr(String controllerAddr) {
		this.controllerAddr = controllerAddr;
	}

	public boolean isFetchControllerAddrByDnsLookup() {
		return fetchControllerAddrByDnsLookup;
	}

	public void setFetchControllerAddrByDnsLookup(boolean fetchControllerAddrByDnsLookup) {
		this.fetchControllerAddrByDnsLookup = fetchControllerAddrByDnsLookup;
	}

	public long getSyncBrokerMetadataPeriod() {
		return syncBrokerMetadataPeriod;
	}

	public void setSyncBrokerMetadataPeriod(long syncBrokerMetadataPeriod) {
		this.syncBrokerMetadataPeriod = syncBrokerMetadataPeriod;
	}

	public long getCheckSyncStateSetPeriod() {
		return checkSyncStateSetPeriod;
	}

	public void setCheckSyncStateSetPeriod(long checkSyncStateSetPeriod) {
		this.checkSyncStateSetPeriod = checkSyncStateSetPeriod;
	}

	public long getSyncControllerMetadataPeriod() {
		return syncControllerMetadataPeriod;
	}

	public void setSyncControllerMetadataPeriod(long syncControllerMetadataPeriod) {
		this.syncControllerMetadataPeriod = syncControllerMetadataPeriod;
	}

	public long getControllerHeartBeatTimeoutMillis() {
		return controllerHeartBeatTimeoutMillis;
	}

	public void setControllerHeartBeatTimeoutMillis(long controllerHeartBeatTimeoutMillis) {
		this.controllerHeartBeatTimeoutMillis = controllerHeartBeatTimeoutMillis;
	}

	public boolean isValidateSystemTopicWhenUpdateTopic() {
		return validateSystemTopicWhenUpdateTopic;
	}

	public void setValidateSystemTopicWhenUpdateTopic(boolean validateSystemTopicWhenUpdateTopic) {
		this.validateSystemTopicWhenUpdateTopic = validateSystemTopicWhenUpdateTopic;
	}

	public int getBrokerElectionPriority() {
		return brokerElectionPriority;
	}

	public void setBrokerElectionPriority(int brokerElectionPriority) {
		this.brokerElectionPriority = brokerElectionPriority;
	}

	public boolean isUseStaticSubscription() {
		return useStaticSubscription;
	}

	public void setUseStaticSubscription(boolean useStaticSubscription) {
		this.useStaticSubscription = useStaticSubscription;
	}

	public MetricsExporterType getMetricsExporterType() {
		return metricsExporterType;
	}

	public void setMetricsExporterType(MetricsExporterType metricsExporterType) {
		this.metricsExporterType = metricsExporterType;
	}

	public int getMetricsOtelCardinalityLimit() {
		return metricsOtelCardinalityLimit;
	}

	public void setMetricsOtelCardinalityLimit(int metricsOtelCardinalityLimit) {
		this.metricsOtelCardinalityLimit = metricsOtelCardinalityLimit;
	}

	public String getMetricsGrpcExporterTarget() {
		return metricsGrpcExporterTarget;
	}

	public void setMetricsGrpcExporterTarget(String metricsGrpcExporterTarget) {
		this.metricsGrpcExporterTarget = metricsGrpcExporterTarget;
	}

	public String getMetricsGrpcExporterHeader() {
		return metricsGrpcExporterHeader;
	}

	public void setMetricsGrpcExporterHeader(String metricsGrpcExporterHeader) {
		this.metricsGrpcExporterHeader = metricsGrpcExporterHeader;
	}

	public long getMetricGrpcExporterTimeoutInMillis() {
		return metricGrpcExporterTimeoutInMillis;
	}

	public void setMetricGrpcExporterTimeoutInMillis(long metricGrpcExporterTimeoutInMillis) {
		this.metricGrpcExporterTimeoutInMillis = metricGrpcExporterTimeoutInMillis;
	}

	public long getMetricGrpcExporterIntervalInMillis() {
		return metricGrpcExporterIntervalInMillis;
	}

	public void setMetricGrpcExporterIntervalInMillis(long metricGrpcExporterIntervalInMillis) {
		this.metricGrpcExporterIntervalInMillis = metricGrpcExporterIntervalInMillis;
	}

	public long getMetricLoggingExporterIntervalInMillis() {
		return metricLoggingExporterIntervalInMillis;
	}

	public void setMetricLoggingExporterIntervalInMillis(long metricLoggingExporterIntervalInMillis) {
		this.metricLoggingExporterIntervalInMillis = metricLoggingExporterIntervalInMillis;
	}

	public int getMetricsPromExporterPort() {
		return metricsPromExporterPort;
	}

	public void setMetricsPromExporterPort(int metricsPromExporterPort) {
		this.metricsPromExporterPort = metricsPromExporterPort;
	}

	public String getMetricsPromExporterHost() {
		return metricsPromExporterHost;
	}

	public void setMetricsPromExporterHost(String metricsPromExporterHost) {
		this.metricsPromExporterHost = metricsPromExporterHost;
	}

	public String getMetricsLabel() {
		return metricsLabel;
	}

	public void setMetricsLabel(String metricsLabel) {
		this.metricsLabel = metricsLabel;
	}

	public boolean isMetricsInDelta() {
		return metricsInDelta;
	}

	public void setMetricsInDelta(boolean metricsInDelta) {
		this.metricsInDelta = metricsInDelta;
	}

	public long getChannelExpiredTimeout() {
		return channelExpiredTimeout;
	}

	public void setChannelExpiredTimeout(long channelExpiredTimeout) {
		this.channelExpiredTimeout = channelExpiredTimeout;
	}

	public long getSubscriptionExpiredTimeout() {
		return subscriptionExpiredTimeout;
	}

	public void setSubscriptionExpiredTimeout(long subscriptionExpiredTimeout) {
		this.subscriptionExpiredTimeout = subscriptionExpiredTimeout;
	}

	public boolean isEstimateAccumulation() {
		return estimateAccumulation;
	}

	public void setEstimateAccumulation(boolean estimateAccumulation) {
		this.estimateAccumulation = estimateAccumulation;
	}

	public boolean isColdStrStrategyEnable() {
		return coldStrStrategyEnable;
	}

	public void setColdStrStrategyEnable(boolean coldStrStrategyEnable) {
		this.coldStrStrategyEnable = coldStrStrategyEnable;
	}

	public boolean isUsePIDColdCtrStrategy() {
		return usePIDColdCtrStrategy;
	}

	public void setUsePIDColdCtrStrategy(boolean usePIDColdCtrStrategy) {
		this.usePIDColdCtrStrategy = usePIDColdCtrStrategy;
	}

	public long getCgColdReadThreshold() {
		return cgColdReadThreshold;
	}

	public void setCgColdReadThreshold(long cgColdReadThreshold) {
		this.cgColdReadThreshold = cgColdReadThreshold;
	}

	public long getGlobalColdReadThreshold() {
		return globalColdReadThreshold;
	}

	public void setGlobalColdReadThreshold(long globalColdReadThreshold) {
		this.globalColdReadThreshold = globalColdReadThreshold;
	}

	public long getFetchNamesrvAddrInterval() {
		return fetchNamesrvAddrInterval;
	}

	public void setFetchNamesrvAddrInterval(long fetchNamesrvAddrInterval) {
		this.fetchNamesrvAddrInterval = fetchNamesrvAddrInterval;
	}

	public boolean isPopResponseReturnActualRetryTopic() {
		return popResponseReturnActualRetryTopic;
	}

	public void setPopResponseReturnActualRetryTopic(boolean popResponseReturnActualRetryTopic) {
		this.popResponseReturnActualRetryTopic = popResponseReturnActualRetryTopic;
	}

	public boolean isEnableSingleTopicRegister() {
		return enableSingleTopicRegister;
	}

	public void setEnableSingleTopicRegister(boolean enableSingleTopicRegister) {
		this.enableSingleTopicRegister = enableSingleTopicRegister;
	}

	public boolean isEnableMaxedMessageType() {
		return enableMaxedMessageType;
	}

	public void setEnableMaxedMessageType(boolean enableMaxedMessageType) {
		this.enableMaxedMessageType = enableMaxedMessageType;
	}

	public boolean isEnableSplitRegistration() {
		return enableSplitRegistration;
	}

	public void setEnableSplitRegistration(boolean enableSplitRegistration) {
		this.enableSplitRegistration = enableSplitRegistration;
	}

	public long getPopInflighMessageThreshold() {
		return popInflighMessageThreshold;
	}

	public void setPopInflighMessageThreshold(long popInflighMessageThreshold) {
		this.popInflighMessageThreshold = popInflighMessageThreshold;
	}

	public boolean isEnablePopMessageThreshold() {
		return enablePopMessageThreshold;
	}

	public void setEnablePopMessageThreshold(boolean enablePopMessageThreshold) {
		this.enablePopMessageThreshold = enablePopMessageThreshold;
	}

	public int getSplitRegistrationSize() {
		return splitRegistrationSize;
	}

	public void setSplitRegistrationSize(int splitRegistrationSize) {
		this.splitRegistrationSize = splitRegistrationSize;
	}

	public String getConfigBlackList() {
		return configBlackList;
	}

	public void setConfigBlackList(String configBlackList) {
		this.configBlackList = configBlackList;
	}
}
