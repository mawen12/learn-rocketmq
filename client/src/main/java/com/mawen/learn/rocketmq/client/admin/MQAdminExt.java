package com.mawen.learn.rocketmq.client.admin;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.mawen.learn.rocketmq.client.MQAdmin;
import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.PlainAccessConfig;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.message.MessageRequestMode;
import com.mawen.learn.rocketmq.remoting.exception.RemotingConnectException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingSendRequestException;
import com.mawen.learn.rocketmq.remoting.exception.RemotingTimeoutException;
import com.mawen.learn.rocketmq.remoting.protocol.admin.ConsumeStats;
import com.mawen.learn.rocketmq.remoting.protocol.admin.RollbackStats;
import com.mawen.learn.rocketmq.remoting.protocol.admin.TopicStatsTable;
import com.mawen.learn.rocketmq.remoting.protocol.body.AclInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import com.mawen.learn.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.BrokerStatsData;
import com.mawen.learn.rocketmq.remoting.protocol.body.ClusterInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumeStatsList;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerConnection;
import com.mawen.learn.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.EpochEntryCache;
import com.mawen.learn.rocketmq.remoting.protocol.body.GroupList;
import com.mawen.learn.rocketmq.remoting.protocol.body.HARuntimeInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.KVTable;
import com.mawen.learn.rocketmq.remoting.protocol.body.ProducerConnection;
import com.mawen.learn.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody;
import com.mawen.learn.rocketmq.remoting.protocol.body.QueueTimeSpan;
import com.mawen.learn.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import com.mawen.learn.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import com.mawen.learn.rocketmq.remoting.protocol.body.TopicList;
import com.mawen.learn.rocketmq.remoting.protocol.body.UserInfo;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.GetMetadataResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.heartbeat.SubscriptionData;
import com.mawen.learn.rocketmq.remoting.protocol.route.TopicRouteData;
import com.mawen.learn.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import com.mawen.learn.rocketmq.remoting.protocol.subscription.GroupForbidden;
import com.mawen.learn.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public interface MQAdminExt extends MQAdmin {

	void start() throws MQClientException;

	void shutdown();

	void addBrokerToContainer(final String brokerContainerAddr, final String brokerConfig) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;

	void removeBrokerFromContainer(final String brokerContainerAddr, String clusterName, final String brokerName, long brokerId) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;

	void updateBrokerConfig(final String brokerAddr, final Properties properties) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException;

	Properties getBrokerConfig(final String brokerAddr) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException;

	void createAndUpdateTopicConfig(final String addr, final TopicConfig config) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

	void createAndUpdatePlainAccessConfig(final String addr, final PlainAccessConfig plainAccessConfig) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

	void deletePlainAccessConfig(final String addr, final String accessKey) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

	void updateGlobalWhiteAddrConfig(final String addr, final String globalWhiteAddrs) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

	void updateGlobalWhiteAddrConfig(final String addr, final String globalWhiteAddrs, String aclFileFullPath) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

	void examineBrokerClusterAclVersionInfo(final String addr) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

	void createAndUpdateSubscriptionGroupConfig(final String addr, final SubscriptionGroupConfig config) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

	SubscriptionGroupConfig examineSubscriptionGroupConfig(final String addr, final String group) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

	TopicStatsTable examineTopicStats(final String topic) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

	TopicStatsTable examineTopicStats(final String brokerAddr, final String topic) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

	AdminToolResult<TopicStatsTable> examineTopicStatsConcurrent(String topic);

	TopicList fetchAllTopicList() throws RemotingException, MQClientException, InterruptedException;

	TopicList fetchTopicByCluster(String clusterName) throws RemotingException, MQClientException, InterruptedException;

	KVTable fetchBrokerRuntimeStats(final String brokerAddr) throws RemotingException, MQClientException, InterruptedException;

	ConsumeStats examineConsumeStats(final String consumeGroup) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

	ConsumeStats examineConsumeStats(final String consumeGroup, final String topic) throws RemotingException, MQClientException, InterruptedException, MQBrokerException;

	ConsumeStats examineConsumeStats(final String brokerAddr, final String consumeGroup, final String topic, final long timeoutMillis) throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException;

	AdminToolResult<ConsumeStats> examineConsumeStatsConcurrent(String consumerGroup, String topic);

	ClusterInfo examineBrokerClusterInfo() throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException;

	TopicRouteData examineTopicRouteInfo(final String topic) throws RemotingException, MQClientException, MQBrokerException;

	ConsumerConnection examineConsumerConnectionInfo(final String consumerGroup) throws RemotingException, MQClientException, MQBrokerException;

	ConsumerConnection examineConsumerConnectionInfo(final String consumerGroup, final String brokerAddr) throws RemotingException, MQClientException, MQBrokerException;

	ProducerConnection examineProducerConnectionInfo(final String producerGroup, final String topic) throws RemotingException, MQClientException, MQBrokerException;

	ProducerConnection examineProducerConnectionInfo(final String brokerAddr) throws RemotingException, MQClientException, MQBrokerException;

	List<String> getNameServerAddressList();

	int wipeWritePermOfBroker(final String namesrvAddr, String brokerName) throws RemotingException, MQClientException;

	int addWritePermOfBroker(final String namesrvAddr, String brokerName) throws RemotingException, MQClientException;

	void putKVConfig(final String namespace, final String key, final String value);

	String getKVConfig(final String namespace, final String key) throws RemotingException, MQClientException;

	KVTable getKVListByNamespace(final String namespace) throws RemotingException, MQClientException;

	void deleteTopic(final String topicName, final String clusterName) throws RemotingException, MQClientException;

	void deleteTopicInBroker(final Set<String> addrs, final String topic) throws RemotingException, MQBrokerException, MQClientException;

	AadminToolResult<BrokerOperatorResult> deleteTopicInBrokerConcurrent(Set<String> addrs, String topic);

	void deleteTopicInNameServer(final Set<String> addrs, final String topic) throws RemotingException, MQClientException, MQBrokerException;

	void deleteTopicInNameServer(final Set<String> addrs, final String clusterName, final String topic) throws RemotingException, MQBrokerException, MQClientException;

	void deleteSubscriptionGroup(final String addr, String groupName) throws RemotingException, MQBrokerException, MQClientException;

	void deleteSubscriptionGroup(final String addr, String groupName, boolean removeOffset) throws RemotingException, MQBrokerException, MQClientException;

	void createAndUpdateKVConfig(String namespace, String key, String value) throws RemotingException, MQBrokerException, MQClientException;

	void deleteKVConfig(String namespace, String key) throws RemotingException, MQBrokerException, MQClientException;

	List<RollbackStats> resetOffsetByTimestampOld(String consumerGroup, String topic, long timestamp, boolean force) throws RemotingException, MQClientException, MQBrokerException;

	Map<MessageQueue, Long> resetOffsetByTimestamp(String topic, String group, long timestamp, boolean force) throws RemotingException, MQClientException, MQBrokerException;

	void resetOffsetNew(String consumerGroup, String topic, long timestamp) throws RemotingException, MQBrokerException, MQClientException;

	AdminToolResult<BrokerOperatorResult> resetOffsetNewConcurrent(final String group, final String topic, final long timestamp);

	Map<String, Map<MessageQueue, Long>> getConsumeStatus(String topic, String group, String clientAddr) throws RemotingException, MQClientException, MQBrokerException;

	void createOrUpdateOrderConf(String key, String value, boolean isCluster) throws RemotingException, MQBrokerException, MQClientException;

	GroupList queryTopicConsumeByWho(String topic) throws RemotingException, MQClientException, MQBrokerException;

	TopicList queryTopicByConsumer(String group) throws RemotingException, MQClientException, MQBrokerException;

	AdminToolResult<TopicList> queryTopicByConsumerGroupConcurrent(final String group);

	SubscriptionData querySubscription(final String group, final String topic) throws RemotingException, MQClientException, MQBrokerException;

	List<QueueTimeSpan> queryConsumeTimeSpan(final String topic, final String group) throws RemotingException, MQClientException, MQBrokerException;

	AdminToolResult<List<QueueTimeSpan>> queryConsumeTimeSpanConcurrent(final String topic, final String group);

	boolean cleanExpiredConsumerQueue(String cluster) throws RemotingException, MQClientException;

	boolean cleanExpiredConsumerQueueByAddr(String addr)throws RemotingException, MQClientException;

	boolean deleteExpiredCommitLog(String cluster)throws RemotingException, MQClientException;

	boolean deleteExpiredCommitLogByAddr(String addr) throws RemotingException, MQClientException;

	boolean cleanUnusedTopic(String cluster)throws RemotingException, MQClientException;

	boolean cleanUnusedTopicByAddr(String cluster) throws RemotingException, MQClientException;

	ConsumerRunningInfo getConsumerRunningInfo(final String consumerGroup, final String clientId, final boolean jstack) throws RemotingException, MQClientException;

	ConsumerRunningInfo getConsumerRunningInfo(final String consumerGroup, final String clientId, final boolean jstack, final boolean metrics) throws RemotingException, MQClientException;

	ConsumeMessageDirectlyResult consumeMessageDirectly(final String consumerGroup, final String clientId, final String topic, final String msgId) throws RemotingException, MQClientException;

	List<MessageTrack> messageTrackDetail(MessageExt msg) throws RemotingException, MQClientException;

	List<MessageTrack> messageTrackDetailConcurrent(MessageExt msg) throws RemotingException, MQClientException;

	void cloneGroupOffset(String srcGroup, String destGroup, String topic, boolean isOffline) throws RemotingException, MQClientException;

	BrokerStatsData viewBrokerStatsData(final String brokerAddr, final String statsName, final String statsKey) throws RemotingException, MQClientException;

	Set<String> getClusterList(final String topic) throws RemotingException, MQClientException;

	ConsumeStatsList fetchConsumeStatsInBroker(final String brokerAddr, boolean isOrder, long timeoutMillis) throws RemotingException, MQClientException;

	Set<String> getTopicClusterList(final String topic) throws RemotingException, MQClientException;

	SubscriptionGroupWrapper getAllSubscriptionGroup(final String brokerAddr, long timeoutMillis) throws RemotingException, MQClientException;

	SubscriptionGroupWrapper getUserSubscriptionGroup(final String brokerAddr, long timeoutMillis) throws RemotingException, MQClientException;

	TopicConfigSerializeWrapper getAllTopicConfig(final String brokerAddr, long timeoutMillis) throws RemotingException, MQClientException;

	TopicConfigSerializeWrapper getUserTopicConfig(final String brokerAddr, long timeoutMillis) throws RemotingException, MQClientException;

	void updateConsumeOffset(String brokerAddr, String consumerGroup, MessageQueue mq, long offset) throws RemotingException, MQClientException;

	void updateNameServerConfig(final Properties properties, final List<String> nameServers) throws RemotingException, MQClientException;;

	Map<String, Properties> getNameServerConfig(final List<String> nameServers) throws RemotingException, MQClientException;;

	QueryConsumeQueueResponseBody queryConsumeQueue(final String brokerAddr, final String topic, final int queueId, final long index, final int count, final String consumeGroup) throws RemotingException, MQClientException;

	boolean resumeCheckHalfMessage(final String topic, final String msgId) throws RemotingException, MQClientException;

	void setMessageRequestMode(final String brokerAddr, final String topic, final String consumerGroup, final MessageRequestMode mode, final int popWorkGroupSize, final long timeoutMillis) throws RemotingException, MQClientException;

	long searchOffset(final String brokerAddr, final String topic, final int queueId, final long timestamp, final long timeoutMillis) throws RemotingException, MQClientException;

	void resetOffsetByQueueId(final String brokerAddr, final String consumerGroup, final String topicName, final int queueId, final long resetOffset) throws RemotingException, MQClientException;

	TopicConfig examineTopicConfig(final String addr, final String topic) throws RemotingException, MQClientException;

	void createStaticTopic(final String addr, final String defaultTopic, final TopicQueueMappingDetail mappingDetail, final boolean force) throws RemotingException, MQClientException;

	GroupForbidden updateAndGetGroupReadForbidden(String brokerAddr, String groupName, String topicName, Boolean readable) throws RemotingException, MQClientException;

	MessageExt queryMessage(String clusterName, String topic, String msgId) throws RemotingException, MQClientException;

	HARuntimeInfo getBrokerHAStatus(String brokerAddr) throws RemotingException, MQClientException;

	BrokerReplicasInfo getInSyncStateData(String controllerAddr, List<String> brokers) throws RemotingException, MQClientException;

	EpochEntryCache getBrokerEpochCache(String brokerAddr) throws RemotingException, MQClientException;

	GetMetadataResponseHeader getControllerMetaData(String controllerAddr) throws RemotingException, MQClientException;

	void resetMasterFlushOffset(String brokerAddr, long masterFlushOffset) throws RemotingException, MQClientException;

	Map<String, Properties> getControllerConfig(List<String> controllerServers) throws RemotingException, MQClientException;

	void updateControllerConfig(final Properties properties, final List<String> controllers) throws RemotingException, MQClientException;

	Pair<ElectMasterRequestHeader, BrokerMemberGroup> electMaster(String controllerAddr, String clusterName, String brokerName, Long brokerId) throws RemotingException, MQClientException;

	void cleanControllerBrokerData(String controllerAddr, String clusterName, String brokerName, String brokerControllerIdsToClean, boolean isCleanLivingBroker) throws RemotingException, MQClientException;

	void updateColdDataFlowCtrGroupConfig(final String brokerAddr, final Properties properties) throws RemotingException, MQClientException;

	void removeColdDataFlowCtrGroupConfig(final String brokerAddr, final String consumeGroup) throws RemotingException, MQClientException;

	String getColdDataFlowCtrInfo(final String brokerAddr) throws RemotingException, MQClientException;

	String setCommitLogReadAheadMode(final String brokerAddr, String mode) throws RemotingException, MQClientException;

	// region user

	void createUser(String brokerAddr, String username, String password, String userType) throws RemotingException, MQClientException;

	void createUser(String brokerAddr, UserInfo userInfo) throws RemotingException, MQClientException;

	void updateUser(String brokerAddr, String username, String password, String userType, String userStatus) throws RemotingException, MQClientException;

	void updateUser(String brokerAddr, UserInfo userInfo) throws RemotingException, MQClientException;

	void deleteUser(String brokerAddr, String username) throws RemotingException, MQClientException;

	UserInfo getUser(String brokerAddr, String username) throws RemotingException, MQClientException;

	List<UserInfo> listUser(String brokerAddr, String filter) throws RemotingException, MQClientException;
	// endregion user


	//	region acl
	void createAcl(String brokerAddr, String subject, List<String> resources, List<String> actions, List<String> sourceIps, String decision) throws RemotingException, MQClientException;

	void createAcl(String brokerAddr, AclInfo aclInfo) throws RemotingException, MQClientException;

	void updateAcl(String brokerAddr, String subject, List<String> resources, List<String> actions, List<String> sourceIps, String decision) throws RemotingException, MQClientException;

	void updateAcl(String brokerAddr, AclInfo aclInfo) throws RemotingException, MQClientException;

	void deleteAcl(String brokerAddr, String subject) throws RemotingException, MQClientException;

	AclInfo getAcl(String brokerAddr, String subject) throws RemotingException, MQClientException;

	List<AclInfo> listAcl(String brokerAddr, String subjectFilter, String resourceFilter) throws RemotingException, MQClientException;
	// endregion acl


}
