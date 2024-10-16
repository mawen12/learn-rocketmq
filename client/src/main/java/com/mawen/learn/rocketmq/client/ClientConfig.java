package com.mawen.learn.rocketmq.client;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.common.utils.NameServerAddressUtils;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;
import com.mawen.learn.rocketmq.remoting.netty.TlsSystemConfig;
import com.mawen.learn.rocketmq.remoting.protocol.LanguageCode;
import com.mawen.learn.rocketmq.remoting.protocol.RequestType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
@Getter
@Setter
@ToString
public class ClientConfig {

	public static final String SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY = "com.mawen.learn.rocketmq.sendMessageWithVIPChannel";
	public static final String SOCKS_PROXY_CONFIG = "com.mawen.learn.rocketmq.socks.proxy.config";
	public static final String DECODE_READ_BODY = "com.mawen.learn.rocketmq.read.body";
	public static final String DECODE_DECOMPRESS_BODY = "com.mawen.learn.rocketmq.decompress.body";
	public static final String SEND_LATENCY_ENABLE = "com.mawen.learn.rocketmq.sendLatencyEnable";
	public static final String START_DETECTOR_ENABLE = "com.mawen.learn.rocketmq.startDetectorEnable";
	public static final String HEART_BEAT_V2 = "com.mawen.learn.rocketmq.heartbeat.v2";

	private String namesrvAddr = NameServerAddressUtils.getNameServerAddresses();
	private String clientIP = NetworkUtil.getLocalAddress();
	private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
	private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
	protected String namespace;
	private boolean namespaceInitialized = false;
	protected String namespaceV2;
	protected AccessChannel accessChannel = AccessChannel.LOCAL;

	private int pollNameServerInterval = 30 * 1000;
	private int heartbeatBrokerInterval = 30 * 1000;
	private int persistConsumerOffsetInterval = 5 * 1000;
	private long pullTimeDelayMillisWhenException = 1000;
	private boolean unitMode = false;
	private String unitName;
	private boolean decodeReadBody = Boolean.parseBoolean(System.getProperty(DECODE_READ_BODY, "true"));
	private boolean decodeDecompressBody = Boolean.parseBoolean(System.getProperty(DECODE_DECOMPRESS_BODY, "true"));
	private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false"));
	private boolean useHeartbeatV2 = Boolean.parseBoolean(System.getProperty(HEART_BEAT_V2, "false"));

	private boolean useTLS = TlsSystemConfig.tlsEnable;

	private String socksProxyConfig = System.getProperty(SOCKS_PROXY_CONFIG, "{}");

	private int mqClientApiTimeout = 3 * 1000;
	private int detectTimeout = 200;
	private int detectInterval = 2 * 1000;

	private LanguageCode language = LanguageCode.JAVA;

	protected boolean enableStreamRequestType = false;

	private boolean sendLatencyEnable = Boolean.parseBoolean(System.getProperty(SEND_LATENCY_ENABLE, "false"));
	private boolean startDetectorEnable = Boolean.parseBoolean(System.getProperty(START_DETECTOR_ENABLE, "false"));
	private boolean enableHeartbeatChannelEventListener = true;

	protected boolean enableTrace = false;

	protected String traceTopic;

	public String buildMQClientId() {
		StringBuilder sb = new StringBuilder();
		sb.append(this.getClientIP());
		sb.append("@");
		sb.append(this.getInstanceName());

		if (!UtilAll.isBlank(this.unitName)) {
			sb.append("@");
			sb.append(this.unitName);
		}

		if (enableStreamRequestType) {
			sb.append("@");
			sb.append(RequestType.STREAM);
		}

		return sb.toString();
	}

	public void changeInstanceNameToPID() {
		if (this.instanceName.equals("DEFAULT")) {
			this.instanceName = UtilAll.getPid() + "#" + System.nanoTime();
		}
	}

	public String withNamespace(String resouece) {
		return NamespaceUtil.wrapNamespace(this.getNamespace(), resouece);
	}

	public Set<String> withNamespace(Set<String> resourceSet) {
		return resourceSet.stream().map(resource -> withNamespace(resource)).collect(Collectors.toSet());
	}

	public MessageQueue queueWithNamespace(MessageQueue queue) {
		if (StringUtils.isEmpty(this.getNamespace())) {
			return queue;
		}
		return new MessageQueue(withNamespace(queue.getTopic()), queue.getBrokerName(), queue.getQueueId());
	}

	public Collection<MessageQueue> queuesWithNamespace(Collection<MessageQueue> queues) {
		if (StringUtils.isEmpty(this.getNamespace())) {
			return queues;
		}

		queues.forEach(queue -> queue.setTopic(withNamespace(queue.getTopic())));

		return queues;
	}

	public void resetClientConfig(final ClientConfig config) {
		this.namesrvAddr = config.namesrvAddr;
		this.clientIP = config.clientIP;
		this.instanceName = config.instanceName;
		this.clientCallbackExecutorThreads = config.clientCallbackExecutorThreads;
		this.pollNameServerInterval = config.pollNameServerInterval;
		this.heartbeatBrokerInterval = config.heartbeatBrokerInterval;
		this.persistConsumerOffsetInterval = config.persistConsumerOffsetInterval;
		this.pullTimeDelayMillisWhenException = config.pullTimeDelayMillisWhenException;
		this.unitMode = config.unitMode;
		this.unitName = config.unitName;
		this.vipChannelEnabled = config.vipChannelEnabled;
		this.useTLS = config.useTLS;
		this.socksProxyConfig = config.socksProxyConfig;
		this.namespace = config.namespace;
		this.language = config.language;
		this.mqClientApiTimeout = config.mqClientApiTimeout;
		this.decodeReadBody = config.decodeReadBody;
		this.decodeDecompressBody = config.decodeDecompressBody;
		this.enableStreamRequestType = config.enableStreamRequestType;
		this.useHeartbeatV2 = config.useHeartbeatV2;
		this.startDetectorEnable = config.startDetectorEnable;
		this.sendLatencyEnable = config.sendLatencyEnable;
		this.enableHeartbeatChannelEventListener = config.enableHeartbeatChannelEventListener;
		this.detectInterval = config.detectInterval;
		this.detectTimeout = config.detectTimeout;
		this.namespaceV2 = config.namespaceV2;
		this.enableTrace = config.enableTrace;
		this.traceTopic = config.traceTopic;
	}

	public ClientConfig cloneClientConfig() {
		ClientConfig config = new ClientConfig();
		config.namesrvAddr = this.namesrvAddr;
		config.clientIP = this.clientIP;
		config.instanceName = this.instanceName;
		config.clientCallbackExecutorThreads = this.clientCallbackExecutorThreads;
		config.pollNameServerInterval = this.pollNameServerInterval;
		config.heartbeatBrokerInterval = this.heartbeatBrokerInterval;
		config.persistConsumerOffsetInterval = this.persistConsumerOffsetInterval;
		config.pullTimeDelayMillisWhenException = this.pullTimeDelayMillisWhenException;
		config.unitMode = this.unitMode;
		config.unitName = this.unitName;
		config.vipChannelEnabled = this.vipChannelEnabled;
		config.useTLS = this.useTLS;
		config.socksProxyConfig = this.socksProxyConfig;
		config.namespace = this.namespace;
		config.language = this.language;
		config.mqClientApiTimeout = this.mqClientApiTimeout;
		config.decodeReadBody = this.decodeReadBody;
		config.decodeDecompressBody = this.decodeDecompressBody;
		config.enableStreamRequestType = this.enableStreamRequestType;
		config.useHeartbeatV2 = this.useHeartbeatV2;
		config.startDetectorEnable = this.startDetectorEnable;
		config.sendLatencyEnable = this.sendLatencyEnable;
		config.enableHeartbeatChannelEventListener = this.enableHeartbeatChannelEventListener;
		config.detectInterval = this.detectInterval;
		config.detectTimeout = this.detectTimeout;
		config.namespaceV2 = this.namespaceV2;
		config.enableTrace = this.enableTrace;
		config.traceTopic = this.traceTopic;
		return config;
	}

	public String getNamesrvAddr() {
		if (StringUtils.isNotEmpty(namesrvAddr) && NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.matcher(namesrvAddr.trim()).matches()) {
			return NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(namesrvAddr);
		}
		return namesrvAddr;
	}

	public void setNamesrvAddr(String namesrvAddr) {
		this.namesrvAddr = namesrvAddr;
		this.namespaceInitialized = false;
	}

	public String getNamespace() {
		if (namespaceInitialized) {
			return namespace;
		}

		if (StringUtils.isNotEmpty(namesrvAddr)) {
			return namespace;
		}

		if (StringUtils.isNotEmpty(namesrvAddr)) {
			if (NameServerAddressUtils.validateInstanceEndpoint(namesrvAddr)) {
				namespace = NameServerAddressUtils.parseInstanceFromEndpoint(namesrvAddr);
			}
		}

		namespaceInitialized = true;
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
		this.namespaceInitialized = true;
	}


}
