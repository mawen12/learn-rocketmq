package com.mawen.learn.rocketmq.remoting.protocol.header;

import java.util.Map;

import com.google.common.base.MoreObjects;
import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.FastCodesHeader;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.rpc.TopicQueueRequestHeader;
import io.netty.buffer.ByteBuf;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
@RocketMQAction(value = RequestCode.PULL_MESSAGE, action = Action.SUB)
public class PullMessageRequestHeader extends TopicQueueRequestHeader implements FastCodesHeader {

	@CFNotNull
	@RocketMQResource(ResourceType.GROUP)
	private String consumerGroup;

	@CFNotNull
	@RocketMQResource(ResourceType.TOPIC)
	private String topic;

	@CFNotNull
	private Integer queueId;

	@CFNotNull
	private Long queueOffset;

	@CFNotNull
	private Integer maxMsgNums;

	@CFNotNull
	private Integer sysFlag;

	@CFNotNull
	private Long commitOffset;

	@CFNotNull
	private Long suspendTimeoutMillis;

	@CFNotNull
	private String subscription;

	@CFNotNull
	private Long subVersion;

	@CFNotNull
	private Integer maxMsgBytes;

	private String expressionType;

	private Integer requestSource;

	private String proxyForwardClientId;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	@Override
	public void encode(ByteBuf out) {
		writeIfNotNull(out, "consumerGroup", consumerGroup);
		writeIfNotNull(out, "topic", topic);
		writeIfNotNull(out, "queueId", queueId);
		writeIfNotNull(out, "queueOffset", queueOffset);
		writeIfNotNull(out, "maxMsgNums", maxMsgNums);
		writeIfNotNull(out, "sysFlag", sysFlag);
		writeIfNotNull(out, "commitOffset", commitOffset);
		writeIfNotNull(out, "suspendTimeoutMillis", suspendTimeoutMillis);
		writeIfNotNull(out, "subscription", subscription);
		writeIfNotNull(out, "subVersion", subVersion);
		writeIfNotNull(out, "expressionType", expressionType);
		writeIfNotNull(out, "maxMsgBytes", maxMsgBytes);
		writeIfNotNull(out, "requestSource", requestSource);
		writeIfNotNull(out, "proxyForwardClientId", proxyForwardClientId);
		writeIfNotNull(out, "lo", lo);
		writeIfNotNull(out, "ns", namespace);
		writeIfNotNull(out, "nsd", namespaced);
		writeIfNotNull(out, "bname", brokerName);
		writeIfNotNull(out, "oway", oneway);
	}

	@Override
	public void decode(Map<String, String> fields) throws RemotingCommandException {
		this.consumerGroup = getAndCheckNotNull(fields, "consumerGroup");

		this.topic = getAndCheckNotNull(fields, "topic");

		this.queueId = Integer.valueOf(getAndCheckNotNull(fields, "queueId"));

		this.queueOffset = Long.valueOf(getAndCheckNotNull(fields, "queueOffset"));

		this.maxMsgNums = Integer.valueOf(getAndCheckNotNull(fields, "maxMsgNums"));

		this.sysFlag = Integer.valueOf(getAndCheckNotNull(fields, "sysFlag"));

		this.commitOffset = Long.valueOf(getAndCheckNotNull(fields, "commitOffset"));

		this.suspendTimeoutMillis = Long.valueOf(getAndCheckNotNull(fields, "suspendTimeoutMillis"));

		String str = fields.get("subscription");
		if (str != null) {
			this.subscription = str;
		}

		this.subVersion = Long.valueOf(getAndCheckNotNull(fields, "subVersion"));

		str = fields.get("expressionType");
		if (str != null) {
			this.expressionType = str;
		}

		str = fields.get("maxMsgBytes");
		if (str != null) {
			this.maxMsgBytes = Integer.valueOf(str);
		}

		str = fields.get("requestSource");
		if (str != null) {
			this.requestSource = Integer.valueOf(str);
		}

		str = fields.get("proxyForwardClientId");
		if (str != null) {
			this.proxyForwardClientId = str;
		}

		str = fields.get("lo");
		if (str != null) {
			this.lo = Boolean.valueOf(str);
		}

		str = fields.get("ns");
		if (str != null) {
			this.namespace = str;
		}

		str = fields.get("nsd");
		if (str != null) {
			this.namespaced = Boolean.valueOf(str);
		}

		str = fields.get("bname");
		if (str != null) {
			this.brokerName = str;
		}

		str = fields.get("oway");
		if (str != null) {
			this.oneway = Boolean.parseBoolean(str);
		}
	}

	@Override
	public Integer getQueueId() {
		return queueId;
	}

	@Override
	public void setQueueId(Integer queueId) {
		this.queueId = queueId;
	}

	@Override
	public String getTopic() {
		return topic;
	}

	@Override
	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public Long getQueueOffset() {
		return queueOffset;
	}

	public void setQueueOffset(Long queueOffset) {
		this.queueOffset = queueOffset;
	}

	public Integer getMaxMsgNums() {
		return maxMsgNums;
	}

	public void setMaxMsgNums(Integer maxMsgNums) {
		this.maxMsgNums = maxMsgNums;
	}

	public Integer getSysFlag() {
		return sysFlag;
	}

	public void setSysFlag(Integer sysFlag) {
		this.sysFlag = sysFlag;
	}

	public Long getCommitOffset() {
		return commitOffset;
	}

	public void setCommitOffset(Long commitOffset) {
		this.commitOffset = commitOffset;
	}

	public Long getSuspendTimeoutMillis() {
		return suspendTimeoutMillis;
	}

	public void setSuspendTimeoutMillis(Long suspendTimeoutMillis) {
		this.suspendTimeoutMillis = suspendTimeoutMillis;
	}

	public String getSubscription() {
		return subscription;
	}

	public void setSubscription(String subscription) {
		this.subscription = subscription;
	}

	public Long getSubVersion() {
		return subVersion;
	}

	public void setSubVersion(Long subVersion) {
		this.subVersion = subVersion;
	}

	public Integer getMaxMsgBytes() {
		return maxMsgBytes;
	}

	public void setMaxMsgBytes(Integer maxMsgBytes) {
		this.maxMsgBytes = maxMsgBytes;
	}

	public String getExpressionType() {
		return expressionType;
	}

	public void setExpressionType(String expressionType) {
		this.expressionType = expressionType;
	}

	public Integer getRequestSource() {
		return requestSource;
	}

	public void setRequestSource(Integer requestSource) {
		this.requestSource = requestSource;
	}

	public String getProxyForwardClientId() {
		return proxyForwardClientId;
	}

	public void setProxyForwardClientId(String proxyForwardClientId) {
		this.proxyForwardClientId = proxyForwardClientId;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("consumerGroup", consumerGroup)
				.add("topic", topic)
				.add("queueId", queueId)
				.add("queueOffset", queueOffset)
				.add("maxMsgBytes", maxMsgBytes)
				.add("maxMsgNums", maxMsgNums)
				.add("sysFlag", sysFlag)
				.add("commitOffset", commitOffset)
				.add("suspendTimeoutMillis", suspendTimeoutMillis)
				.add("subscription", subscription)
				.add("subVersion", subVersion)
				.add("expressionType", expressionType)
				.add("requestSource", requestSource)
				.add("proxyForwardClientId", proxyForwardClientId)
				.toString();
	}
}
