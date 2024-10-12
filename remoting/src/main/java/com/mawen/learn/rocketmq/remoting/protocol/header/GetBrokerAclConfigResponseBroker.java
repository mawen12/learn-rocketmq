package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.common.resource.RocketMQResource;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.annotation.CFNotNull;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.GET_BROKER_CLUSTER_ACL_INFO, resource = ResourceType.CLUSTER, action = Action.LIST)
public class GetBrokerAclConfigResponseBroker implements CommandCustomHeader {

	@CFNotNull
	private String version;

	private String allAclFileVersion;

	@CFNotNull
	private String brokerName;

	@CFNotNull
	private String brokerAddr;

	@CFNotNull
	@RocketMQResource(ResourceType.CLUSTER)
	private String clusterName;

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getAllAclFileVersion() {
		return allAclFileVersion;
	}

	public void setAllAclFileVersion(String allAclFileVersion) {
		this.allAclFileVersion = allAclFileVersion;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public String getBrokerAddr() {
		return brokerAddr;
	}

	public void setBrokerAddr(String brokerAddr) {
		this.brokerAddr = brokerAddr;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
}
