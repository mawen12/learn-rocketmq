package com.mawen.learn.rocketmq.remoting.protocol.header.namesrv;

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
@RocketMQAction(value = RequestCode.REGISTER_BROKER, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class RegisterBrokerRequestHeader implements CommandCustomHeader {

	@CFNotNull
	private String brokerName;

	@CFNotNull
	private String brokerAddr;

	@CFNotNull
	@RocketMQResource(ResourceType.CLUSTER)
	private String clusterName;

	@CFNotNull
	private String haServerAddr;

	@CFNotNull
	private String brokerId;

	@CFNotNull
	private String heartbeatTimeoutMillis;

	@CFNotNull
	private Boolean enableActingMaster;

	private boolean compressed;

	private Integer bodyCrc32 = 0;

	@Override
	public void checkFields() throws RemotingCommandException {

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

	public String getHaServerAddr() {
		return haServerAddr;
	}

	public void setHaServerAddr(String haServerAddr) {
		this.haServerAddr = haServerAddr;
	}

	public String getBrokerId() {
		return brokerId;
	}

	public void setBrokerId(String brokerId) {
		this.brokerId = brokerId;
	}

	public String getHeartbeatTimeoutMillis() {
		return heartbeatTimeoutMillis;
	}

	public void setHeartbeatTimeoutMillis(String heartbeatTimeoutMillis) {
		this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
	}

	public Boolean getEnableActingMaster() {
		return enableActingMaster;
	}

	public void setEnableActingMaster(Boolean enableActingMaster) {
		this.enableActingMaster = enableActingMaster;
	}

	public boolean isCompressed() {
		return compressed;
	}

	public void setCompressed(boolean compressed) {
		this.compressed = compressed;
	}

	public Integer getBodyCrc32() {
		return bodyCrc32;
	}

	public void setBodyCrc32(Integer bodyCrc32) {
		this.bodyCrc32 = bodyCrc32;
	}
}
