package com.mawen.learn.rocketmq.remoting.protocol.header;

import com.mawen.learn.rocketmq.common.action.Action;
import com.mawen.learn.rocketmq.common.action.RocketMQAction;
import com.mawen.learn.rocketmq.common.resource.ResourceType;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
@RocketMQAction(value = RequestCode.NOTIFY_BROKER_ROLE_CHANGED, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class NotifyBrokerRoleChangedRequestHeader implements CommandCustomHeader {

	private String masterAddress;

	private Integer masterEpoch;

	private Integer syncStateSetEpoch;

	private Long masterBrokerId;

	public NotifyBrokerRoleChangedRequestHeader() {
	}

	public NotifyBrokerRoleChangedRequestHeader(String masterAddress, Integer masterEpoch, Integer syncStateSetEpoch, Long masterBrokerId) {
		this.masterAddress = masterAddress;
		this.masterEpoch = masterEpoch;
		this.syncStateSetEpoch = syncStateSetEpoch;
		this.masterBrokerId = masterBrokerId;
	}

	@Override
	public void checkFields() throws RemotingCommandException {

	}

	public String getMasterAddress() {
		return masterAddress;
	}

	public void setMasterAddress(String masterAddress) {
		this.masterAddress = masterAddress;
	}

	public Integer getMasterEpoch() {
		return masterEpoch;
	}

	public void setMasterEpoch(Integer masterEpoch) {
		this.masterEpoch = masterEpoch;
	}

	public Integer getSyncStateSetEpoch() {
		return syncStateSetEpoch;
	}

	public void setSyncStateSetEpoch(Integer syncStateSetEpoch) {
		this.syncStateSetEpoch = syncStateSetEpoch;
	}

	public Long getMasterBrokerId() {
		return masterBrokerId;
	}

	public void setMasterBrokerId(Long masterBrokerId) {
		this.masterBrokerId = masterBrokerId;
	}

	@Override
	public String toString() {
		return "NotifyBrokerRoleChangedRequestHeader{" +
				"masterAddress='" + masterAddress + '\'' +
				", masterEpoch=" + masterEpoch +
				", syncStateSetEpoch=" + syncStateSetEpoch +
				", masterBrokerId=" + masterBrokerId +
				'}';
	}
}
