package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.Set;

import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class RoleChangeNotifyEntry {

	private final BrokerMemberGroup brokerMemberGroup;

	private final String masterAddress;

	private final Long masterBrokerId;

	private final int masterEpoch;

	private final int syncStateSetEpoch;

	private final Set<Long> syncStateSet;

	public RoleChangeNotifyEntry(BrokerMemberGroup brokerMemberGroup, String masterAddress, Long masterBrokerId, int masterEpoch, int syncStateSetEpoch, Set<Long> syncStateSet) {
		this.brokerMemberGroup = brokerMemberGroup;
		this.masterAddress = masterAddress;
		this.masterBrokerId = masterBrokerId;
		this.masterEpoch = masterEpoch;
		this.syncStateSetEpoch = syncStateSetEpoch;
		this.syncStateSet = syncStateSet;
	}

	public static RoleChangeNotifyEntry convert(RemotingCommand electMasterResponse) {
		ElectMasterResponseHeader header = (ElectMasterResponseHeader) electMasterResponse.readCustomHeader();
		BrokerMemberGroup brokerMemberGroup = null;
		Set<Long> syncStateSet = null;

		if (electMasterResponse.getBody() != null && electMasterResponse.getBody().length > 0) {
			ElectMasterResponseBody body = RemotingSerializable.decode(electMasterResponse.getBody(), ElectMasterResponseBody.class);
			brokerMemberGroup = body.getBrokerMemberGroup();
			syncStateSet = body.getSyncStateSet();
		}

		return new RoleChangeNotifyEntry(brokerMemberGroup, header.getMasterAddress(), header.getMasterBrokerId(), header.getMasterEpoch(), header.getSyncStateSetEpoch(), syncStateSet);
	}

	public BrokerMemberGroup getBrokerMemberGroup() {
		return brokerMemberGroup;
	}

	public String getMasterAddress() {
		return masterAddress;
	}

	public Long getMasterBrokerId() {
		return masterBrokerId;
	}

	public int getMasterEpoch() {
		return masterEpoch;
	}

	public int getSyncStateSetEpoch() {
		return syncStateSetEpoch;
	}

	public Set<Long> getSyncStateSet() {
		return syncStateSet;
	}
}
