package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Objects;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class ElectMasterResponseBody extends RemotingSerializable {

	private BrokerMemberGroup brokerMemberGroup;

	private Set<Long> syncStateSet;

	public ElectMasterResponseBody() {
		this.syncStateSet = new HashSet<>();
		this.brokerMemberGroup = null;
	}

	public ElectMasterResponseBody(Set<Long> syncStateSet) {
		this.syncStateSet = syncStateSet;
		this.brokerMemberGroup = null;
	}

	public ElectMasterResponseBody(BrokerMemberGroup brokerMemberGroup, Set<Long> syncStateSet) {
		this.brokerMemberGroup = brokerMemberGroup;
		this.syncStateSet = syncStateSet;
	}

	public BrokerMemberGroup getBrokerMemberGroup() {
		return brokerMemberGroup;
	}

	public void setBrokerMemberGroup(BrokerMemberGroup brokerMemberGroup) {
		this.brokerMemberGroup = brokerMemberGroup;
	}

	public Set<Long> getSyncStateSet() {
		return syncStateSet;
	}

	public void setSyncStateSet(Set<Long> syncStateSet) {
		this.syncStateSet = syncStateSet;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ElectMasterResponseBody that = (ElectMasterResponseBody) o;
		return Objects.equal(brokerMemberGroup, that.brokerMemberGroup) &&
		       Objects.equal(syncStateSet, that.syncStateSet);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(brokerMemberGroup, syncStateSet);
	}

	@Override
	public String toString() {
		return "BrokerMemberGroup{" +
		       "brokerMemberGroup='" + brokerMemberGroup.toString() + '\'' +
		       ", syncStateSet='" + syncStateSet.toString() +
		       '}';
	}
}
