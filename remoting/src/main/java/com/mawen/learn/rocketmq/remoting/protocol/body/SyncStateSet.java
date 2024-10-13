package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.Set;

import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class SyncStateSet extends RemotingSerializable {

	private Set<Long> syncStateSet;

	private int syncStateSetEpoch;

	public SyncStateSet(Set<Long> syncStateSet, int syncStateSetEpoch) {
		this.syncStateSet = syncStateSet;
		this.syncStateSetEpoch = syncStateSetEpoch;
	}

	public Set<Long> getSyncStateSet() {
		return syncStateSet;
	}

	public void setSyncStateSet(Set<Long> syncStateSet) {
		this.syncStateSet = syncStateSet;
	}

	public int getSyncStateSetEpoch() {
		return syncStateSetEpoch;
	}

	public void setSyncStateSetEpoch(int syncStateSetEpoch) {
		this.syncStateSetEpoch = syncStateSetEpoch;
	}

	@Override
	public String toString() {
		return "SyncStateSet{" +
		       "syncStateSet=" + syncStateSet +
		       ", syncStateSetEpoch=" + syncStateSetEpoch +
		       "} " + super.toString();
	}
}
