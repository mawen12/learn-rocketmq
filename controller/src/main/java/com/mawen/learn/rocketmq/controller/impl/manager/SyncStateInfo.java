package com.mawen.learn.rocketmq.controller.impl.manager;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
public class SyncStateInfo implements Serializable {

	@Getter
	private final String clusterName;
	@Getter
	private final String brokerName;
	private final AtomicInteger masterEpoch;
	private final AtomicInteger syncStateSetEpoch;

	private Set<Long> syncStateSet;

	@Getter
	private Long masterBrokerId;

	public SyncStateInfo(String clusterName, String brokerName) {
		this.clusterName = clusterName;
		this.brokerName = brokerName;
		this.masterEpoch = new AtomicInteger(0);
		this.syncStateSetEpoch = new AtomicInteger(0);
		this.syncStateSet = Collections.emptySet();
	}

	public void updateMasterInfo(Long masterBrokerId) {
		this.masterBrokerId = masterBrokerId;
		this.masterEpoch.incrementAndGet();
	}

	public void updateSyncStateSetInfo(Set<Long> newSyncStateSet) {
		this.syncStateSet = new HashSet<>(newSyncStateSet);
		this.syncStateSetEpoch.incrementAndGet();
	}

	public boolean isFirstTimeForElect() {
		return masterEpoch.get() == 0;
	}

	public boolean isMasterExist() {
		return masterBrokerId != null;
	}

	public Set<Long> getSyncStateSet() {
		return new HashSet<>(syncStateSet);
	}

	public int getSyncStateSetEpoch() {
		return syncStateSetEpoch.get();
	}

	public int getMasterEpoch() {
		return masterEpoch.get();
	}

	public void removeFromSyncState(final Long brokerId) {
		syncStateSet.remove(brokerId);
	}
}
