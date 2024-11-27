package com.mawen.learn.rocketmq.controller.impl.manager;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.Pair;
import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
@Getter
public class BrokerReplicaInfo implements Serializable {

	private final String clusterName;

	private final String brokerName;

	private final AtomicLong nextAssignBrokerId;

	private final Map<Long, Pair<String, String>> brokerIdInfo;

	public BrokerReplicaInfo(String clusterName, String brokerName) {
		this.clusterName = clusterName;
		this.brokerName = brokerName;
		this.nextAssignBrokerId = new AtomicLong(MixAll.FIRST_BROKER_CONTROLLER_ID);
		this.brokerIdInfo = new ConcurrentHashMap<>();
	}

	public void removeBrokerId(final Long brokerId) {
		brokerIdInfo.remove(brokerId);
	}

	public Long getNextAssignBrokerId() {
		return nextAssignBrokerId.get();
	}

	public void addBroker(final Long brokerId, final String ipAddress, final String registerCheckCode) {
		brokerIdInfo.put(brokerId, new Pair<>(ipAddress, registerCheckCode));
		nextAssignBrokerId.incrementAndGet();
	}

	public boolean isBrokerExist(final Long brokerId) {
		return brokerIdInfo.containsKey(brokerId);
	}

	public Set<Long> getAllBroker() {
		return new HashSet<>(brokerIdInfo.keySet());
	}

	public Map<Long, String> getBrokerIdTable() {
		Map<Long, String> map = new HashMap<>(brokerIdInfo.size());
		brokerIdInfo.forEach((id, pair) -> {
			map.put(id, pair.getObject1());
		});
		return map;
	}

	public String getBrokerAddress(final Long brokerId) {
		if (brokerId == null) {
			return null;
		}
		Pair<String, String> pair = brokerIdInfo.get(brokerId);
		if (pair != null) {
			return pair.getObject1();
		}
		return null;
	}

	public String getBrokerRegisterCheckCode(final Long brokerId) {
		if (brokerId == null) {
			return null;
		}
		Pair<String, String> pair = brokerIdInfo.get(brokerId);
		if (pair != null) {
			return pair.getObject2();
		}
		return null;
	}

	public void updateBrokerAddress(final Long brokerId, final String brokerAddress) {
		if (brokerId == null) {
			return;
		}
		Pair<String, String> oldPair = brokerIdInfo.get(brokerId);
		if (oldPair != null) {
			brokerIdInfo.put(brokerId, new Pair<>(brokerAddress, oldPair.getObject2()));
		}
	}
}
