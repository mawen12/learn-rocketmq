package com.mawen.learn.rocketmq.controller.elect.impl;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.mawen.learn.rocketmq.controller.elect.ElectPolicy;
import com.mawen.learn.rocketmq.controller.helper.BrokerLiveInfoGetter;
import com.mawen.learn.rocketmq.controller.helper.BrokerValidPredicate;
import com.mawen.learn.rocketmq.controller.impl.heartbeat.BrokerLiveInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DefaultElectPolicy implements ElectPolicy {

	private BrokerValidPredicate validPredicate;

	private BrokerLiveInfoGetter brokerLiveInfoGetter;

	private final Comparator<BrokerLiveInfo> comparator = ((o1, o2) -> {
		if (o1.getEpoch() == o2.getEpoch()) {
			return o1.getMaxOffset() == o2.getMaxOffset()
					? o1.getElectionPriority() - o2.getElectionPriority()
					: (int) (o2.getMaxOffset() - o1.getMaxOffset());
		}
		else {
			return o2.getEpoch() - o1.getEpoch();
		}
	});

	@Override
	public Long elect(String clusterName, String brokerName, Set<Long> syncStateBrokers, Set<Long> allReplicaBrokers, Long oldMaster, Long brokerId) {
		Long newMaster = null;
		if (syncStateBrokers != null) {
			newMaster = tryElect(clusterName, brokerName, syncStateBrokers, oldMaster, brokerId);
		}
		if (newMaster != null) {
			return newMaster;
		}

		if (allReplicaBrokers != null) {
			newMaster = tryElect(clusterName, brokerName, allReplicaBrokers, oldMaster, brokerId);
		}
		return newMaster;
	}

	private Long tryElect(String clusterName, String brokerName, Set<Long> brokers, Long oldMaster, Long preferBrokerId) {
		if (validPredicate != null) {
			brokers = brokers.stream().filter(brokerAddr -> validPredicate.check(clusterName, brokerName, brokerAddr)).collect(Collectors.toSet());
		}
		if (!brokers.isEmpty()) {
			if (brokers.contains(oldMaster) && (preferBrokerId == null || preferBrokerId.equals(oldMaster))) {
				return oldMaster;
			}

			if (preferBrokerId != null) {
				return brokers.contains(preferBrokerId) ? preferBrokerId : null;
			}

			if (brokerLiveInfoGetter != null) {
				TreeSet<BrokerLiveInfo> brokerLiveInfos = new TreeSet<>(comparator);
				brokers.forEach(brokerAddr -> brokerLiveInfos.add(brokerLiveInfoGetter.get(clusterName, brokerName, brokerAddr)));
				if (!brokerLiveInfos.isEmpty()) {
					return brokerLiveInfos.first().getBrokerId();
				}
			}
			return brokers.iterator().next();
		}
		return null;
	}
}
