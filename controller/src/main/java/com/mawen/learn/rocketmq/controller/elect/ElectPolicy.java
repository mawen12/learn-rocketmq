package com.mawen.learn.rocketmq.controller.elect;

import java.util.Set;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
public interface ElectPolicy {

	Long elect(String clusterName, String brokerName, Set<Long> syncStateBrokers, Set<Long> allReplicaBrokers, Long oldMaster, Long brokerId);
}
