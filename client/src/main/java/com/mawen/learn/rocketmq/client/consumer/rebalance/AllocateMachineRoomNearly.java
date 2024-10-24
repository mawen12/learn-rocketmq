package com.mawen.learn.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.mawen.learn.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public class AllocateMachineRoomNearly extends AbstractAllocateMessageQueueStrategy{

	private final AllocateMessageQueueStrategy delegate;
	private final MachineRoomResolver machineRoomResolver;

	public AllocateMachineRoomNearly(AllocateMessageQueueStrategy delegate, MachineRoomResolver machineRoomResolver) {
		if (delegate == null) {
			throw new NullPointerException("allocateMessageQueueStrategy is null");
		}
		if (machineRoomResolver == null) {
			throw new NullPointerException("machineRoomResolver is null");
		}

		this.delegate = delegate;
		this.machineRoomResolver = machineRoomResolver;
	}

	@Override
	public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {

		List<MessageQueue> result = new ArrayList<>();
		if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
			return result;
		}

		Map<String, List<MessageQueue>> mr2Mq = new TreeMap<>();
		for (MessageQueue mq : mqAll) {
			String brokerMachineRoom = machineRoomResolver.brokerDeployIn(mq);
			if (StringUtils.isNotEmpty(brokerMachineRoom)) {
				mr2Mq.computeIfAbsent(brokerMachineRoom, k -> new ArrayList<>()).add(mq);
			}
			else {
				throw new IllegalArgumentException("Machine room is null for mq " + mq);
			}
		}

		Map<String, List<String>> mr2c = new TreeMap<>();
		for (String cid : cidAll) {
			String consumerMachineRoom = machineRoomResolver.consumerDeployIn(cid);
			if (StringUtils.isNotEmpty(consumerMachineRoom)) {
				mr2c.computeIfAbsent(consumerMachineRoom, k -> new ArrayList<>()).add(cid);
			}
			else {
				throw new IllegalArgumentException("Machine room is null for consumer id " + cid);
			}
		}

		List<MessageQueue> allocateResults = new ArrayList<>();

		String currentMachineRoom = machineRoomResolver.consumerDeployIn(currentCID);
		List<MessageQueue> mqInThisMachineRoom = mr2Mq.remove(currentMachineRoom);
		List<String> consumerInThisMachineRoom = mr2c.get(currentMachineRoom);
		if (CollectionUtils.isNotEmpty(mqInThisMachineRoom)) {
			allocateResults.addAll(delegate.allocate(consumerGroup, currentCID, mqAll, consumerInThisMachineRoom));
		}

		for (Map.Entry<String, List<MessageQueue>> entry : mr2Mq.entrySet()) {
			if (!mr2c.containsKey(entry.getKey())) {
				allocateResults.addAll(delegate.allocate(consumerGroup, currentCID, entry.getValue(), cidAll));
			}
		}

		return allocateResults;
	}

	@Override
	public String getName() {
		return "MACHINE_ROOM_NEARBY-" + delegate.getName();
	}

	public interface MachineRoomResolver {
		String brokerDeployIn(MessageQueue messageQueue);

		String consumerDeployIn(String clientID);
	}

}
