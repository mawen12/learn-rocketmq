package com.mawen.learn.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Setter
@Getter
public class AllocateMessageQueueByMachineRoom extends AbstractAllocateMessageQueueStrategy{

	private Set<String> consumeridcs;

	@Override
	public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {

		List<MessageQueue> result = new ArrayList<>();
		if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
			return result;
		}

		int currentIndex = cidAll.indexOf(currentCID);
		if (currentIndex < 0) {
			return result;
		}

		List<MessageQueue> premqAll = new ArrayList<>();
		for (MessageQueue mq : mqAll) {
			String[] temp = mq.getBrokerName().split("@");
			if (temp.length == 2 && consumeridcs.contains(temp[0])) {
				premqAll.add(mq);
			}
		}

		int mod = premqAll.size() / cidAll.size();
		int rem = premqAll.size() % cidAll.size();
		int startIndex = mod * currentIndex;
		int endIndex = startIndex + mod;

		for (int i = startIndex; i < endIndex; i++) {
			result.add(premqAll.get(i));
		}

		if (rem > currentIndex) {
			result.add(premqAll.get(currentIndex + mod * cidAll.size()));
		}

		return result;
	}

	@Override
	public String getName() {
		return "MACHINE_ROOM";
	}
}
