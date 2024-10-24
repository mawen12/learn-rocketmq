package com.mawen.learn.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.common.message.MessageQueue;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy{

	@Override
	public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {

		List<MessageQueue> result = new ArrayList<>();
		if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
			return result;
		}

		int index = cidAll.indexOf(currentCID);
		int mod = mqAll.size() % cidAll.size();
		int averageSize = mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 : mqAll.size() / cidAll.size());

		int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
		int range = Math.min(averageSize, mqAll.size() - startIndex);
		for (int i = 0; i < range; i++) {
			result.add(mqAll.get(startIndex + i));
		}

		return result;
	}

	@Override
	public String getName() {
		return "AVG";
	}
}
