package com.mawen.learn.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.common.message.MessageQueue;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public class AllocateMessageQueueAveragelyByCircle extends AbstractAllocateMessageQueueStrategy{

	@Override
	public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {

		List<MessageQueue> result = new ArrayList<>();
		if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
			return result;
		}

		int index = cidAll.indexOf(currentCID);
		for (int i = index; i < mqAll.size(); i++) {
			if (i % cidAll.size() == index) {
				result.add(mqAll.get(i));
			}
		}

		return result;
	}

	@Override
	public String getName() {
		return "AVG_BY_CIRCLE";
	}
}
