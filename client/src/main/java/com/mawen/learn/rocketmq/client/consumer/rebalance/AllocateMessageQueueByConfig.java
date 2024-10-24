package com.mawen.learn.rocketmq.client.consumer.rebalance;

import java.util.List;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
@Setter
public class AllocateMessageQueueByConfig extends AbstractAllocateMessageQueueStrategy{

	private List<MessageQueue> messageQueues;

	@Override
	public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> maAll, List<String> cidAll) {
		return this.messageQueues;
	}

	@Override
	public String getName() {
		return "CONFIG";
	}
}
