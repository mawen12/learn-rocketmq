package com.mawen.learn.rocketmq.client.consumer.rebalance;

import java.util.List;

import com.mawen.learn.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public abstract class AbstractAllocateMessageQueueStrategy implements AllocateMessageQueueStrategy {

	private static final Logger log = LoggerFactory.getLogger(AbstractAllocateMessageQueueStrategy.class);

	public boolean check(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
		if (StringUtils.isEmpty(currentCID)) {
			throw new IllegalArgumentException("currentCID is empty");
		}
		if (CollectionUtils.isEmpty(mqAll)) {
			throw new IllegalArgumentException("mqAll is null or empty");
		}
		if (CollectionUtils.isEmpty(cidAll)) {
			throw new IllegalArgumentException("cidAll is null or empty");
		}

		if (!cidAll.contains(currentCID)) {
			log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
					consumerGroup, currentCID, cidAll);
			return false;
		}
		return true;
	}
}
