package com.mawen.learn.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.mawen.learn.rocketmq.common.consistenthash.ConsistentHashRouter;
import com.mawen.learn.rocketmq.common.consistenthash.HashFunction;
import com.mawen.learn.rocketmq.common.consistenthash.Node;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import lombok.RequiredArgsConstructor;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
public class AllocateMessageQueueConsistentHash extends AbstractAllocateMessageQueueStrategy{

	private final int virtualNodeCnt;
	private final HashFunction customHashFunction;

	public AllocateMessageQueueConsistentHash() {
		this(10);
	}

	public AllocateMessageQueueConsistentHash(int virtualNodeCnt) {
		this(virtualNodeCnt, null);
	}

	public AllocateMessageQueueConsistentHash(int virtualNodeCnt, HashFunction customHashFunction) {
		if (virtualNodeCnt < 0) {
			throw new IllegalArgumentException("illegal virtualNodeCnt :" + virtualNodeCnt);
		}
		this.virtualNodeCnt = virtualNodeCnt;
		this.customHashFunction = customHashFunction;
	}

	@Override
	public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
		List<MessageQueue> result = new ArrayList<>();
		if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
			return result;
		}

		Collection<ClientNode> cidNodes = new ArrayList<>();
		for (String cid : cidAll) {
			cidNodes.add(new ClientNode(cid));
		}

		final ConsistentHashRouter<ClientNode> router;
		if (customHashFunction != null) {
			router = new ConsistentHashRouter<>(cidNodes, virtualNodeCnt, customHashFunction);
		}
		else {
			router = new ConsistentHashRouter<>(cidNodes, virtualNodeCnt);
		}

		for (MessageQueue mq : mqAll) {
			ClientNode clientNode = router.routeNode(mq.toString());
			if (clientNode != null && currentCID.equals(clientNode.getKey())) {
				result.add(mq);
			}
		}
		return result;
	}

	@Override
	public String getName() {
		return "CONSISTENT_HASH";
	}

	@RequiredArgsConstructor
	private static class ClientNode implements Node {
		private final String clientID;

		@Override
		public String getKey() {
			return clientID;
		}
	}
}
