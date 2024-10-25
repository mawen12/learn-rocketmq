package com.mawen.learn.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.mawen.learn.rocketmq.client.common.ThreadLocalIndex;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.protocol.route.QueueData;
import com.mawen.learn.rocketmq.remoting.protocol.route.TopicRouteData;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/18
 */
@Setter
@Getter
@ToString
public class TopicPublishInfo {
	private boolean orderTopic = false;
	private boolean haveTopicRouterInfo = false;
	private List<MessageQueue> messageQueueList = new ArrayList<>();
	private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
	private TopicRouteData topicRouteData;

	public MessageQueue selectOneMessageQueue(QueueFilter... filters) {
		return selectOneMessageQueue(this.messageQueueList, this.sendWhichQueue, filters);
	}

	private MessageQueue selectOneMessageQueue(List<MessageQueue> messageQueueList, ThreadLocalIndex sendQueue, QueueFilter... filters) {
		if (messageQueueList == null || messageQueueList.isEmpty()) {
			return null;
		}

		if (filters != null && filters.length != 0) {
			for (int i = 0; i < messageQueueList.size(); i++) {
				int index = Math.abs(sendQueue.incrementAndGet() % messageQueueList.size());
				MessageQueue mq = messageQueueList.get(index);
				boolean filterResult = true;

				for (QueueFilter filter : filters) {
					Preconditions.checkNotNull(filter);
					filterResult &= filter.filter(mq);
				}

				if (filterResult) {
					return mq;
				}
			}
			return null;
		}

		return selectOneMessageQueue();
	}

	public void resetIndex() {
		this.sendWhichQueue.reset();
	}

	public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
		if (lastBrokerName != null) {
			for (int i = 0; i < messageQueueList.size(); i++) {
				MessageQueue mq = selectOneMessageQueue();
				if (!mq.getBrokerName().equals(lastBrokerName)) {
					return mq;
				}
			}
		}
		return selectOneMessageQueue();
	}

	public MessageQueue selectOneMessageQueue() {
		int index = this.sendWhichQueue.incrementAndGet();
		int pos = index % messageQueueList.size();

		return this.messageQueueList.get(pos);
	}

	public int getWriteQueueNumsByBroker(final String brokerName) {
		for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
			QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
			if (queueData.getBrokerName().equals(brokerName)) {
				return queueData.getWriteQueueNums();
			}
		}
		return -1;
	}

	public boolean ok() {
		return this.messageQueueList != null && !this.messageQueueList.isEmpty();
	}

	public interface QueueFilter {
		boolean filter(MessageQueue mq);
	}
}
