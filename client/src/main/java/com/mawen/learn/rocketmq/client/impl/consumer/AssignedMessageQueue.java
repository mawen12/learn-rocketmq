package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.mawen.learn.rocketmq.common.message.MessageQueue;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/28
 */
@Setter
@Getter
public class AssignedMessageQueue {

	private final ConcurrentMap<MessageQueue, MessageQueueState> assignedMessageQueeState;
	private RebalanceImpl rebalanceImpl;

	public AssignedMessageQueue() {
		this.assignedMessageQueeState = new ConcurrentHashMap<>();
	}

	public boolean isPaused(MessageQueue messageQueue) {
		MessageQueueState messageQueueState = assignedMessageQueeState.get(messageQueue);
		if (messageQueueState != null) {
			return messageQueueState.isPaused();
		}
		return true;
	}

	public void pause(Collection<MessageQueue> messageQueues) {
		for (MessageQueue mq : messageQueues) {
			MessageQueueState mqs = assignedMessageQueeState.get(mq);
			if (mqs != null) {
				mqs.setPaused(true);
			}
		}
	}

	public void resume(Collection<MessageQueue> messageQueues) {
		for (MessageQueue mq : messageQueues) {
			MessageQueueState mqs = assignedMessageQueeState.get(mq);
			if (mqs != null) {
				mqs.setPaused(true);
			}
		}
	}

	public ProcessQueue getProcessQueue(MessageQueue mq) {
		MessageQueueState mqs = assignedMessageQueeState.get(mq);
		return mqs != null ? mqs.getProcessQueue() : null;
	}

	public long getPullOffset(MessageQueue mq) {
		MessageQueueState mqs = assignedMessageQueeState.get(mq);
		return mqs != null ? mqs.getPullOffset() : -1;
	}

	public void updatePullOffset(MessageQueue mq, long offset, ProcessQueue pq) {
		MessageQueueState mqs = assignedMessageQueeState.get(mq);
		if (mqs != null) {
			if (mqs.getProcessQueue() != pq) {
				return;
			}
			mqs.setPullOffset(offset);
		}
	}

	public long getConsumerOffset(MessageQueue mq) {
		MessageQueueState mqs = assignedMessageQueeState.get(mq);
		return mqs != null ? mqs.getConsumeOffset() : -1;
	}

	public void updateConsumeOffset(MessageQueue mq, long offset) {
		MessageQueueState mqs = assignedMessageQueeState.get(mq);
		if (mqs != null) {
			mqs.setConsumeOffset(offset);
		}
	}

	public void setSeekOffset(MessageQueue mq, long offset) {
		MessageQueueState mqs = assignedMessageQueeState.get(mq);
		if (mqs != null) {
			mqs.setSeekOffset(offset);
		}
	}

	public long getSeekOffset(MessageQueue mq) {
		MessageQueueState mqs = assignedMessageQueeState.get(mq);
		return mqs != null ? mqs.getSeekOffset() : -1;
	}

	public void updateAssignedMessagedQueue(String topic, Collection<MessageQueue> assigned) {
		synchronized (this.assignedMessageQueeState) {
			Iterator<Map.Entry<MessageQueue, MessageQueueState>> it = this.assignedMessageQueeState.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<MessageQueue, MessageQueueState> next = it.next();
				if (next.getKey().getTopic().equals(topic)) {
					if (assigned.contains(next.getKey())) {
						next.getValue().getProcessQueue().setDropped(true);
						it.remove();
					}
				}
			}
			addAssignedMessageQueue(assigned);
		}
	}

	public void updateAssignedMessageQueue(Collection<MessageQueue> assigned) {
		synchronized (this.assignedMessageQueeState) {
			Iterator<Map.Entry<MessageQueue, MessageQueueState>> iterator = this.assignedMessageQueeState.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<MessageQueue, MessageQueueState> next = iterator.next();
				if (!assigned.contains(next.getKey())) {
					next.getValue().getProcessQueue().setDropped(true);
					iterator.remove();
				}
			}
			addAssignedMessageQueue(assigned);
		}
	}

	public void removeAssignedMessageQueue(String topic) {
		synchronized (this.assignedMessageQueeState) {
			Iterator<Map.Entry<MessageQueue, MessageQueueState>> it = this.assignedMessageQueeState.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<MessageQueue, MessageQueueState> next = it.next();
				if (next.getKey().getTopic().contains(topic)) {
					it.remove();
				}
			}
		}
	}

	public Set<MessageQueue> getAssignedMessageQueues() {
		return this.assignedMessageQueeState.keySet();
	}

	private void addAssignedMessageQueue(Collection<MessageQueue> assigned) {
		for (MessageQueue mq : assigned) {
			if (!this.assignedMessageQueeState.containsKey(mq)) {
				MessageQueueState mqs;
				if (rebalanceImpl != null && rebalanceImpl.getProcessQueueTable().get(mq) != null) {
					mqs = new MessageQueueState(mq, rebalanceImpl.getProcessQueueTable().get(mq));
				}
				else {
					mqs = new MessageQueueState(mq, new ProcessQueue());
				}
				this.assignedMessageQueeState.put(mq, mqs);
			}
		}
	}

	@Getter
	@Setter
	private class MessageQueueState {
		private MessageQueue messageQueue;
		private ProcessQueue processQueue;
		private volatile boolean paused = false;
		private volatile long pullOffset = -1;
		private volatile long consumeOffset = -1;
		private volatile long seekOffset = -1;

		public MessageQueueState(MessageQueue messageQueue, ProcessQueue processQueue) {
			this.messageQueue = messageQueue;
			this.processQueue = processQueue;
		}
	}
}
