package com.mawen.learn.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.mawen.learn.rocketmq.common.message.MessageAccessor;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.remoting.protocol.body.ProcessQueueInfo;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
@Getter
@Setter
public class ProcessQueue {

	private static final Logger log = LoggerFactory.getLogger(ProcessQueue.class);

	public static final long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
	public static final long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
	public static final long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));

	private final ReadWriteLock treeMapLock = new ReentrantReadWriteLock();
	private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<>();
	private final AtomicLong msgCount = new AtomicLong();
	private final AtomicLong msgSize = new AtomicLong();
	private final ReadWriteLock consumeLock = new ReentrantReadWriteLock();
	private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<>();
	private final AtomicLong tryUnlockTimes = new AtomicLong(0);

	private volatile long queueOffsetMax = 0L;
	private volatile boolean dropped = false;
	private volatile long lastPullTimestamp = System.currentTimeMillis();
	private volatile long lastConsumeTimestamp = System.currentTimeMillis();
	private volatile boolean locked = false;
	private volatile long lastLockTimestamp = System.currentTimeMillis();
	private volatile boolean consuming = false;
	private volatile long msgAccCnt = 0L;

	public boolean isLockExpired() {
		return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
	}

	public boolean isPullExpired() {
		return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
	}

	public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
		if (pushConsumer.isConsumeOrderly()) {
			return;
		}

		int loop = Math.max(msgTreeMap.size(), 16);
		for (int i = 0; i < loop; i++) {
			MessageExt msg = null;
			try {
				this.treeMapLock.readLock().lockInterruptibly();
				try {
					if (!msgTreeMap.isEmpty()) {
						String consumeStartTimeStamp = MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue());
						if (StringUtils.isNotEmpty(consumeStartTimeStamp) && System.currentTimeMillis() - Long.parseLong(consumeStartTimeStamp) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
							msg = msgTreeMap.firstEntry().getValue();
						}
					}
				}
				finally {
					this.treeMapLock.readLock().unlock();
				}
			}
			catch (InterruptedException e) {
				log.error("getExpiredMsg exception", e);
			}

			if (msg == null) {
				break;
			}

			try {
				pushConsumer.sendMessageBack(msg, 3);
				log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());

				try {
					this.treeMapLock.writeLock().lockInterruptibly();
					try {
						if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
							try {
								removeMessage(Collections.singletonList(msg));
							}
							catch (Exception e) {
								log.error("send expired msg exception", e);
							}
						}
					}
					finally {
						this.treeMapLock.writeLock().unlock();
					}
				}
				catch (InterruptedException e) {
					log.error("getExpiredMsg exception", e);
				}
			}
			catch (Exception e) {
				log.error("getExpiredMsg exception", e);
			}
		}
	}

	public boolean putMessage(final List<MessageExt> msgs) {
		boolean dispatchToConsume = false;
		try {
			this.treeMapLock.writeLock().lockInterruptibly();
			try {
				int validMsgCnt = 0;
				for (MessageExt msg : msgs) {
					MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
					if (old == null) {
						validMsgCnt++;
						this.queueOffsetMax = msg.getQueueOffset();
						msgSize.addAndGet(msg.getBody().length);
					}
				}
				msgCount.addAndGet(validMsgCnt);

				if (!msgTreeMap.isEmpty() && !this.consuming) {
					dispatchToConsume = true;
					this.consuming = true;
				}

				if (!msgs.isEmpty()) {
					MessageExt messageExt = msgs.get(msgs.size() - 1);
					String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
					if (property != null) {
						long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
						if (accTotal > 0) {
							this.msgAccCnt = accTotal;
						}
					}
				}
			}
			finally {
				this.treeMapLock.writeLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("putMessage exception", e);
		}

		return dispatchToConsume;
	}

	public long getMaxSpan() {
		try {
			this.treeMapLock.readLock().lockInterruptibly();
			try {
				if (!this.msgTreeMap.isEmpty()) {
					return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
				}
			}
			finally {
				this.treeMapLock.readLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("getMaxSpan exception", e);
		}

		return 0;
	}

	public long removeMessage(final List<MessageExt> msgs) {
		long result = -1;
		final long now = System.currentTimeMillis();

		try {
			this.treeMapLock.writeLock().lockInterruptibly();
			this.lastConsumeTimestamp = now;

			try {
				if (!msgTreeMap.isEmpty()) {
					result = this.queueOffsetMax + 1;
					int removedCnt = 0;

					for (MessageExt msg : msgs) {
						MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
						if (prev != null) {
							removedCnt--;
							msgSize.addAndGet(-msg.getBody().length);
						}
					}

					if (!msgTreeMap.isEmpty()) {
						result = msgTreeMap.firstKey();
					}
				}
			}
			finally {
				this.treeMapLock.writeLock().unlock();
			}
		}
		catch (Throwable t) {
			log.error("removeMessage exception", t);
		}

		return result;
	}

	public void rollback() {
		try {
			this.treeMapLock.writeLock().lockInterruptibly();
			try {
				this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
				this.consumingMsgOrderlyTreeMap.clear();
			}
			finally {
				this.treeMapLock.writeLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("rollback Exception", e);
		}
	}

	public long commit() {
		try {
			this.treeMapLock.writeLock().lockInterruptibly();
			try {
				Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
				if (msgCount.addAndGet(-this.consumingMsgOrderlyTreeMap.size()) == 0) {
					msgSize.set(0);
				}
				else {
					for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
						msgSize.addAndGet(-msg.getBody().length);
					}
				}

				this.consumingMsgOrderlyTreeMap.clear();
				if (offset != null) {
					return offset + 1;
				}
			}
			finally {
				this.treeMapLock.writeLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("commit exception", e);
		}

		return -1;
	}

	public void makeMessageToConsumeAgain(List<MessageExt> msgs) {
		try {
			this.treeMapLock.writeLock().lockInterruptibly();
			try {
				for (MessageExt msg : msgs) {
					this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
					this.msgTreeMap.put(msg.getQueueOffset(), msg);
				}
			}
			finally {
				this.treeMapLock.writeLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("makeMessageToConsumeAgain exception", e);
		}
	}

	public List<MessageExt> takeMessages(final int batchSize) {
		List<MessageExt> result = new ArrayList<>(batchSize);
		final long now = System.currentTimeMillis();

		try {
			this.treeMapLock.writeLock().lockInterruptibly();
			this.lastConsumeTimestamp = now;
			try {
				if (!this.msgTreeMap.isEmpty()) {
					for (int i = 0; i < batchSize; i++) {
						Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
						if (entry != null) {
							result.add(entry.getValue());
							consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
						}
						else {
							break;
						}
					}
				}

				if (result.isEmpty()) {
					consuming = false;
				}
			}
			finally {
				this.treeMapLock.writeLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("take Messages exception", e);
		}

		return result;
	}

	public boolean containsMessage(MessageExt message) {
		if (message == null) {
			return false;
		}

		try {
			this.treeMapLock.readLock().lockInterruptibly();
			try {
				return this.msgTreeMap.containsKey(message.getQueueOffset());
			}
			finally {
				this.treeMapLock.readLock().unlock();
			}
		}
		catch (Throwable t) {
			log.error("Failed to check message's existence in process queue, message={}", message, t);
		}
		return false;
	}

	public boolean hasTempMessage() {
		try {
			this.treeMapLock.readLock().lockInterruptibly();
			try {
				return !this.msgTreeMap.isEmpty();
			}
			finally {
				this.treeMapLock.readLock().unlock();
			}
		}
		catch (InterruptedException ignored) {
		}
		return true;
	}

	public void clear() {
		try {
			this.treeMapLock.writeLock().lockInterruptibly();
			try {
				this.msgTreeMap.clear();
				this.consumingMsgOrderlyTreeMap.clear();
				this.msgCount.set(0);
				this.msgSize.set(0);
				this.queueOffsetMax = 0L;
			}
			finally {
				this.treeMapLock.writeLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("clear exception", e);
		}
	}

	public  void incTryUnlockTimes() {
		this.tryUnlockTimes.incrementAndGet();
	}

	public void fillProcessQueueInfo(final ProcessQueueInfo info) {
		try {
			this.treeMapLock.readLock().lockInterruptibly();

			if (!this.msgTreeMap.isEmpty()) {
				info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
				info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
				info.setCachedMsgCount(this.msgTreeMap.size());
			}
			info.setCachedMsgSizeInMiB(this.msgSize.get() / (1024 * 1024));

			if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
				info.setTransactionMsgMinOffset(consumingMsgOrderlyTreeMap.firstKey());
				info.setTransactionMsgMaxOffset(consumingMsgOrderlyTreeMap.lastKey());
				info.setTransactionMsgCount(consumingMsgOrderlyTreeMap.size());
			}

			info.setLocked(this.locked);
			info.setTryUnlockTimes(tryUnlockTimes.get());
			info.setLastLockTimestamp(lastLockTimestamp);

			info.setDroped(dropped);
			info.setLastPullTimestamp(lastPullTimestamp);
			info.setLastConsumeTimestamp(lastConsumeTimestamp);
		}
		catch (Exception ignored) {
		}
		finally {
			this.treeMapLock.readLock().unlock();
		}

	}
}
