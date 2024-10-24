package com.mawen.learn.rocketmq.client.producer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.message.Message;
import com.mawen.learn.rocketmq.common.message.MessageBatch;
import com.mawen.learn.rocketmq.common.message.MessageClientIDSetter;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageDecoder;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/24
 */
@Getter
public class ProduceAccumulator {

	private static final Logger log = LoggerFactory.getLogger(ProduceAccumulator.class);

	private long totalHoldSize = 32 * 1024 * 1024;
	private long holdSize = 32 * 1024;
	private int holdMs = 10;

	private final GuardForSyncSendService guardThreadForSyncSend;
	private final GuardForAsyncSendService guardThreadForAsyncSend;
	private Map<AggregateKey, MessageAccumulation> syncSendBatchs = new ConcurrentHashMap<>();
	private Map<AggregateKey, MessageAccumulation> asyncSendBatchs = new ConcurrentHashMap<>();
	private AtomicLong currentlyHoldSize = new AtomicLong(0L);
	private final String instanceName;

	public ProduceAccumulator(String instanceName) {
		this.instanceName = instanceName;
		this.guardThreadForSyncSend = new GuardForSyncSendService(this.instanceName);
		this.guardThreadForAsyncSend = new GuardForAsyncSendService(this.instanceName);
	}

	void start() {
		this.guardThreadForSyncSend.start();
		this.guardThreadForAsyncSend.start();
	}

	void shutdown() {
		this.guardThreadForSyncSend.shutdown();
		this.guardThreadForAsyncSend.shutdown();
	}

	int getBatchMaxDelayMs() {
		return holdMs;
	}

	void batchMaxDelayMs(int holdMs) {
		if (holdMs <= 0 || holdMs > 30 * 1000) {
			throw new IllegalArgumentException(String.format("batchMaxDelayMs expect between 1ms and 30s, but get %d!", holdMs));
		}
		this.holdMs = holdMs;
	}

	long getBatchMaxBytes() {
		return holdSize;
	}

	void batchMaxBytes(long totalHoldSize) {
		if (totalHoldSize <= 0 || totalHoldSize > 2 * 1024 * 1024) {
			throw new IllegalArgumentException(String.format("batchMaxBytes expect between 1B and 2MB, but get %d!", totalHoldSize));
		}
		this.holdSize = totalHoldSize;
	}

	private MessageAccumulation getOrCreateSyncSendBatch(AggregateKey aggregateKey, DefaultMQProducer defaultMQProducer) {
		return syncSendBatchs.computeIfAbsent(aggregateKey, k -> new MessageAccumulation(aggregateKey, defaultMQProducer));
	}

	private MessageAccumulation getOrCreateAsyncSendBatch(AggregateKey aggregateKey, DefaultMQProducer defaultMQProducer) {
		return asyncSendBatchs.computeIfAbsent(aggregateKey, k -> new MessageAccumulation(aggregateKey, defaultMQProducer));
	}

	SendResult send(Message msg, DefaultMQProducer defaultMQProducer) throws InterruptedException {
		AggregateKey partitionKey = new AggregateKey(msg);
		while (true) {
			MessageAccumulation batch = getOrCreateSyncSendBatch(partitionKey, defaultMQProducer);
			int index = batch.add(msg);
			if (index == -1) {
				syncSendBatchs.remove(partitionKey, batch);
			}
			else {
				return batch.sendResults[index];
			}
		}
	}

	SendResult send(Message msg, MessageQueue mq, DefaultMQProducer defaultMQProducer) throws InterruptedException {
		AggregateKey partitionKey = new AggregateKey(msg, mq);
		while (true) {
			MessageAccumulation batch = getOrCreateSyncSendBatch(partitionKey, defaultMQProducer);
			int index = batch.add(msg);
			if (index == -1) {
				syncSendBatchs.remove(partitionKey, batch);
			}
			else {
				return batch.sendResults[index];
			}
		}
	}

	void send(Message msg, SendCallback callback, DefaultMQProducer defaultMQProducer) {
		AggregateKey partitionKey = new AggregateKey(msg);
		while (true) {
			MessageAccumulation batch = getOrCreateAsyncSendBatch(partitionKey, defaultMQProducer);
			if (!batch.add(msg, callback)) {
				asyncSendBatchs.remove(partitionKey, batch);
			}
			else {
				return;
			}
		}
	}

	void send(Message msg, MessageQueue mq, SendCallback callback, DefaultMQProducer defaultMQProducer) {
		AggregateKey partitionKey = new AggregateKey(msg);
		while (true) {
			MessageAccumulation batch = getOrCreateAsyncSendBatch(partitionKey, defaultMQProducer);
			if (!batch.add(msg, callback)) {
				asyncSendBatchs.remove(partitionKey, batch);
			}
			else {
				return;
			}
		}
	}

	boolean tryAddMessage(Message message) {
		synchronized (currentlyHoldSize) {
			if (currentlyHoldSize.get() < totalHoldSize) {
				currentlyHoldSize.addAndGet(message.getBody().length);
				return true;
			}
			else {
				return false;
			}
		}
	}

	private class GuardForSyncSendService extends ServiceThread {

		private final String serviceName;

		public GuardForSyncSendService(String clientInstanceName) {
			this.serviceName = String.format("Client_%s_GuardForSyncSend", clientInstanceName);
		}

		@Override
		public String getServiceName() {
			return serviceName;
		}

		@Override
		public void run() {
			log.info("{} service started", this.getServiceName());

			while (!this.isStopped()) {
				try {
					this.doWork();
				}
				catch (Exception e) {
					log.warn("{} service has exception.", this.getServiceName(), e);
				}
			}

			log.info("{} service end", this.getServiceName());
		}

		private void doWork() throws InterruptedException {
			Collection<MessageAccumulation> values = syncSendBatchs.values();
			int sleepTime = Math.max(1, holdMs / 2);

			for (MessageAccumulation v : values) {
				v.wakeup();
				synchronized (v) {
					synchronized (v.closed) {
						if (v.messagesSize.get() == 0) {
							v.closed.set(true);
							syncSendBatchs.remove(v.aggregateKey, v);
						}
						else {
							v.notify();
						}
					}
				}
			}

			Thread.sleep(sleepTime);
		}
	}

	private class GuardForAsyncSendService extends ServiceThread {

		private final String serviceName;

		public GuardForAsyncSendService(String clientInstanceName) {
			this.serviceName = String.format("Client_%s_GuardForAsyncSend", clientInstanceName);
		}

		@Override
		public String getServiceName() {
			return serviceName;
		}

		@Override
		public void run() {
			log.info("{} service started", this.getServiceName());

			while (!this.isStopped()) {
				try {
					this.doWork();
				}
				catch (Exception e) {
					log.warn("{} service has exception", this.getServiceName(), e);
				}
			}

			log.info("{} service end", this.getServiceName());
		}

		private void doWork() throws InterruptedException {
			Collection<MessageAccumulation> values = asyncSendBatchs.values();
			int sleepTime = Math.max(1, holdMs / 2);

			for (MessageAccumulation v : values) {
				if (v.readyToSend()) {
					v.send(null);
				}

				synchronized (v.closed) {
					if (v.messagesSize.get() == 0) {
						v.closed.set(true);
						asyncSendBatchs.remove(v.aggregateKey, v);
					}
				}
			}

			Thread.sleep(sleepTime);
		}
	}

	@EqualsAndHashCode
	@AllArgsConstructor
	private class AggregateKey {
		public String topic;
		public MessageQueue mq;
		public boolean waitStoreMsgOK;
		public String tag;

		public AggregateKey(Message message) {
			this(message.getTopic(), null, message.isWaitStoreMsgOK(), message.getTags());
		}

		public AggregateKey(Message message, MessageQueue mq) {
			this(message.getTopic(), mq, message.isWaitStoreMsgOK(), message.getTags());
		}
	}

	private class MessageAccumulation {
		private final DefaultMQProducer defaultMQProducer;
		private LinkedList<Message> messages;
		private LinkedList<SendCallback> sendCallbacks;
		private Set<String> keys;
		private AtomicBoolean closed;
		private SendResult[] sendResults;
		private AggregateKey aggregateKey;
		private AtomicInteger messagesSize;
		private int count;
		private long createTime;

		public MessageAccumulation(AggregateKey aggregateKey, DefaultMQProducer defaultMQProducer) {
			this.defaultMQProducer = defaultMQProducer;
			this.messages = new LinkedList<>();
			this.sendCallbacks = new LinkedList<>();
			this.keys = new HashSet<>();
			this.closed = new AtomicBoolean(false);
			this.messagesSize = new AtomicInteger(0);
			this.aggregateKey = aggregateKey;
			this.count = 0;
			this.createTime = System.currentTimeMillis();
		}

		private boolean readyToSend() {
			return this.messagesSize.get() > holdMs || System.currentTimeMillis() >= this.createTime + holdMs;
		}

		public int add(Message msg) throws InterruptedException {
			int ret = -1;
			synchronized (this.closed) {
				if (this.closed.get()) {
					return ret;
				}

				ret = this.count++;
				this.messages.add(msg);
				this.messagesSize.addAndGet(msg.getBody().length);

				String msgKeys = msg.getKeys();
				if (msgKeys != null) {
					this.keys.addAll(Arrays.asList(msgKeys.split(MessageConst.KEY_SEPARATOR)));
				}
			}

			synchronized (this) {
				while (!this.closed.get()) {
					if (readyToSend()) {
						this.send();
						break;
					}
					else {
						this.wait();
					}
				}
				return ret;
			}
		}

		public boolean add(Message msg, SendCallback callback) {
			synchronized (this.closed) {
				if (this.closed.get()) {
					return false;
				}

				this.count++;
				this.messages.add(msg);
				this.sendCallbacks.add(callback);
				this.messagesSize.getAndAdd(msg.getBody().length);
			}

			if (readyToSend()) {
				this.send(callback);
			}
			return true;
		}

		public synchronized void wakeup() {
			if (this.closed.get()) {
				return;
			}
			this.notify();
		}

		private MessageBatch batch() {
			MessageBatch batch = new MessageBatch(this.messages);
			batch.setTopic(this.aggregateKey.topic);
			batch.setWaitStoreMsgOK(this.aggregateKey.waitStoreMsgOK);
			batch.setKeys(this.keys);
			batch.setTags(this.aggregateKey.tag);
			MessageClientIDSetter.setUniqID(batch);
			batch.setBody(MessageDecoder.encodeMessages(this.messages));
			return batch;
		}

		private void splitSendResults(SendResult result) {
			if (result == null) {
				throw new IllegalArgumentException("sendResult is null");
			}

			this.sendResults = new SendResult[this.count];
			boolean isBatchConsumerQueue = !result.getMsgId().contains(".");
			if (!isBatchConsumerQueue) {
				String[] msgIds = result.getMsgId().split(",");
				String[] offsetMsgIds = result.getOffsetMsgId().split(",");
				if (offsetMsgIds.length != this.count || msgIds.length != this.count) {
					throw new IllegalArgumentException("sendResult is illegal");
				}

				for (int i = 0; i < this.count; i++) {
					this.sendResults[i] = new SendResult(result.getSendStatus(), msgIds[i], result.getMessageQueue(), result.getQueueOffset() + i, result.getTransactionId(), offsetMsgIds[i], result.getRegionId());
				}
			}
			else {
				for (int i = 0; i < count; i++) {
					this.sendResults[i] = result;
				}
			}
		}

		private void send() {
			synchronized (this.closed) {
				if (this.closed.getAndSet(true)) {
					return;
				}
			}

			MessageBatch batch = this.batch();
			SendResult result = null;
			try {
				if (defaultMQProducer != null) {
					result = defaultMQProducer.sendDirect(batch, aggregateKey.mq, null);
					this.splitSendResults(result);
				}
				else {
					throw new IllegalArgumentException("defaultMQProducer is null, can not send message");
				}
			}
			finally {
				currentlyHoldSize.addAndGet(-messagesSize.get());
				this.notifyAll();
			}
		}

		private void send(SendCallback sendCallback) {
			synchronized (this.closed) {
				if (this.closed.getAndSet(true)) {
					return;
				}
			}

			MessageBatch batch = this.batch();
			SendResult result = null;
			try {
				if (defaultMQProducer != null) {
					final int size = messagesSize.get();
					defaultMQProducer.sendDirect(batch, aggregateKey.mq, new SendCallback() {
						@Override
						public void onSuccess(SendResult sendResult) {
							try {
								splitSendResults(sendResult);
								int i = 0;
								Iterator<SendCallback> it = sendCallbacks.iterator();

								while (it.hasNext()) {
									SendCallback v = it.next();
									v.onSuccess(sendResults[i++]);
								}

								if (i != count) {
									throw new IllegalArgumentException("sendResult is illegal");
								}
								currentlyHoldSize.addAndGet(-size);
							}
							catch (Exception e) {
								onException(e);
							}
						}

						@Override
						public void onException(Throwable e) {
							for (SendCallback callback : sendCallbacks) {
								callback.onException(e);
							}
							currentlyHoldSize.addAndGet(-size);
						}
					});
				}
			}
			catch (Exception e) {
				for (SendCallback callback : sendCallbacks) {
					callback.onException(e);
				}
			}
		}
	}
}
