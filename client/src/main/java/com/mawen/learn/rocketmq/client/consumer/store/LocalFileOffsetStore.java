package com.mawen.learn.rocketmq.client.consumer.store;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq.client.exception.MQBrokerException;
import com.mawen.learn.rocketmq.client.exception.MQClientException;
import com.mawen.learn.rocketmq.client.impl.factory.MQClientInstance;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.help.FAQUrl;
import com.mawen.learn.rocketmq.common.message.MessageQueue;
import com.mawen.learn.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/23
 */
public class LocalFileOffsetStore implements OffsetStore {

	private static final Logger log = LoggerFactory.getLogger(LocalFileOffsetStore.class);

	private final static String LOCAL_OFFSET_STORE_DIR = System.getProperty("rocketmq.client.localOffsetStoreDir", System.getProperty("user.home") + File.separator + ".rocketmq_offsets");

	private final MQClientInstance mqClientFactory;
	private final String groupName;
	private final String storePath;
	private ConcurrentMap<MessageQueue, ControllableOffset> offsetTable = new ConcurrentHashMap<>();

	public LocalFileOffsetStore(MQClientInstance mqClientFactory, String groupName) {
		this.mqClientFactory = mqClientFactory;
		this.groupName = groupName;
		this.storePath = LOCAL_OFFSET_STORE_DIR + File.separator
				+ this.mqClientFactory.getClientId() + File.separator
				+ this.groupName + File.separator
				+ "offsets.json";
	}

	@Override
	public void load() throws MQClientException {
		OffsetSerializeWrapper wrapper = this.readLocalOffset();

		if (wrapper != null && wrapper.getOffsetTable() != null) {
			for (Map.Entry<MessageQueue, AtomicLong> entry : wrapper.getOffsetTable().entrySet()) {

				AtomicLong offset = entry.getValue();
				offsetTable.put(entry.getKey(), new ControllableOffset(offset.get()));
				log.info("load consumer's offset, {} {} {}", this.groupName, entry.getKey(), offset.get());
			}
		}
	}

	@Override
	public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
		if (mq != null) {
			ControllableOffset offsetOld = this.offsetTable.get(mq);
			if (offsetOld == null) {
				offsetOld = this.offsetTable.putIfAbsent(mq, new ControllableOffset(offset));
			}

			if (offsetOld != null) {
				offsetOld.update(offset, increaseOnly);
			}
		}
	}

	@Override
	public void updateAndFreezeOffset(MessageQueue mq, long offset) {
		if (mq != null) {
			this.offsetTable.computeIfAbsent(mq, k -> new ControllableOffset(offset))
					.updateAndFreeze(offset);
		}
	}

	@Override
	public long readOffset(MessageQueue mq, ReadOffsetType type) {
		if (mq != null) {
			switch (type) {
				case MEMORY_FIRST_THEN_STORE:
				case READ_FROM_MEMORY: {
					ControllableOffset offset = this.offsetTable.get(mq);
					if (offset != null) {
						return offset.getOffset();
					}
					else if (ReadOffsetType.READ_FROM_MEMORY == type) {
						return -1;
					}
				}
				case READ_FROM_STORE: {
					OffsetSerializeWrapper wrapper;
					try {
						wrapper = this.readLocalOffset();
					}
					catch (MQClientException e) {
						return -1;
					}

					if (wrapper != null && wrapper.getOffsetTable() != null) {
						AtomicLong offset = wrapper.getOffsetTable().get(mq);
						if (offset != null) {
							this.updateOffset(mq, offset.get(), false);
							return offset.get();
						}
					}
				}
				default:
					break;
			}
		}

		return -1;
	}

	@Override
	public void removeOffset(MessageQueue mq) {
		if (mq != null) {
			this.offsetTable.remove(mq);
			log.info("remove unnecessary messageQueue offset, group={}, mq={}, offsetTableSize={}", this.groupName, mq, offsetTable.size());
		}
	}

	@Override
	public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
		Map<MessageQueue, Long> cloneOffsetTable = new HashMap<>(this.offsetTable.size(), 1);
		for (Map.Entry<MessageQueue, ControllableOffset> entry : this.offsetTable.entrySet()) {
			MessageQueue mq = entry.getKey();
			if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
				continue;
			}
			cloneOffsetTable.put(mq, entry.getValue().getOffset());
		}

		return cloneOffsetTable;
	}

	@Override
	public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

	}

	@Override
	public void persist(MessageQueue mq) {
		if (mq == null) {
			return;
		}

		ControllableOffset offset = this.offsetTable.get(mq);
		if (offset != null) {
			OffsetSerializeWrapper wrapper = null;
			try {
				wrapper = readLocalOffset();
			}
			catch (MQClientException e) {
				log.error("readLocalOffset exception", e);
				return;
			}

			if (wrapper == null) {
				wrapper = new OffsetSerializeWrapper();
			}

			wrapper.getOffsetTable().put(mq, new AtomicLong(offset.getOffset()));
			String jsonStr = wrapper.toJson(true);
			if (jsonStr != null) {
				try {
					MixAll.string2File(jsonStr, this.storePath);
				}
				catch (IOException e) {
					log.error("persist consumer offset exception, {}", this.storePath, e);
				}
			}
		}
	}

	@Override
	public void persistAll(Set<MessageQueue> mqs) {
		if (mqs == null || mqs.isEmpty()) {
			return;
		}

		OffsetSerializeWrapper wrapper = null;
		try {
			wrapper = readLocalOffset();
		}
		catch (MQClientException e) {
			log.error("readLocalOffset exception", e);
			return;
		}

		if (wrapper == null) {
			wrapper = new OffsetSerializeWrapper();
		}

		for (Map.Entry<MessageQueue, ControllableOffset> entry : this.offsetTable.entrySet()) {
			if (mqs.contains(entry.getKey())) {
				AtomicLong offset = new AtomicLong(entry.getValue().getOffset());
				wrapper.getOffsetTable().put(entry.getKey(), offset);
			}
		}

		String jsonStr = wrapper.toJson(true);
		if (jsonStr != null) {
			try {
				MixAll.string2File(jsonStr, this.storePath);
			}
			catch (IOException e) {
				log.error("persistAll consumer offset Exception, {}", this.storePath, e);
			}
		}
	}

	private OffsetSerializeWrapper readLocalOffset() throws MQClientException {
		String content = null;
		try {
			content = MixAll.file2String(this.storePath);
		}
		catch (IOException e) {
			log.warn("Load local offset store file exception", e);
		}

		if (content == null || content.length() == 0) {
			return readLocalOffsetBak();
		}
		else {
			OffsetSerializeWrapper wrapper = null;
			try {
				wrapper = OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
			}
			catch (Exception e) {
				log.warn("readLocalOffset Exception, and try to correct", e);
				return this.readLocalOffsetBak();
			}
			return wrapper;
		}
	}

	private OffsetSerializeWrapper readLocalOffsetBak() throws MQClientException {
		String content = null;
		try {
			content = MixAll.file2String(this.storePath + ".bak");
		}
		catch (IOException e) {
			log.warn("Load local offset store bak file exception", e);
		}

		if (content != null && content.length() > 0) {
			OffsetSerializeWrapper wrapper = null;
			try {
				wrapper = OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
			}
			catch (Exception e) {
				log.warn("readLocalOffset exception", e);
				throw new MQClientException("readLocalOffset exception, maybe fastjson version too low" + FAQUrl.suggestTodo(FAQUrl.LOAD_JSON_EXCEPTION), e);
			}
			return wrapper;
		}

		return null;
	}
}
