package com.mawen.learn.rocketmq.store.kv;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.TopicConfig;
import com.mawen.learn.rocketmq.common.attribute.CleanupPolicy;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.CleanupPolicyUtils;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.GetMessageResult;
import com.mawen.learn.rocketmq.store.SelectMappedBufferResult;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import lombok.Getter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@Getter
public class CompactionStore {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	public static final String COMPACTION_DIR = "compaction";
	public static final String COMPACTION_LOG_DIR = "compactionLog";
	public static final String COMPACTION_CQ_DIR = "compactionCq";

	private final String compactionPath;
	private final String compactionLogPath;
	private final String compactionCqPath;
	private final DefaultMessageStore defaultMessageStore;
	private final CompactionPositionMgr positionMgr;
	private final ConcurrentHashMap<String, CompactionLog> compactionLogTable;
	private final ScheduledExecutorService compactionSchedule;
	private final int scanInterval = 30000;
	private final int compactionInterval;
	private final int compactionThreadNum;
	private final int offsetMapSize;
	private String masterAddr;

	public CompactionStore(DefaultMessageStore defaultMessageStore) {
		this.defaultMessageStore = defaultMessageStore;
		this.compactionLogTable = new ConcurrentHashMap<>();
		MessageStoreConfig config = defaultMessageStore.getMessageStoreConfig();
		String storeRootPath = config.getStorePathRootDir();
		this.compactionPath = Paths.get(storeRootPath, COMPACTION_DIR).toString();
		this.compactionLogPath = Paths.get(compactionPath, COMPACTION_LOG_DIR).toString();
		this.compactionCqPath = Paths.get(compactionPath, COMPACTION_CQ_DIR).toString();
		this.positionMgr = new CompactionPositionMgr(compactionPath);
		this.compactionThreadNum = Math.min(Runtime.getRuntime().availableProcessors(), Math.max(1, config.getCompactionThreadNum()));
		this.compactionSchedule = ThreadUtils.newScheduledThreadPool(compactionThreadNum, new ThreadFactoryImpl("compactionSchedule_"));
		this.offsetMapSize = config.getMaxOffsetMapSize() / compactionThreadNum;
		this.compactionInterval = defaultMessageStore.getMessageStoreConfig().getCompactionScheduleInternal();
	}

	public void load(boolean exitOK) throws Exception {
		File logRoot = new File(compactionLogPath);
		File[] fileTopicList = logRoot.listFiles();
		if (fileTopicList != null) {
			for (File fileTopic : fileTopicList) {
				if (!fileTopic.isDirectory()) {
					continue;
				}

				File[] fileQueueIdList = fileTopic.listFiles();
				if (fileQueueIdList != null) {
					for (File fileQueueId : fileQueueIdList) {
						if (!fileQueueId.isDirectory()) {
							continue;
						}

						try {
							String topic = fileTopic.getName();
							int queueId = Integer.parseInt(fileQueueId.getName());

							if (Files.isDirectory(Paths.get(compactionCqPath, topic, String.valueOf(queueId)))) {
								loadAndGetClog(topic, queueId);
							}
							else {
								log.error("{}:{} compactionLog mismatch with compactionCq", topic, queueId);
							}
						}
						catch (Exception e) {
							log.error("load compactionLog {}:{} exception: ", fileTopic.getName(), fileQueueId.getName(), e);
							throw new Exception("load compactionLog " + fileTopic.getName() + ":" + fileQueueId.getName() + " exception: " + e.getMessage());
						}
					}
				}
			}
		}
		log.info("compactionStore {}:{} load completed.", compactionLogPath, compactionCqPath);

		compactionSchedule.scheduleWithFixedDelay(this::scanAllTopicConfig, scanInterval, scanInterval, TimeUnit.MILLISECONDS);
		log.info("loop to scan all topicConfig with fixed delay {}ms", scanInterval);
	}

	private void scanAllTopicConfig() {
		log.info("start to scan all topicConfig");
		try {
			Iterator<Map.Entry<String, TopicConfig>> iterator = defaultMessageStore.getTopicConfigs().entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<String, TopicConfig> it = iterator.next();
				TopicConfig topicConfig = it.getValue();
				CleanupPolicy policy = CleanupPolicyUtils.getDeletePolicy(Optional.ofNullable(topicConfig));
				if (Objects.equals(policy, CleanupPolicy.COMPACTION)) {
					for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
						loadAndGetClog(it.getKey(), i);
					}
				}
			}
		}
		catch (Throwable ignored) {}
		log.info("scan all topicConfig over");
	}

	private CompactionLog loadAndGetClog(String topic, int queueId) {
		CompactionLog clog = compactionLogTable.compute(topic + "_" + queueId, (k, v) -> {
			if (v == null) {
				try {
					v = new CompactionLog(defaultMessageStore, this, topic, queueId);
					v.load(true);
					int randomDelay = 1000 + new Random(System.currentTimeMillis()).nextInt(compactionInterval);
					compactionSchedule.scheduleWithFixedDelay(v::doCompaction, compactionInterval + randomDelay, compactionInterval + randomDelay, TimeUnit.MILLISECONDS);
				}
				catch (IOException e) {
					log.error("create compactionLog exception: ", e);
					return null;
				}
			}
			return v;
		});
		return clog;
	}

	public void putMessage(String topic, int queueId, SelectMappedBufferResult result) {
		CompactionLog clog = loadAndGetClog(topic, queueId);
		if (clog != null) {
			clog.asyncPutMessage(result);
		}
	}

	public void doDispatch(DispatchRequest request, SelectMappedBufferResult result) {
	 	CompactionLog clog = loadAndGetClog(request.getTopic(), request.getQueueId());
		if (clog != null) {
			clog.asyncPutMessage(result.getByteBuffer(), request);
		}
	}

	public GetMessageResult getMessage(String group, String topic, int queueId, long offset, int maxMsgNums, int maxTotalMsgSize) {
		CompactionLog log = compactionLogTable.get(topic + "_" + queueId);
		return log != null ? log.getMessage(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize) : GetMessageResult.NO_MATCHED_LOGIC_QUEUE;
	}

	public void flush(int flushLeastPages) {
		compactionLogTable.values().forEach(log -> log.flush(flushLeastPages));
	}

	public void flushLog(int flushLeastPages) {
		compactionLogTable.values().forEach(log -> log.flushLog(flushLeastPages));
	}

	public void flushCQ(int flushLeastPages) {
		compactionLogTable.values().forEach(log -> log.flushCQ(flushLeastPages));
	}

	public void updateMasterAddress(String addr) {
		masterAddr = addr;
	}

	public void shutdown() {
		compactionSchedule.shutdown();
		try {
			if (!compactionSchedule.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
				List<Runnable> droppedTasks = compactionSchedule.shutdownNow();
				log.warn("compactionSchedule was abruptly shutdown. {} tasks will not be executed.", droppedTasks.size());
			}
		}
		catch (InterruptedException e) {
			log.warn("wait compaction schedule shutdown interrupted.");
		}
		flush(0);
		positionMgr.persist();
	}
}
