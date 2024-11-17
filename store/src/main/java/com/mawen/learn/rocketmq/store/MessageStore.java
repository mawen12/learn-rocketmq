package com.mawen.learn.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.SystemClock;
import com.mawen.learn.rocketmq.common.filter.MessageFilter;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageExtBatch;
import com.mawen.learn.rocketmq.common.message.MessageExtBrokerInner;
import com.mawen.learn.rocketmq.remoting.protocol.body.HARuntimeInfo;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import com.mawen.learn.rocketmq.store.ha.HAService;
import com.mawen.learn.rocketmq.store.hook.PutMessageHook;
import com.mawen.learn.rocketmq.store.hook.SendMessageBackHook;
import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import com.mawen.learn.rocketmq.store.queue.ConsumeQueueInterface;
import com.mawen.learn.rocketmq.store.queue.ConsumeQueueStoreInterface;
import com.mawen.learn.rocketmq.store.stats.BrokerStatsManager;
import com.mawen.learn.rocketmq.store.timer.TimerMessageStore;
import com.mawen.learn.rocketmq.store.util.PerfCounter;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.ViewBuilder;
import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
public interface MessageStore {

	boolean load();

	void start() throws Exception;

	void shutdown();

	void destroy();

	default CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
		return CompletableFuture.completedFuture(putMessage(msg));
	}

	default CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBatch messageExtBatch) {
		return CompletableFuture.completedFuture(putMessages(messageExtBatch));
	}

	PutMessageResult putMessage(final MessageExtBrokerInner msg);

	PutMessageResult putMessages(final MessageExtBatch messageExtBatch);

	GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final MessageFilter messageFilter);

	CompletableFuture<GetMessageResult> getMessageAsync(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final MessageFilter messageFilter);

	GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter);

	CompletableFuture<GetMessageResult> getMessageAsync(final String group, final String topic, final int queueId, final long offset, final int maxMsgNums, final int maxTotalMsgSize, final MessageFilter messageFilter);

	long getMaxOffsetInQueue(final String topic, final int queueId);

	long getMaxOffsetInQueue(final String topic, final int queueId, final boolean committed);

	long getMinOffsetInQueue(final String topic, final int queueId);

	TimerMessageStore getTimerMessageStore();

	void setTimerMessageStore(TimerMessageStore timerMessageStore);

	long getCommitLogOffsetInQueue(final String topic, final int queueId, final long consumeQueueOffset);

	long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp);

	long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp, final BoundaryType boundaryType);

	MessageExt lookMessageByOffset(final long commitLogOffset);

	MessageExt lookMessageByOffset(long commitLogOffset, int size);

	SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset);

	SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset, final long msgSize);

	String getRunningDataInfo();

	long getTimingMessageCount(String topic);

	HashMap<String, String> getRuntimeInfo();

	HARuntimeInfo getHARuntimeInfo();

	long getMaxPhyOffset();

	long getMinPhyOffset();

	long getEarliestMessageTime(final String topic, final int queueId);

	long getEarliestMessageTime();

	CompletableFuture<Long> getEarliestMessageTimeAsync(final String topic, final int queueId);

	long getMessageStoreTimeStamp(final String topic, final int queueId, final long consumeQueueOffset);

	CompletableFuture<Long> getMessageStoreTimeStampAsync(final String topic, final int queueId, final long consumeQueueOffset);

	long getMessageTotalInQueue(final String topic, final int queueId);

	SelectMappedBufferResult getCommitLogData(final long offset);

	List<SelectMappedBufferResult> getBulkCommitLogData(final long offset, final int size);

	boolean appendToCommitLog(final long startOffset, final byte[] data, final int dataStart, final int dataLength);

	void executeDeleteFilesManually();

	QueryMessageResult queryMessage(final String topic, final String key, final int maxNum, final long begin, final long end);

	CompletableFuture<QueryMessageResult> queryMessageAsync(final String topic, final String key, final int maxNum, final long begin, final long end);

	void updateHaMasterAddress(final String newAddr);

	void updateMasterAddress(final String newAddr);

	long slaveFailBehindMuch();

	long now();

	int deleteTopics(final Set<String> deleteTopics);

	int cleanUnusedTopic(final Set<String> retainTopics);

	void cleanExpiredConsumeQueue();

	boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset);

	boolean checkInMemByConsumeOffset(final String topic, final int queueId, long consumeOffset, int batchSize);

	boolean checkInStoreByConsumeOffset(final String topic, final int queueId, long consumeOffset);

	long dispatchBehindBytes();

	long flush();

	long getFlushedWhere();

	boolean resetWriteOffset(long phyOffset);

	long getConfirmOffset();

	void setConfirmOffset(long phyOffset);

	boolean isOSPageCacheBusy();

	long lockTimeMills();

	boolean isTransientStorePoolDeficient();

	LinkedList<CommitLogDispatcher> getDispatcherList();

	void addDispatcher(CommitLogDispatcher dispatcher);

	ConsumeQueueInterface getConsumeQueue(String topic, int queueId);

	ConsumeQueueInterface findConsumeQueue(String topic, int queueId);

	BrokerStatsManager getBrokerStatsManager();

	void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile);

	void onCommitLogDispatch(DispatchRequest request, boolean doDispatch, MappedFile commitLogFile, boolean isRecover, boolean isFileEnd) throws RocksDBException;

	void finishCommitLogDispatch();

	MessageStoreConfig getMessageStoreConfig();

	StoreStatService getStoreStatsService();

	StoreCheckPoint getStoreCheckPoint();

	SystemClock getSystemClock();

	CommitLog getCommitLog();

	RunningFlags getRunningFlags();

	TransientStorePool getTransientStorePool();

	HAService getHaService();

	AllocateMappedFileService getAllocateMappedFileService();

	void truncateDirtyLogicFiles(long phyOffset) throws RocksDBException;

	void unlockMappedFile(MappedFile unlockMappedFile);

	PerfCounter.Ticks getPerfCounter();

	ConsumeQueueStoreInterface getQueueStore();

	boolean isSyncDiskFlush();

	boolean isSyncMaster();

	void assignOffset(MessageExtBrokerInner msg) throws RocksDBException;

	void increaseOffset(MessageExtBrokerInner msg, short messageNum);

	MessageStore getMasterStoreInProcess();

	void setMasterStoreInProcess(MessageStore masterStoreInProcess);

	boolean getData(long offset, int size, ByteBuffer buffer);

	void setAliveReplicaNumInGroup(int aliveReplicaNums);

	int getAliveReplicaNumInGroup();

	void wakeupHAClient();

	long getMasterFlushedOffset();

	long getBrokerInitMaxOffset();

	void setMasterFlushedOffset(long masterFlushedOffset);

	void setBrokerInitMaxOffset(long brokerInitMaxOffset);

	byte[] calcDeltaChecksum(long from, long to);

	boolean truncateFiles(long offsetToTruncate) throws RocksDBException;

	boolean isOffsetAligned(long offset);

	List<PutMessageHook> getPutMessageHookList();

	void setSendMessageBackHook(SendMessageBackHook sendMessageBackHook);

	SendMessageBackHook getSendMessageHook();

	long getLastFileFromOffset();

	boolean getLastMappedFile(long startOffset);

	void setPhysicalOffset(long phyOffset);

	boolean isMappedFilesEmpty();

	long getStateMachineVersion();

	DispatchRequest checkMessageAndReturnSize(final ByteBuffer buffer, final boolean checkCRC, final boolean checkDupInfo, final boolean readBody);

	int remainTransientStoreBufferNumbs();

	long remainHowManyDataToCommit();

	long remainHowManyDataToFlush();

	boolean isShutdown();

	long estimateMessageCount(String topic, int queueId, long from, long to, MessageFilter filter);

	List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView();

	void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier);

	void recoverTopicQueueTable();

	void notifyMessageArriveIfNecessary(DispatchRequest request);
}
