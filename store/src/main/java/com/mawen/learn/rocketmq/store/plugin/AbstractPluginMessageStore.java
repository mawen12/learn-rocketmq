package com.mawen.learn.rocketmq.store.plugin;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import com.mawen.learn.rocketmq.common.Pair;
import com.mawen.learn.rocketmq.common.SystemClock;
import com.mawen.learn.rocketmq.common.filter.MessageFilter;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageExtBatch;
import com.mawen.learn.rocketmq.common.message.MessageExtBrokerInner;
import com.mawen.learn.rocketmq.remoting.protocol.body.HARuntimeInfo;
import com.mawen.learn.rocketmq.store.AllocateMappedFileService;
import com.mawen.learn.rocketmq.store.AppendMessageResult;
import com.mawen.learn.rocketmq.store.CommitLog;
import com.mawen.learn.rocketmq.store.CommitLogDispatcher;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.GetMessageResult;
import com.mawen.learn.rocketmq.store.MessageStore;
import com.mawen.learn.rocketmq.store.PutMessageResult;
import com.mawen.learn.rocketmq.store.QueryMessageResult;
import com.mawen.learn.rocketmq.store.RunningFlags;
import com.mawen.learn.rocketmq.store.SelectMappedBufferResult;
import com.mawen.learn.rocketmq.store.StoreCheckPoint;
import com.mawen.learn.rocketmq.store.StoreStatService;
import com.mawen.learn.rocketmq.store.TransientStorePool;
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
import lombok.AllArgsConstructor;
import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@AllArgsConstructor
public abstract class AbstractPluginMessageStore implements MessageStore {

	protected MessageStore next;
	protected MessageStorePluginContext context;

	@Override
	public long getEarliestMessageTime() {
		return next.getEarliestMessageTime();
	}

	@Override
	public long lockTimeMills() {
		return next.lockTimeMills();
	}

	@Override
	public boolean isOSPageCacheBusy() {
		return next.isOSPageCacheBusy();
	}

	@Override
	public boolean isTransientStorePoolDeficient() {
		return next.isTransientStorePoolDeficient();
	}

	@Override
	public boolean load() {
		return next.load();
	}

	@Override
	public void start() throws Exception {
		next.start();
	}

	@Override
	public void shutdown() {
		next.shutdown();
	}

	@Override
	public void destroy() {
		next.destroy();
	}

	@Override
	public PutMessageResult putMessage(MessageExtBrokerInner msg) {
		return next.putMessage(msg);
	}

	@Override
	public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
		return next.asyncPutMessage(msg);
	}

	@Override
	public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBatch messageExtBatch) {
		return next.asyncPutMessage(messageExtBatch);
	}

	@Override
	public GetMessageResult getMessage(String group, String topic, int queueId, long offset, int maxMsgNums, MessageFilter messageFilter) {
		return next.getMessage(group, topic, queueId, offset, maxMsgNums, messageFilter);
	}

	@Override
	public CompletableFuture<GetMessageResult> getMessageAsync(String group, String topic, int queueId, long offset, int maxMsgNums, MessageFilter messageFilter) {
		return next.getMessageAsync(group, topic, queueId, offset, maxMsgNums, messageFilter);
	}

	@Override
	public long getMaxOffsetInQueue(String topic, int queueId) {
		return next.getMaxOffsetInQueue(topic, queueId);
	}

	@Override
	public long getMaxOffsetInQueue(String topic, int queueId, boolean committed) {
		return next.getMaxOffsetInQueue(topic, queueId, committed);
	}

	@Override
	public long getMinOffsetInQueue(String topic, int queueId) {
		return next.getMinOffsetInQueue(topic, queueId);
	}

	@Override
	public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
		return next.getCommitLogOffsetInQueue(topic, queueId, consumeQueueOffset);
	}

	@Override
	public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
		return next.getOffsetInQueueByTime(topic, queueId, timestamp);
	}

	@Override
	public MessageExt lookMessageByOffset(long commitLogOffset) {
		return next.lookMessageByOffset(commitLogOffset);
	}

	@Override
	public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
		return next.selectOneMessageByOffset(commitLogOffset);
	}

	@Override
	public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, long msgSize) {
		return next.selectOneMessageByOffset(commitLogOffset, msgSize);
	}

	@Override
	public String getRunningDataInfo() {
		return next.getRunningDataInfo();
	}

	@Override
	public HashMap<String, String> getRuntimeInfo() {
		return next.getRuntimeInfo();
	}

	@Override
	public long getMaxPhyOffset() {
		return next.getMaxPhyOffset();
	}

	@Override
	public long getMinPhyOffset() {
		return next.getMinPhyOffset();
	}

	@Override
	public long getEarliestMessageTime(String topic, int queueId) {
		return next.getEarliestMessageTime(topic, queueId);
	}

	@Override
	public CompletableFuture<Long> getEarliestMessageTimeAsync(String topic, int queueId) {
		return next.getEarliestMessageTimeAsync(topic, queueId);
	}

	@Override
	public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
		return next.getMessageStoreTimeStamp(topic, queueId, consumeQueueOffset);
	}

	@Override
	public CompletableFuture<Long> getMessageStoreTimeStampAsync(String topic, int queueId, long consumeQueueOffset) {
		return next.getMessageStoreTimeStampAsync(topic, queueId, consumeQueueOffset);
	}

	@Override
	public long getMessageTotalInQueue(String topic, int queueId) {
		return next.getMessageTotalInQueue(topic, queueId);
	}

	@Override
	public SelectMappedBufferResult getCommitLogData(long offset) {
		return next.getCommitLogData(offset);
	}

	@Override
	public boolean appendToCommitLog(long startOffset, byte[] data, int dataStart, int dataLength) {
		return next.appendToCommitLog(startOffset, data, dataStart, dataLength);
	}

	@Override
	public void executeDeleteFilesManually() {
		next.executeDeleteFilesManually();
	}

	@Override
	public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
		return next.queryMessage(topic, key, maxNum, begin, end);
	}

	@Override
	public CompletableFuture<QueryMessageResult> queryMessageAsync(String topic, String key, int maxNum, long begin, long end) {
		return next.queryMessageAsync(topic, key, maxNum, begin, end);
	}

	@Override
	public long now() {
		return next.now();
	}

	@Override
	public int deleteTopics(Set<String> deleteTopics) {
		return next.deleteTopics(deleteTopics);
	}

	@Override
	public int cleanUnusedTopic(Set<String> retainTopics) {
		return next.cleanUnusedTopic(retainTopics);
	}

	@Override
	public void cleanExpiredConsumeQueue() {
		next.cleanExpiredConsumeQueue();
	}

	@Override
	public boolean checkInDiskByConsumeOffset(String topic, int queueId, long consumeOffset) {
		return next.checkInDiskByConsumeOffset(topic, queueId, consumeOffset);
	}

	@Override
	public boolean checkInMemByConsumeOffset(String topic, int queueId, long consumeOffset, int batchSize) {
		return next.checkInMemByConsumeOffset(topic, queueId, consumeOffset, batchSize);
	}

	@Override
	public boolean checkInStoreByConsumeOffset(String topic, int queueId, long consumeOffset) {
		return next.checkInStoreByConsumeOffset(topic, queueId, consumeOffset);
	}

	@Override
	public long dispatchBehindBytes() {
		return next.dispatchBehindBytes();
	}

	@Override
	public long flush() {
		return next.flush();
	}

	@Override
	public boolean resetWriteOffset(long phyOffset) {
		return next.resetWriteOffset(phyOffset);
	}

	@Override
	public long getConfirmOffset() {
		return next.getConfirmOffset();
	}

	@Override
	public void setConfirmOffset(long phyOffset) {
		next.setConfirmOffset(phyOffset);
	}

	@Override
	public LinkedList<CommitLogDispatcher> getDispatcherList() {
		return next.getDispatcherList();
	}

	@Override
	public void addDispatcher(CommitLogDispatcher dispatcher) {
		next.addDispatcher(dispatcher);
	}

	@Override
	public ConsumeQueueInterface getConsumeQueue(String topic, int queueId) {
		return next.getConsumeQueue(topic, queueId);
	}

	@Override
	public ConsumeQueueInterface findConsumeQueue(String topic, int queueId) {
		return next.findConsumeQueue(topic, queueId);
	}

	@Override
	public BrokerStatsManager getBrokerStatsManager() {
		return next.getBrokerStatsManager();
	}

	@Override
	public int remainTransientStoreBufferNumbs() {
		return next.remainTransientStoreBufferNumbs();
	}

	@Override
	public long remainHowManyDataToCommit() {
		return next.remainHowManyDataToCommit();
	}

	@Override
	public long remainHowManyDataToFlush() {
		return next.remainHowManyDataToFlush();
	}

	@Override
	public DispatchRequest checkMessageAndReturnSize(ByteBuffer buffer, boolean checkCRC, boolean checkDupInfo, boolean readBody) {
		return next.checkMessageAndReturnSize(buffer, checkCRC, checkDupInfo, readBody);
	}

	@Override
	public long getStateMachineVersion() {
		return next.getStateMachineVersion();
	}

	@Override
	public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
		return next.putMessages(messageExtBatch);
	}

	@Override
	public HARuntimeInfo getHARuntimeInfo() {
		return next.getHARuntimeInfo();
	}

	@Override
	public boolean getLastMappedFile(long startOffset) {
		return next.getLastMappedFile(startOffset);
	}

	@Override
	public void updateHaMasterAddress(String newAddr) {
		next.updateHaMasterAddress(newAddr);
	}

	@Override
	public void updateMasterAddress(String newAddr) {
		next.updateMasterAddress(newAddr);
	}

	@Override
	public long slaveFailBehindMuch() {
		return next.slaveFailBehindMuch();
	}

	@Override
	public long getFlushedWhere() {
		return next.getFlushedWhere();
	}

	@Override
	public MessageStore getMasterStoreInProcess() {
		return next.getMasterStoreInProcess();
	}

	@Override
	public void setMasterStoreInProcess(MessageStore masterStoreInProcess) {
		next.setMasterStoreInProcess(masterStoreInProcess);
	}

	@Override
	public boolean getData(long offset, int size, ByteBuffer buffer) {
		return next.getData(offset, size, buffer);
	}

	@Override
	public void setAliveReplicaNumInGroup(int aliveReplicaNums) {
		next.setAliveReplicaNumInGroup(aliveReplicaNums);
	}

	@Override
	public int getAliveReplicaNumInGroup() {
		return next.getAliveReplicaNumInGroup();
	}

	@Override
	public void wakeupHAClient() {
		next.wakeupHAClient();
	}

	@Override
	public long getMasterFlushedOffset() {
		return next.getMasterFlushedOffset();
	}

	@Override
	public long getBrokerInitMaxOffset() {
		return next.getBrokerInitMaxOffset();
	}

	@Override
	public void setMasterFlushedOffset(long masterFlushedOffset) {
		next.setMasterFlushedOffset(masterFlushedOffset);
	}

	@Override
	public void setBrokerInitMaxOffset(long brokerInitMaxOffset) {
		next.setBrokerInitMaxOffset(brokerInitMaxOffset);
	}

	@Override
	public byte[] calcDeltaChecksum(long from, long to) {
		return next.calcDeltaChecksum(from, to);
	}

	@Override
	public HAService getHaService() {
		return next.getHaService();
	}

	@Override
	public boolean truncateFiles(long offsetToTruncate) throws RocksDBException {
		return next.truncateFiles(offsetToTruncate);
	}

	@Override
	public boolean isOffsetAligned(long offset) {
		return next.isOffsetAligned(offset);
	}

	@Override
	public RunningFlags getRunningFlags() {
		return next.getRunningFlags();
	}

	@Override
	public void setSendMessageBackHook(SendMessageBackHook sendMessageBackHook) {
		next.setSendMessageBackHook(sendMessageBackHook);
	}

	@Override
	public GetMessageResult getMessage(String group, String topic, int queueId, long offset, int maxMsgNums, int maxTotalMsgSize, MessageFilter messageFilter) {
		return next.getMessage(group, topic, queueId, offset, maxMsgNums, maxTotalMsgSize, messageFilter);
	}

	@Override
	public CompletableFuture<GetMessageResult> getMessageAsync(String group, String topic, int queueId, long offset, int maxMsgNums, int maxTotalMsgSize, MessageFilter messageFilter) {
		return next.getMessageAsync(group,topic,queueId,offset,maxMsgNums,messageFilter);
	}

	@Override
	public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
		return next.lookMessageByOffset(commitLogOffset,size);
	}

	@Override
	public List<SelectMappedBufferResult> getBulkCommitLogData(long offset, int size) {
		return next.getBulkCommitLogData(offset,size);
	}

	@Override
	public void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
		next.onCommitLogAppend(msg,result,commitLogFile);
	}

	@Override
	public void onCommitLogDispatch(DispatchRequest request, boolean doDispatch, MappedFile commitLogFile, boolean isRecover, boolean isFileEnd) throws RocksDBException {
		next.onCommitLogDispatch(request,doDispatch,commitLogFile,isRecover,isFileEnd);
	}

	@Override
	public MessageStoreConfig getMessageStoreConfig() {
		return next.getMessageStoreConfig();
	}

	@Override
	public StoreStatService getStoreStatsService() {
		return next.getStoreStatsService();
	}

	@Override
	public StoreCheckPoint getStoreCheckPoint() {
		return next.getStoreCheckPoint();
	}

	@Override
	public SystemClock getSystemClock() {
		return next.getSystemClock();
	}

	@Override
	public CommitLog getCommitLog() {
		return next.getCommitLog();
	}

	@Override
	public TransientStorePool getTransientStorePool() {
		return next.getTransientStorePool();
	}

	@Override
	public AllocateMappedFileService getAllocateMappedFileService() {
		return next.getAllocateMappedFileService();
	}

	@Override
	public void truncateDirtyLogicFiles(long phyOffset) throws RocksDBException {
		next.truncateDirtyLogicFiles(phyOffset);
	}

	@Override
	public void unlockMappedFile(MappedFile unlockMappedFile) {
		next.unlockMappedFile(unlockMappedFile);
	}

	@Override
	public PerfCounter.Ticks getPerfCounter() {
		return next.getPerfCounter();
	}

	@Override
	public ConsumeQueueStoreInterface getQueueStore() {
		return next.getQueueStore();
	}

	@Override
	public boolean isSyncDiskFlush() {
		return next.isSyncDiskFlush();
	}

	@Override
	public boolean isSyncMaster() {
		return next.isSyncMaster();
	}

	@Override
	public void assignOffset(MessageExtBrokerInner msg) throws RocksDBException {
		next.assignOffset(msg);
	}

	@Override
	public void increaseOffset(MessageExtBrokerInner msg, short messageNum) {
		next.increaseOffset(msg,messageNum);
	}

	@Override
	public List<PutMessageHook> getPutMessageHookList() {
		return next.getPutMessageHookList();
	}

	@Override
	public long getLastFileFromOffset() {
		return next.getLastFileFromOffset();
	}

	@Override
	public void setPhysicalOffset(long phyOffset) {
		next.setPhysicalOffset(phyOffset);
	}

	@Override
	public boolean isMappedFilesEmpty() {
		return next.isMappedFilesEmpty();
	}

	@Override
	public TimerMessageStore getTimerMessageStore() {
		return next.getTimerMessageStore();
	}

	@Override
	public void setTimerMessageStore(TimerMessageStore timerMessageStore) {
		next.setTimerMessageStore(timerMessageStore);
	}

	@Override
	public long getTimingMessageCount(String topic) {
		return next.getTimingMessageCount(topic);
	}

	@Override
	public boolean isShutdown() {
		return next.isShutdown();
	}

	@Override
	public long estimateMessageCount(String topic, int queueId, long from, long to, MessageFilter filter) {
		return next.estimateMessageCount(topic, queueId, from, to, filter);
	}

	@Override
	public List<Pair<InstrumentSelector, ViewBuilder>> getMetricsView() {
		return next.getMetricsView();
	}

	@Override
	public void initMetrics(Meter meter, Supplier<AttributesBuilder> attributesBuilderSupplier) {
		next.initMetrics(meter,attributesBuilderSupplier);
	}

	@Override
	public void finishCommitLogDispatch() {
		next.finishCommitLogDispatch();
	}

	@Override
	public void recoverTopicQueueTable() {
		next.recoverTopicQueueTable();
	}

	@Override
	public void notifyMessageArriveIfNecessary(DispatchRequest request) {
		next.notifyMessageArriveIfNecessary(request);
	}
}
