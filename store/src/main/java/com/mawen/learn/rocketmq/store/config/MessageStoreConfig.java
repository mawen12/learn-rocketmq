package com.mawen.learn.rocketmq.store.config;

import java.io.File;

import com.mawen.learn.rocketmq.common.annotation.ImportantField;
import com.mawen.learn.rocketmq.store.ConsumeQueue;
import com.mawen.learn.rocketmq.store.StoreType;
import com.sun.jna.WString;
import lombok.Getter;
import lombok.Setter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
@Getter
@Setter
public class MessageStoreConfig {

	public static final String MULTI_PATH_SPLIITER = System.getProperty("rocketmq.broker.multiPathSplitter", ",");

	@ImportantField
	private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";

	@ImportantField
	private String storePathCommitLog;

	@ImportantField
	private String storePathDLedgerCommitLog;

	@ImportantField
	private String storePathEpochFile;

	@ImportantField
	private String readOnlyCommitLogStorePaths;

	private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;

	private int compactionMappedFileSize = 100 * 1024 * 1024;

	private int compactionCqMappedFileSize = 10 * 1024 * 1024;

	private int compactionScheduleInternal = 15 * 50 * 1000;

	private int maxOffsetMapSize = 100 * 1024 * 1024;

	private int compactionThreadNum = 6;

	private boolean enableCompaction = true;

	private int mappedFileSizeTimerLog = 100 * 1024 * 1024;

	private int timerPrecisionMs = 1000;

	private int timerRollWindowSlot = 2 * 24 * 3600;

	private int timerFlushIntervalMs = 1000;

	private int timerGetMessageThreadNum = 3;

	private int timerPutMessageThreadNum = 3;

	private boolean timerEnableDisruptor = false;

	private boolean timerEnableCheckMetrics = true;

	private boolean timerInterceptDelayLevel = false;

	private int timerMaxDelaySec = 3600 * 24 * 3;

	private boolean timerWheelEnable = true;

	@ImportantField
	private int disappearTimeAfterStart = -1;

	private boolean timerStopEnqueue = false;

	private String timerCheckMetricsWhen = "05";

	private boolean timerSkipUnknownError = false;
	private boolean timerWarmEnable = false;
	private boolean timerStopDequeue = false;
	private int timerCongestNumEachSlot = Integer.MAX_VALUE;

	private int timerMetricSmallThreshold = 1000000;
	// ---
	private int timerProgressLogIntervalMs = 10 * 1000;
	@ImportantField
	private String storeType = StoreType.DEFAULT.getStoreType();
	private int mappedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;
	private boolean enableConsumeQueueExt = false;
	private int mappedFileSizeConsumeQueueExt = 48 * 1024 * 1024;
	private int mapperFileSizeBatchConsumeQueue = 300000 * BatchConsumeQueue.CQ_STORE_UNIT_SIZE;
	private int bitMapLengthConsumeQueueExt = 64;
	@ImportantField
	private int flushIntervalCommitLog = 500;
	@ImportantField
	private int commitIntervalCommitLog = 200;
	private int maxRecoveryCommitLogFiles = 30;
	private int diskSpaceWarningLevelRatio = 90;
	private int diskSpaceCleanForciblyRatio = 85;

	private boolean useReentrantLockWhenPutMessage = true;
	@ImportantField
	private boolean flushCommitLogTimed = true;
	private int flushIntervalConsumeQueue = 1000;
	private int cleanResourceInterval = 10000;
	private int deleteCommitLogFilesInterval = 100;
	private int deleteConsumeQueueFilesInterval = 100;
	private int destroyMapedFileIntervalForcibly = 120 * 1000;
	private int redeleteHangedFileInterval = 120 * 1000;
	@ImportantField
	private String deleteWhen = "04";
	private int diskMaxUsedSpaceRatio = 75;
	@ImportantField
	private int fileReservedTime =72;
	@ImportantField
	private int deleteFileBatchMax = 10;
	private int putMsgIndexHightWater = 600000;
	private int maxMessageSize = 4 * 1024 * 1024;
	private int maxFilterMessageSize = 16000;
	private boolean checkCRCOnRecover = true;
	private int flushCommitLogLeastPages = 4;
	private int commitCommitLogLeastPages = 4;
	private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;
	private int flushConsumeQueueLeastPages = 2;
	private int flushCommitLogThoroughInterval = 10 * 1000;
	private int commitCommitLogThoroughInterval = 200;
	private int flushConsumeQueueThoroughInterval = 60 * 1000;
	@ImportantField
	private int maxTransferBytesOnMessageInMemory = 256 * 1024;
	@ImportantField
	private int maxTransferCountOnMessageInMemory = 32;
	@ImportantField
	private int maxTransferBytesOnMessageInDisk = 64 * 1024;
	@ImportantField
	private int maxTransferCountOnMessageInDisk = 8;
	@ImportantField
	private int accessMessageInMemoryMaxRatio = 40;
	@ImportantField
	private boolean messageIndexEnable = true;
	private int maxHashSlotNum = 5000000;
	private int maxIndexNum = 4 * 5000000;
	private int maxMsgsNumBatch = 64;
	@ImportantField
	private boolean messageIndexStore = false;
	private int haSendHeartbeartInterval = 5 * 1000;
	private int haHouseKeepingInterval = 20 * 1000;
	private int haTransferBatchSize = 32 * 1024;
	@ImportantField
	private String haMasterAddress;
	private int haMaxGapNotInSync = 256 * 1024 * 1024;
	@ImportantField
	private volatile BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;
	@ImportantField
	private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;
	private int syncFlushTimeout = 5 * 1000;
	private int putMessageTimeout = 8 * 1000;
	private int slaveTimeout = 3000;
	private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
	private long flushDelayOffsetInterval = 10 * 1000;
	@ImportantField
	private boolean cleanFileForciblyEnable = true;
	private boolean warmMapedFileEnable = true;
	private boolean offsetCheckInSlave = false;
	private boolean debugLockEnable = false;
	private boolean duplicationEnable = false;
	private boolean diskFallRecorded = true;
	private long osPageCacheBusyTimeOutMills = 1000;
	private int defaultQueryMaxNum = 32;

	@ImportantField
	private boolean transientStorePoolEnable = false;
	private int transientStorePoolSize =5;
	private boolean fastFailIfNoBufferInStorePool = false;

	private boolean enableDLegerCommitLog = false;
	private String dLegerGroup;
	private String dLegerPeers;
	private String dLegerSelfId;
	private String preferredLeaderId;
	private boolean enableBatchPush = false;

	private boolean enableScheduleMessageStats = true;

	private boolean enableLmq = false;
	private boolean enableMultiDispatch = false;
	private int maxLmqConsumeQueueNum = 20000;

	private boolean enableScheduleAsyncDeliver = false;
	private int scheduleAsyncDeliverMaxPendingLimit = 2000;
	private int scheduleAsyncDeliverMaxResendNum2Blocked = 3;

	private int maxBatchDeleteFilesNum = 50;
	private int dispatchCqThreads = 10;
	private int dispatchCqCacheNUm = 4 * 1024;
	private boolean enableAsyncReput = true;

	private boolean recheckReputOffsetFromCq = false;

	private int maxTopicLength = Byte.MAX_VALUE;

	private boolean autoMessageVersionOnTopicLen = true;

	private boolean enabledAppendPropCRC = false;
	private boolean forceVerifyPropCRC = false;
	private int travelCqFileNumWhenGetMessage = 1;

	private int correctLogicMinOffsetSleepInterval = 1;
	private int correctLogicMinOffsetForceInterval = 5 * 60 * 1000;
	private boolean mappedFileSwapEnable = true;
	private long commitLogForceMapInterval = 12 * 60 * 60 * 1000;
	private long commitLogSwapMapInterval = 1 * 60 * 60 * 1000;
	private int commitLogSwapMapReserveFileNum = 100;
	private long logicQueueForceSwapMapInterval = 12 * 60 * 60 * 1000;
	private long logicQueueSwapMapInterval = 1 * 60 * 60 * 1000;
	private long cleanSwapedMapInterval = 5 * 60 * 1000;
	private int logicQueueSwapMapReserveFileNum = 20;

	private boolean searchBcqByCacheEnable = true;

	@ImportantField
	private boolean dispatchFromSenderThread = false;

	@ImportantField
	private boolean wakeCommitWhenPutMessage = true;

	@ImportantField
	private boolean wakeFlushWhenPutMessage = false;

	@ImportantField
	private boolean enableCleanExpiredOffset = false;

	private int maxAsyncPutMessageRequests = 5000;

	private int pullBatchMaxMessageCount = 160;

	@ImportantField
	private int totalReplicas = 1;

	@ImportantField
	private int inSyncReplicas = 1;

	@ImportantField
	private int minInSyncReplicas = 1;

	@ImportantField
	private boolean allAckInSyncStateSet = false;

	@ImportantField
	private boolean enableAuthInSyncReplicas = false;

	@ImportantField
	private boolean haFlowControlEnable = false;

	private long maxHaTransferByteInSecond = 100 * 1024 * 1024;

	private long haMaxTimeSlaveNoCatchup = 15 * 1000;

	private boolean syncMasterFlushOffsetWhenStartup = false;

	private long maxChecksumRange = 1024 * 1024 * 1024;

	private int replicasPerDiskPartition = 1;

	private double logicDiskSpaceCleanForciblyThreshold = 0.8;

	private long maxSlaveResendLength = 256 * 1024 * 1024;

	private boolean syncFromLastFile = false;

	private boolean asyncLearner = false;

	private int maxConsumeQueueScan = 20_000;

	private int sampleCountThreshold = 5000;

	private boolean coldDataFlowControlEnable = false;
	private boolean coldDataScanEnable = false;
	private boolean dataReadAheadEnable = true;
	private int timerColdDataCheckIntervalMs = 60 * 1000;
	private int sampleSteps = 32;
	private int accessMessageInMemoryHotRatio = 26;

	private boolean enableBuildConsumeQueueConcurrently = false;

	private int batchDispatchRequestThreadPoolNums = 16;

	private long cleanRocksDBDirtyCQIntervalMin = 60;
	private long statRocksDBCQIntervalSec = 10;
	private long memTableFlushIntervalMs = 60 * 60 * 1000L;
	private boolean realTimePersistRocksDBConfig = true;
	private boolean enableRocksDBLog = false;

	private int topicQueueLockNum = 32;
}
