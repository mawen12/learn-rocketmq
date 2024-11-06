package com.mawen.learn.rocketmq.store.rocksdb;

import com.mawen.learn.rocketmq.common.config.ConfigRocksDBStorage;
import com.mawen.learn.rocketmq.store.MessageStore;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompactionStopStyle;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.IndexType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.util.SizeUnit;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
public class RocksDBOptionsFactory {

	public static ColumnFamilyOptions createCQCFOptions(final MessageStore messageStore) {
		BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig()
				.setFormatVersion(5)
				.setIndexType(IndexType.kBinarySearch)
				.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash)
				.setDataBlockHashTableUtilRatio(0.75)
				.setBlockSize(32 * SizeUnit.KB)
				.setMetadataBlockSize(4 * SizeUnit.KB)
				.setFilterPolicy(new BloomFilter(16, false))
				.setCacheIndexAndFilterBlocks(false)
				.setCacheIndexAndFilterBlocksWithHighPriority(true)
				.setPinL0FilterAndIndexBlocksInCache(false)
				.setPinTopLevelIndexAndFilter(true)
				.setBlockCache(new LRUCache(1024 * SizeUnit.MB, 8, false))
				.setWholeKeyFiltering(true);

		ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
		CompactionOptionsUniversal compactionOption = new CompactionOptionsUniversal()
				.setSizeRatio(100)
				.setMaxSizeAmplificationPercent(25)
				.setAllowTrivialMove(true)
				.setMinMergeWidth(2)
				.setMaxMergeWidth(Integer.MAX_VALUE)
				.setStopStyle(CompactionStopStyle.CompactionStopStyleTotalSize)
				.setCompressionSizePercent(-1);

		return new ColumnFamilyOptions()
				.setMaxWriteBufferNumber(4)
				.setWriteBufferSize(128 * SizeUnit.MB)
				.setMinWriteBufferNumberToMerge(1)
				.setTableFormatConfig(blockBasedTableConfig)
				.setMemTableConfig(new SkipListMemTableConfig())
				.setCompressionType(CompressionType.LZ4_COMPRESSION)
				.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
				.setNumLevels(7)
				.setCompactionStyle(CompactionStyle.UNIVERSAL)
				.setCompactionOptionsUniversal(compactionOption)
				.setMaxCompactionBytes(100 * SizeUnit.GB)
				.setSoftPendingCompactionBytesLimit(100 * SizeUnit.GB)
				.setHardPendingCompactionBytesLimit(256 * SizeUnit.GB)
				.setLevel0FileNumCompactionTrigger(2)
				.setLevel0SlowdownWritesTrigger(8)
				.setLevel0StopWritesTrigger(10)
				.setTargetFileSizeBase(256 * SizeUnit.MB)
				.setTargetFileSizeMultiplier(2)
				.setMergeOperator(new StringAppendOperator())
				.setCompactionFilterFactory(new ConsumeQueueCompactionFilterFactory(messageStore))
				.setReportBgIoStats(true)
				.setOptimizeFiltersForHits(true);
	}

	public static ColumnFamilyOptions createOffsetCFOptions() {
		BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig()
				.setFormatVersion(5)
				.setIndexType(IndexType.kBinarySearch)
				.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinarySearch)
				.setBlockSize(32 * SizeUnit.KB)
				.setFilterPolicy(new BloomFilter(16, false))
				.setCacheIndexAndFilterBlocks(false)
				.setCacheIndexAndFilterBlocksWithHighPriority(true)
				.setPinL0FilterAndIndexBlocksInCache(false)
				.setPinTopLevelIndexAndFilter(true)
				.setBlockCache(new LRUCache(128 * SizeUnit.MB, 8, false))
				.setWholeKeyFiltering(true);

		return new ColumnFamilyOptions()
				.setMaxWriteBufferNumber(4)
				.setWriteBufferSize(64 * SizeUnit.MB)
				.setMinWriteBufferNumberToMerge(1)
				.setTableFormatConfig(blockBasedTableConfig)
				.setMemTableConfig(new SkipListMemTableConfig())
				.setCompressionType(CompressionType.NO_COMPRESSION)
				.setNumLevels(7)
				.setCompactionStyle(CompactionStyle.LEVEL)
				.setLevel0FileNumCompactionTrigger(2)
				.setLevel0SlowdownWritesTrigger(8)
				.setLevel0StopWritesTrigger(10)
				.setTargetFileSizeBase(64 * SizeUnit.MB)
				.setMaxBytesForLevelBase(256 * SizeUnit.MB)
				.setMaxBytesForLevelMultiplier(2)
				.setMergeOperator(new StringAppendOperator())
				.setInplaceUpdateSupport(true);
	}

	public static DBOptions createDBOptions() {
		Statistics statistics = new Statistics();
		statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);

		return new DBOptions()
				.setDbLogDir(ConfigRocksDBStorage.getDBLogDir())
				.setInfoLogLevel(InfoLogLevel.INFO_LEVEL)
				.setWalRecoveryMode(WALRecoveryMode.PointInTimeRecovery)
				.setManualWalFlush(true)
				.setMaxTotalWalSize(0)
				.setWalSizeLimitMB(0)
				.setWalTtlSeconds(0)
				.setCreateIfMissing(true)
				.setCreateMissingColumnFamilies(true)
				.setMaxOpenFiles(-1)
				.setMaxLogFileSize(1 * SizeUnit.GB)
				.setKeepLogFileNum(5)
				.setMaxManifestFileSize(1 * SizeUnit.GB)
				.setAllowConcurrentMemtableWrite(false)
				.setStatistics(statistics)
				.setAtomicFlush(true)
				.setMaxBackgroundJobs(32)
				.setMaxSubcompactions(8)
				.setParanoidChecks(true)
				.setDelayedWriteRate(16 * SizeUnit.MB)
				.setRateLimiter(new RateLimiter(100 * SizeUnit.MB))
				.setUseDirectIoForFlushAndCompaction(false)
				.setUseDirectReads(false);
	}

}
