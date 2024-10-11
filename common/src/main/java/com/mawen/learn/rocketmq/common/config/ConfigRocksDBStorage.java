package com.mawen.learn.rocketmq.common.config;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.common.UtilAll;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.CompactionOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.IndexType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class ConfigRocksDBStorage extends AbstractRocksDBStorage{

	public ConfigRocksDBStorage(final String dbPath) {
		super();
		this.dbPath = dbPath;
		this.readOnly = false;
	}

	public ConfigRocksDBStorage(final String dbPath, boolean readOnly) {
		super();
		this.dbPath = dbPath;
		this.readOnly = readOnly;
	}

	public static String getDBLogDir() {
		String rootPath = System.getProperty("user.home");
		if (StringUtils.isEmpty(rootPath)) {
			return "";
		}

		rootPath += File.separator + "logs";
		UtilAll.ensureDirOK(rootPath);
		return rootPath + File.separator + "rocketmqlogs" + File.separator;
	}

	@Override
	protected boolean postLoad() {
		try {
			UtilAll.ensureDirOK(this.dbPath);

			initOptions();

			final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
			ColumnFamilyOptions defaultOptions = createConfigOptions();
			this.cfOptions.add(defaultOptions);
			cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));

			final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
			open(cfDescriptors, cfHandles);

			this.defaultCFHandle = cfHandles.get(0);
		}
		catch (Exception e) {
			log.error("postLoad Failed: {}", this.dbPath, e);
			return false;
		}
		return false;
	}

	@Override
	protected void preShutdown() {

	}

	public void put(final byte[] keyBytes, final int keyLen, final byte[] valueBytes) throws RocksDBException {
		put(this.defaultCFHandle, this.ableWalWriteOptions, keyBytes, keyLen, valueBytes, valueBytes.length);
	}

	public void put(final ByteBuffer keyBB, final ByteBuffer valueBB) throws RocksDBException {
		put(this.defaultCFHandle, this.ableWalWriteOptions, keyBB, valueBB);
	}

	public byte[] get(final byte[] keyBytes) throws RocksDBException {
		return get(this.defaultCFHandle, this.totalOrderReadOptions, keyBytes);
	}

	public void delete(final byte[] keyBytes) throws RocksDBException {
		delete(this.defaultCFHandle, this.ableWalWriteOptions, keyBytes);
	}

	public List<byte[]> multiGet(final List<ColumnFamilyHandle> chFirst, final List<byte[]> keys) throws RocksDBException {
		return multiGet(this.totalOrderReadOptions, chFirst, keys);
	}

	public void batchPut(final WriteBatch batch) throws RocksDBException {
		batchPut(this.writeOptions, batch);
	}

	public void batchPutWithWal(final WriteBatch batch) throws RocksDBException {
		batchPut(this.ableWalWriteOptions, batch);
	}

	public RocksIterator iterator() {
		return this.db.newIterator(this.defaultCFHandle, this.totalOrderReadOptions);
	}

	public void rangeDelete(final byte[] startKey, final byte[] endKey) throws RocksDBException {
		rangeDelete(this.defaultCFHandle, this.writeOptions, startKey, endKey);
	}

	public RocksIterator iterator(ReadOptions readOptions) {
		return this.db.newIterator(this.defaultCFHandle, readOptions);
	}

	private void initOptions() {
		this.options = createConfigDBOptions();

		this.writeOptions = new WriteOptions()
				.setSync(false)
				.setDisableWAL(true)
				.setNoSlowdown(true);

		this.ableWalWriteOptions = new WriteOptions()
				.setSync(false)
				.setDisableWAL(false)
				.setNoSlowdown(true);

		this.readOptions = new ReadOptions()
				.setPrefixSameAsStart(true)
				.setTotalOrderSeek(false)
				.setTailing(false);

		this.totalOrderReadOptions = new ReadOptions()
				.setPrefixSameAsStart(false)
				.setTotalOrderSeek(false)
				.setTailing(false);

		this.compactRangeOptions = new CompactRangeOptions()
				.setBottommostLevelCompaction(CompactRangeOptions.BottommostLevelCompaction.kForce)
				.setAllowWriteStall(true)
				.setExclusiveManualCompaction(false)
				.setChangeLevel(true)
				.setTargetLevel(-1)
				.setMaxSubcompactions(4);

		this.compactionOptions = new CompactionOptions()
				.setCompression(CompressionType.LZ4_COMPRESSION)
				.setMaxSubcompactions(4)
				.setOutputFileSizeLimit(4 * 1024 * 1024 * 1024L);
	}

	private ColumnFamilyOptions createConfigOptions() {
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
				.setBlockCache(new LRUCache(4 * SizeUnit.MB, 8, false))
				.setWholeKeyFiltering(true);

		return new ColumnFamilyOptions()
				.setMaxWriteBufferNumber(2)
				.setWriteBufferSize(8 * SizeUnit.MB)
				.setMinWriteBufferNumberToMerge(1)
				.setTableFormatConfig(blockBasedTableConfig)
				.setMemTableConfig(new SkipListMemTableConfig())
				.setCompressionType(CompressionType.NO_COMPRESSION)
				.setNumLevels(7)
				.setCompactionStyle(CompactionStyle.LEVEL)
				.setLevel0FileNumCompactionTrigger(4)
				.setLevel0SlowdownWritesTrigger(8)
				.setLevel0StopWritesTrigger(12)
				.setTargetFileSizeBase(64 * SizeUnit.MB)
				.setTargetFileSizeMultiplier(2)
				.setMaxBytesForLevelBase(256 * SizeUnit.MB)
				.setMaxBytesForLevelMultiplier(2)
				.setMergeOperator(new StringAppendOperator())
				.setInplaceUpdateSupport(true);
	}

	private DBOptions createConfigDBOptions() {
		Statistics statistics = new Statistics();
		statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);

		return new DBOptions()
				.setDbLogDir(getDBLogDir())
				.setInfoLogLevel(InfoLogLevel.INFO_LEVEL)
				.setWalRecoveryMode(WALRecoveryMode.SkipAnyCorruptedRecords)
				.setManualWalFlush(true)
				.setMaxTotalWalSize(500 * SizeUnit.MB)
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
				.setStatsDumpPeriodSec(600)
				.setAtomicFlush(true)
				.setMaxBackgroundJobs(32)
				.setMaxSubcompactions(4)
				.setParanoidChecks(true)
				.setDelayedWriteRate(16 * SizeUnit.MB)
				.setRateLimiter(new RateLimiter(100 * SizeUnit.MB))
				.setUseDirectIoForFlushAndCompaction(true)
				.setUseDirectReads(true);

	}

}
