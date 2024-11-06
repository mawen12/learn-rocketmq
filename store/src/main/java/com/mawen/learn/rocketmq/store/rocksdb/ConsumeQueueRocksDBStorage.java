package com.mawen.learn.rocketmq.store.rocksdb;

import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.config.AbstractRocksDBStorage;
import com.mawen.learn.rocketmq.common.utils.DataConverter;
import com.mawen.learn.rocketmq.store.MessageStore;
import lombok.Getter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/6
 */
@Getter
public class ConsumeQueueRocksDBStorage extends AbstractRocksDBStorage {

	private final MessageStore messageStore;
	private volatile ColumnFamilyHandle offsetCFHandle;

	public ConsumeQueueRocksDBStorage(MessageStore messageStore, final String dbPath, final int prefixLen) {
		this.messageStore = messageStore;
		this.dbPath = dbPath;
		this.readOnly = false;
	}

	private void initOptions() {
		options = RocksDBOptionsFactory.createDBOptions();

		writeOptions = new WriteOptions()
				.setSync(false)
				.setDisableWAL(true)
				.setNoSlowdown(true);

		totalOrderReadOptions = new ReadOptions()
				.setPrefixSameAsStart(false)
				.setTotalOrderSeek(false);

		compactRangeOptions = new CompactRangeOptions()
				.setBottommostLevelCompaction(CompactRangeOptions.BottommostLevelCompaction.kForce)
				.setAllowWriteStall(true)
				.setExclusiveManualCompaction(false)
				.setChangeLevel(true)
				.setTargetLevel(-1)
				.setMaxSubcompactions(4);
	}

	@Override
	protected boolean postLoad() {
		try {
			UtilAll.ensureDirOK(dbPath);

			initOptions();

			List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

			ColumnFamilyOptions cqcfOptions = RocksDBOptionsFactory.createCQCFOptions(messageStore);
			cfOptions.add(cqcfOptions);
			cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cqcfOptions));

			ColumnFamilyOptions offsetCFOptions = RocksDBOptionsFactory.createOffsetCFOptions();
			cfOptions.add(offsetCFOptions);
			cfDescriptors.add(new ColumnFamilyDescriptor("offset".getBytes(DataConverter.CHARSET_UTF8), offsetCFOptions));

			List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
			open(cfDescriptors, cfHandles);

			defaultCFHandle = cfHandles.get(0);
			offsetCFHandle = cfHandles.get(1);
		}
		catch (Exception e) {
			log.error("postLoad Failed. {}", dbPath, e);
			return false;
		}
		return true;
	}

	@Override
	protected void preShutdown() {
		offsetCFHandle.close();
	}

	public byte[] getCQ(final byte[] keyBytes) throws RocksDBException {
		return get(defaultCFHandle, totalOrderReadOptions, keyBytes);
	}

	public byte[] getOffset(final byte[] keyBytes) throws RocksDBException {
		return get(offsetCFHandle, totalOrderReadOptions, keyBytes);
	}

	public List<byte[]> multiGet(final List<ColumnFamilyHandle> cfList, final List<byte[]> keys) throws RocksDBException {
		return multiGet(totalOrderReadOptions, cfList, keys);
	}

	public void batchPut(final WriteBatch batch) throws RocksDBException {
		batchPut(writeOptions, batch);
	}

	public void manualCompaction(final long minPhyOffset) {
		try {
			manualCompaction(minPhyOffset, compactRangeOptions);
		}
		catch (Exception e) {
			log.error("manualCompaction Failed. minPhyOffset: {}", minPhyOffset, e);
		}
	}

	public RocksIterator seekOffsetCF() {
		return db.newIterator(offsetCFHandle, totalOrderReadOptions);
	}
}
