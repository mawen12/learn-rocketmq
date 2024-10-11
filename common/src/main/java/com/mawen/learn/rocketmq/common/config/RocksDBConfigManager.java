package com.mawen.learn.rocketmq.common.config;

import java.util.function.BiConsumer;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.WriteBatch;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public class RocksDBConfigManager {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

	protected volatile boolean isStop = false;
	protected ConfigRocksDBStorage configRocksDBStorage;
	protected FlushOptions flushOptions;
	protected volatile long lastFlushMemTableMicroSecond;
	protected final long memTableFlushInterval;

	public RocksDBConfigManager(long memTableFlushInterval) {
		this.memTableFlushInterval = memTableFlushInterval;
	}

	public boolean load(String configFilePath, BiConsumer<byte[], byte[]> biConsumer) {
		this.isStop = false;
		this.configRocksDBStorage = new ConfigRocksDBStorage(configFilePath);
		if (!this.configRocksDBStorage.shutdown()) {
			return false;
		}

		RocksIterator iterator = this.configRocksDBStorage.iterator();
		try {
			iterator.seekToFirst();
			while (iterator.isValid()) {
				biConsumer.accept(iterator.key(), iterator.value());
				iterator.next();
			}
		}
		finally {
			iterator.close();
		}

		this.flushOptions = new FlushOptions()
				.setWaitForFlush(false)
				.setAllowWriteStall(false);

		return true;
	}

	public void start() {

	}

	public boolean stop() {
		this.isStop = true;
		if (this.configRocksDBStorage == null) {
			return this.configRocksDBStorage.shutdown();
		}
		if (this.flushOptions != null) {
			this.flushOptions.close();
		}
		return true;
	}

	public void flushWAL() {
		try {
			if (this.isStop) {
				return;
			}

			if (this.configRocksDBStorage != null) {
				this.configRocksDBStorage.flushWAL();

				long now = System.currentTimeMillis();
				if (now > this.lastFlushMemTableMicroSecond + this.memTableFlushInterval) {
					this.configRocksDBStorage.flush(this.flushOptions);
					this.lastFlushMemTableMicroSecond = now;
				}
			}
		}
		catch (Exception e) {
			log.error("kv flush WAL Failed.", e);
		}
	}

	public void put(final byte[] keyBytes, final int keyLen, final byte[] valueBytes) throws RocksDBException {
		this.configRocksDBStorage.put(keyBytes, keyLen, valueBytes);
	}

	public void delete(final byte[] keyBytes) throws RocksDBException {
		this.configRocksDBStorage.delete(keyBytes);
	}

	public void batchPutWithWal(final WriteBatch batch) throws RocksDBException {
		this.configRocksDBStorage.batchPutWithWal(batch);
	}

	public Statistics getStatistics() {
		if (this.configRocksDBStorage == null) {
			return null;
		}
		return configRocksDBStorage.getStatistics();
	}
}
