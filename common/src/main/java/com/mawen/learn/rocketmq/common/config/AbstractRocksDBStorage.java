package com.mawen.learn.rocketmq.common.config;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang3.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public abstract class AbstractRocksDBStorage {

	protected static final Logger log = LoggerFactory.getLogger(AbstractRocksDBStorage.class);

	private static final String SPACE = " | ";


	protected String dbPath;
	protected boolean readOnly;
	protected RocksDB db;
	protected DBOptions options;

	protected WriteOptions writeOptions;
	protected WriteOptions ableWalWriteOptions;

	protected ReadOptions readOptions;
	protected ReadOptions totalOrderReadOptions;

	protected CompactionOptions compactionOptions;
	protected CompactionOptions compactRangeOptions;

	protected ColumnFamilyHandle defaultCGHandle;
	protected final List<ColumnFamilyOptions> cfOptions = new ArrayList<>();

	protected volatile boolean loaded;
	private volatile boolean closed;

	private final Semaphore reloadPermit = new Semaphore(1);
//	private final ScheduledExecutorService reloadScheduler = ThreadUtils.new
}
