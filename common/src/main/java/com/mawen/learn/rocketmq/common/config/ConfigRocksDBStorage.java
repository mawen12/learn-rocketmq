package com.mawen.learn.rocketmq.common.config;

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


}
