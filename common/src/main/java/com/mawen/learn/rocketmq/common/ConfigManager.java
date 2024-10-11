package com.mawen.learn.rocketmq.common;

import java.io.IOException;
import java.util.Map;

import com.mawen.learn.rocketmq.common.config.RocksDBConfigManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.Statistics;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public abstract class ConfigManager {
	private static final Logger log = LoggerFactory.getLogger(ConfigManager.class);

	protected RocksDBConfigManager rocksDBConfigManager;

	public boolean load() {
		String fileName = null;
		try {
			fileName = this.configFilePath();
			String jsonString = MixAll.file2String(fileName);

			if (jsonString == null || jsonString.length() == 0) {
				return this.loadBak();
			}
			else {
				this.decode(jsonString);
				log.info("load " + fileName + " OK");
				return true;
			}
		}
		catch (Exception e) {
			log.error("laod " + fileName + " failed, and try to load backup file", e);
			return this.loadBak();
		}
	}

	public boolean loadBak() {
		String fileName = null;
		try {
			fileName = this.configFilePath();
			String jsonString = MixAll.file2String(fileName + ".bak");
			if (jsonString != null && jsonString.length() > 0) {
				this.decode(jsonString);
				log.info("load " + fileName + " OK");
				return true;
			}
		}
		catch (Exception e) {
			log.error("load " + fileName + " Failed", e);
			return false;
		}

		return true;
	}

	public synchronized <T> void persist(String topicName, T t) {
		this.persist();
	}

	public synchronized <T> void persist(Map<String, T> m) {
		this.persist();
	}

	public synchronized void persist() {
		String jsonString = this.encode(true);
		if (jsonString != null) {
			String fileName = this.configFilePath();
			try {
				MixAll.string2File(jsonString, fileName);
			}
			catch (IOException e) {
				log.error("persist file " + fileName + " exception", e);
			}
		}
	}

	public boolean stop() {
		return true;
	}

	public abstract String configFilePath();

	public abstract String encode();

	public abstract String encode(boolean prettyFormat);

	public abstract void decode(final String jsonString);

	public Statistics getStatistics() {
		return rocksDBConfigManager == null ? null : rocksDBConfigManager.getStatistics();
	}

	protected void decode0(final byte[] key, final byte[] body) {

	}
}
