package com.mawen.learn.rocketmq.common;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public abstract class ConfigManager {
	private static final Logger log = LoggerFactory.getLogger(ConfigManager.class);

	protected RocksDBConfigManager rocksDBConfigManager;

}
