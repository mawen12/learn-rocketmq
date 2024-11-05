package com.mawen.learn.rocketmq.store;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
@Getter
@AllArgsConstructor
public enum StoreType {

	DEFAULT("default"),
	DEFAULT_ROCKSDB("defaultRocksDB");

	private String storeType;
}
