package com.mawen.learn.rocketmq.store;

import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
public interface CommitLogDispatcher {

	void dispatch(final DispatchRequest request) throws RocksDBException;
}
