package com.mawen.learn.rocketmq.store.kv;

import com.mawen.learn.rocketmq.store.CommitLogDispatcher;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import lombok.RequiredArgsConstructor;
import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@RequiredArgsConstructor
public class CommitLogDispatcherCompaction implements CommitLogDispatcher {

	private final CompactionService compactionService;

	@Override
	public void dispatch(DispatchRequest request) throws RocksDBException {
		if (compactionService != null) {
			compactionService.putRequest(request);
		}
	}
}
