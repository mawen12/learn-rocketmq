package com.mawen.learn.rocketmq.store.queue;

import com.mawen.learn.rocketmq.store.Swappable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public interface FileQueueLifeCycle extends Swappable {

	boolean load();

	void recover();

	void checkSelf();

	boolean flush(int flushLeastPages);

	void destroy();

	void truncateDirtyLogicFiles(long maxCommitLogPos);

	int deleteExpiredFile(long minCommitLogPos);

	long rollNextFile(final long nextBeginOffset);

	boolean isFirstFileAvailable();

	boolean isFirstFileExist();
}
