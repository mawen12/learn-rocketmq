package com.mawen.learn.rocketmq.store.ha;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq.remoting.protocol.body.HARuntimeInfo;
import com.mawen.learn.rocketmq.store.CommitLog;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/8
 */
public interface HAService {

	void init(DefaultMessageStore defaultMessageStore) throws IOException;

	void start() throws Exception;

	void shutdown();

	default boolean changeToMaster(int masterEpoch) throws RocksDBException {
		return false;
	}

	default boolean changeToMasterWhenLastRoleIsMaster(int masterEpoch) {
		return false;
	}

	default boolean changeToSlave(String newMasterAttr, int newMasterEpoch, Long slaveId) {
		return false;
	}

	default boolean changeToSlaveWhenMasterNotChange(String newMasterAttr, int newMasterEpoch) {
		return false;
	}

	void updateMasterAddress(String newAddr);

	void updateHaMasterAddress(String newAddr);

	int inSyncReplicasNums(long masterPutWhere);

	AtomicInteger getConnectionCount();

	void putRequest(final CommitLog.GroupCommitRequest request);

	void putGroupConnectionStateRequest(HAConnectionStateNotificationRequest request);

	List<HAConnection> getConnectionList();

	HAClient getHAClient();

	AtomicLong getPush2SlaveMaxOffset();

	HARuntimeInfo getRuntimeInfo(final long masterPutWhere);

	WaitNotifyObject getWaitNotifyObject();

	boolean isSlaveOK(long masterPutWhere);
}
