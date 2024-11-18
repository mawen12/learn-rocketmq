package com.mawen.learn.rocketmq.store.ha.autoswitch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.remoting.protocol.EpochEntry;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.ha.DefaultHAService;
import com.mawen.learn.rocketmq.store.ha.GroupTransferService;
import com.mawen.learn.rocketmq.store.ha.HAClient;
import com.mawen.learn.rocketmq.store.ha.HAConnection;
import com.mawen.learn.rocketmq.store.ha.HAConnectionStateNotificationService;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/18
 */
public class AutoSwitchHAService extends DefaultHAService {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private final ExecutorService executorService = ThreadUtils.newSingleThreadExecutor(new ThreadFactoryImpl("AutoSwitchHAService_Executor_"));
	private final ConcurrentHashMap<Long, Long> connectionCaughtUpTimeTable = new ConcurrentHashMap<>();
	private final List<Consumer<Set<Long>>> syncStateSetChangedListeners = new ArrayList<>();
	private final Set<Long> syncStateSet = new HashSet<>();
	private final Set<Long> remoteSyncStateSet = new HashSet<>();
	private final ReadWriteLock syncStateSetReadWriteLock = new ReentrantReadWriteLock();
	private final Lock readLock = syncStateSetReadWriteLock.readLock();
	private final Lock writeLock = syncStateSetReadWriteLock.writeLock();

	private volatile boolean isSynchronizingSyncStateSet = false;

	private EpochFileCache epochCache;
	private AutoSwitchHAClient haClient;

	private Long localBrokerId;

	@Override
	public void init(DefaultMessageStore defaultMessageStore) throws IOException {
		this.epochCache = new EpochFileCache(defaultMessageStore.getMessageStoreConfig().getStorePathEpochFile());
		this.epochCache.initCacheFromFile();
		this.defaultMessageStore = defaultMessageStore;
		this.acceptSocketService = new AutoSwitchAcceptSocketService(defaultMessageStore.getMessageStoreConfig());
		this.groupTransferService = new GroupTransferService(this, defaultMessageStore);
		this.haConnectionStateNotificationService = new HAConnectionStateNotificationService(this, defaultMessageStore);
	}

	@Override
	public void shutdown() {
		super.shutdown();
		if (haClient != null) {
			haClient.shutdown();
		}
		executorService.shutdown();
	}

	@Override
	public void removeConnection(HAConnection conn) {
		if (!defaultMessageStore.isShutdown()) {
			Set<Long> syncStateSet = getLocalSyncStateSet();
			Long slave = ((AutoSwitchHAConnection)conn).getSlaveId();
			if (syncStateSet.contains(slave)) {
				syncStateSet.remove(slave);
				markSynchronizingSyncStateSet(syncStateSet);
				notifySyncStateSetChanged(syncStateSet);
			}
		}
		super.removeConnection(conn);
	}

	@Override
	public boolean changeToMaster(int masterEpoch) throws RocksDBException {
		int lastEpoch = epochCache.lastEpoch();
		if (masterEpoch < lastEpoch) {
			log.warn("newMasterEpoch {} < lastEpoch {}, fail to change to master", masterEpoch, lastEpoch);
			return false;
		}

		destroyConnections();

		if (haClient != null) {
			haClient.shutdown();
		}

		long truncateOffset = truncateInvalidMsg();

		defaultMessageStore.setConfirmOffset(computeConfirmOffset());

		if (truncateOffset >= 0) {
			epochCache.truncateSuffixByOffset(truncateOffset);
		}

		EpochEntry newEpochEntry = new EpochEntry(masterEpoch, defaultMessageStore.getMaxPhyOffset());
		if (epochCache.lastEpoch() >= masterEpoch) {
			epochCache.truncateSuffixByEpoch(masterEpoch);
		}
		epochCache.appendEntry(newEpochEntry);

		while (defaultMessageStore.dispatchBehindBytes() > 0) {
			try {
				Thread.sleep(100);
			}
			catch (Exception ignored) {
			}
		}

		if (defaultMessageStore.isTransientStorePoolDeficient()) {
			waitingForAllCommit();
			defaultMessageStore.getTransientStorePool().setRealCommit(true);
		}

		log.info("TruncateOffset is {}, confirmOffset is {}, maxPhyOffset is {}",
				truncateOffset, defaultMessageStore.getConfirmOffset(), defaultMessageStore.getMaxPhyOffset());
		defaultMessageStore.recoverTopicQueueTable();
		defaultMessageStore.setStateMachineVersion(masterEpoch);
		log.info("Change ha to master success, newMasterEpoch:{}, startOffset:{}",
				masterEpoch, newEpochEntry.getStartOffset());
		return true;
	}

	@Override
	public boolean changeToSlave(String newMasterAttr, int newMasterEpoch, Long slaveId) {
		int lastEpoch = epochCache.lastEpoch();
		if (newMasterEpoch < lastEpoch) {
			log.warn("newMasterEpoch {} < lastEpoch {}, fail to change to slave",
					newMasterEpoch, lastEpoch);
			return false;
		}

		try {
			destroyConnections();
			if (haClient == null) {
				haClient = new AutoSwitchHAClient(this, defaultMessageStore, epochCache, slaveId);
			}
			else {
				haClient.reOpen();
			}
			haClient.updateMasterAddress(newMasterAttr);
			haClient.updateHaMasterAddress(null);
			haClient.start();

			if (defaultMessageStore.isTransientStorePoolDeficient()) {
				waitingForAllCommit();
				defaultMessageStore.getTransientStorePool().setRealCommit(false);
			}

			defaultMessageStore.setStateMachineVersion(newMasterEpoch);

			log.info("Change ha to slave success, newMasterAddress:{}, newMasterEpoch:{}",
					newMasterAttr, newMasterEpoch);
			return true;
		}
		catch (Exception e) {
			log.error("Error happen when change ha to slave", e);
			return false;
		}
	}

	@Override
	public boolean changeToMasterWhenLastRoleIsMaster(int masterEpoch) {
		int lastEpoch = epochCache.lastEpoch();
		if (masterEpoch < lastEpoch) {
			log.warn("newMasterEpoch {} < lastEpoch {}, fail to change to master", masterEpoch, lastEpoch);
			return false;
		}

		EpochEntry newEpochEntry = new EpochEntry(masterEpoch, defaultMessageStore.getMaxPhyOffset());
		if (epochCache.lastEpoch() >= masterEpoch) {
			epochCache.truncateSuffixByEpoch(masterEpoch);
		}
		epochCache.appendEntry(newEpochEntry);

		defaultMessageStore.setStateMachineVersion(masterEpoch);
		log.info("Change ha to master success, last role is master, newMasterEpoch:{}, startOffset:{}",
				masterEpoch, newEpochEntry.getStartOffset());
		return true;
	}

	@Override
	public boolean changeToSlaveWhenMasterNotChange(String newMasterAttr, int newMasterEpoch) {
		int lastEpoch = epochCache.lastEpoch();
		if (newMasterEpoch < lastEpoch) {
			log.warn("newMasterEpoch {} < lastEpoch {}, fail to change to slave", newMasterEpoch, lastEpoch);
			return false;
		}

		defaultMessageStore.setStateMachineVersion(newMasterEpoch);
		log.info("Change ha to slave success, master doesn't change, newMasterAddress:{}, newMasterEpoch:{}", newMasterAttr, newMasterEpoch);
		return true;
	}

	public void waitingForAllCommit() {
		while (defaultMessageStore.remainHowManyDataToCommit() > 0) {
			defaultMessageStore.getCommitLog().getFlushManager().wakeUpCommit();
			try {
				Thread.sleep(100);
			}
			catch (Exception e) {}
		}
	}

	@Override
	public HAClient getHAClient() {
		return haClient;
	}

	@Override
	public void updateHaMasterAddress(String newAddr) {
		if (haClient != null) {
			haClient.updateHaMasterAddress(newAddr);
		}
	}

	@Override
	public void updateMasterAddress(String newAddr) {
		// NOP
	}

	public void registerSyncStateSetChangedListener(Consumer<Set<Long>> listener) {
		syncStateSetChangedListeners.add(listener);
	}

	public void notifySyncStateSetChanged(Set<Long> newSyncStateSet) {
		executorService.submit(() -> syncStateSetChangedListeners.forEach(listener -> listener.accept(newSyncStateSet)));
		log.info("Notify the syncStateSet has been changed into {}.", newSyncStateSet);
	}

	public Set<Long> maybeShrinkSyncStateSet() {
		getLocalSyncStateSet();
	}
}
