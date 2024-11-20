package com.mawen.learn.rocketmq.store.ha.autoswitch;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.ConcurrentHashMapUtils;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.remoting.protocol.EpochEntry;
import com.mawen.learn.rocketmq.remoting.protocol.body.HARuntimeInfo;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.SelectMappedBufferResult;
import com.mawen.learn.rocketmq.store.config.BrokerRole;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import com.mawen.learn.rocketmq.store.ha.DefaultHAService;
import com.mawen.learn.rocketmq.store.ha.GroupTransferService;
import com.mawen.learn.rocketmq.store.ha.HAClient;
import com.mawen.learn.rocketmq.store.ha.HAConnection;
import com.mawen.learn.rocketmq.store.ha.HAConnectionStateNotificationService;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.RocksDBException;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/18
 */
@Getter
@Setter
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
		final Set<Long> newSyncStateSet = getLocalSyncStateSet();
		boolean isSyncStateSetChanged = false;
		long haMaxTimeSlaveNoCatchup = defaultMessageStore.getMessageStoreConfig().getHaMaxTimeSlaveNoCatchup();
		for (Map.Entry<Long, Long> next : connectionCaughtUpTimeTable.entrySet()) {
			Long slaveBrokerId = next.getKey();
			if (newSyncStateSet.contains(slaveBrokerId)) {
				Long lastCaughtUpTimeMs = next.getValue();
				if ((System.currentTimeMillis() - lastCaughtUpTimeMs) > haMaxTimeSlaveNoCatchup) {
					newSyncStateSet.remove(slaveBrokerId);
					isSyncStateSetChanged = true;
				}
			}
		}

		Iterator<Long> iterator = newSyncStateSet.iterator();
		while (iterator.hasNext()) {
			Long slaveBrokerId = iterator.next();
			if (!Objects.equals(slaveBrokerId, localBrokerId) && !connectionCaughtUpTimeTable.containsKey(slaveBrokerId)) {
				iterator.remove();
				isSyncStateSetChanged = true;
			}
		}

		if (isSyncStateSetChanged) {
			markSynchronizingSyncStateSet(newSyncStateSet);
		}
		return newSyncStateSet;
	}

	public void maybeExpandInSyncStateSet(Long slaveBrokerId, long slaveMaxOffset) {
		Set<Long> currentSyncStateSet = getLocalSyncStateSet();
		if (currentSyncStateSet.contains(slaveBrokerId)) {
			return;
		}

		long confirmOffset = defaultMessageStore.getConfirmOffset();
		if (slaveMaxOffset >= confirmOffset) {
			EpochEntry currentLeaderEpoch = epochCache.lastEntry();
			if (slaveMaxOffset >= currentLeaderEpoch.getStartOffset()) {
				log.info("The slave {} has caught up, slaveMaxOffset: {}, confirmOffset: {}, epoch: {}, leader epoch startOffset: {}.",
						slaveBrokerId, slaveMaxOffset, confirmOffset, currentLeaderEpoch.getEpoch(), currentLeaderEpoch.getStartOffset());
				currentSyncStateSet.add(slaveBrokerId);
				markSynchronizingSyncStateSet(currentSyncStateSet);
				notifySyncStateSetChanged(currentSyncStateSet);
			}
		}
	}

	private void markSynchronizingSyncStateSet(Set<Long> newSyncStateSet) {
		writeLock.lock();
		try {
			isSynchronizingSyncStateSet = true;
			remoteSyncStateSet.clear();
			remoteSyncStateSet.addAll(newSyncStateSet);
		}
		finally {
			writeLock.unlock();
		}
	}

	private void markSynchronizingSyncStateSetDone() {
		isSynchronizingSyncStateSet = false;
	}

	public void updateConnectionLastCaughtUpTime(Long slaveBrokerId, long lastCaughtUpTimeMs) {
		Long prevTime = ConcurrentHashMapUtils.computeIfAbsent(connectionCaughtUpTimeTable, slaveBrokerId, k -> 0L);
		connectionCaughtUpTimeTable.put(slaveBrokerId, Math.max(prevTime, lastCaughtUpTimeMs));
	}

	public void updateConfirmOffsetWhenSlaveAck(Long slaveBrokerId) {
		readLock.lock();
		try {
			if (syncStateSet.contains(slaveBrokerId)) {
				defaultMessageStore.setConfirmOffset(computConfirmOffset());
			}
		}
		finally {
			readLock.unlock();
		}
	}

	@Override
	public int inSyncReplicasNums(long masterPutWhere) {
		readLock.lock();
		try {
			if (isSynchronizingSyncStateSet) {
				return Math.max(syncStateSet.size(), remoteSyncStateSet.size());
			}
			else {
				return syncStateSet.size();
			}
		}
		finally {
			readLock.unlock();
		}
	}

	@Override
	public HARuntimeInfo getRuntimeInfo(long masterPutWhere) {
		HARuntimeInfo info = new HARuntimeInfo();

		if (BrokerRole.SLAVE.equals(defaultMessageStore.getMessageStoreConfig().getBrokerRole())) {
			info.setMaster(false);

			info.getHaClientRuntimeInfo().setMasterAddr(haClient.getHaMasterAddress());
			info.getHaClientRuntimeInfo().setMaxOffset(defaultMessageStore.getMaxPhyOffset());
			info.getHaClientRuntimeInfo().setLastReadTimestamp(haClient.getLastReadTimestamp());
			info.getHaClientRuntimeInfo().setLastWriteTimestamp(haClient.getLastWriteTimestamp());
			info.getHaClientRuntimeInfo().setTransferredByteInSecond(haClient.getTransferredByteInSecond());
			info.getHaClientRuntimeInfo().setMasterFlushOffset(defaultMessageStore.getMasterFlushedOffset());
		}
		else {
			info.setMaster(true);

			info.setMasterCommitLogMaxOffset(masterPutWhere);

			Set<Long> localSyncStateSet = getLocalSyncStateSet();
			for (HAConnection conn : connectionList) {
				HARuntimeInfo.HAConnectionRuntimeInfo cInfo = new HARuntimeInfo.HAConnectionRuntimeInfo();

				long slaveAckOffset = conn.getSlaveAckOffset();
				cInfo.setSlaveAckOffset(slaveAckOffset);
				cInfo.setDiff(masterPutWhere - slaveAckOffset);
				cInfo.setAddr(conn.getClientAddress().substring(1));
				cInfo.setTransferredByteInSecond(conn.getTransferredByteInSecond());
				cInfo.setTransferFromWhere(conn.getTransferFromWhere());

				cInfo.setInSync(localSyncStateSet.contains(((AutoSwitchHAConnection)conn).getSlaveId()));

				info.getHaConnectionInfo().add(cInfo);
			}
			info.setInSyncSlaveNums(localSyncStateSet.size() - 1);
		}
		return info;
	}

	public long computeConfirmOffset() {
		Set<Long> currentSyncStateSet = getSyncStateSet();
		long newConfirmOffset = defaultMessageStore.getMaxPhyOffset();
		List<Object> idList = connectionList.stream().map(conn -> ((AutoSwitchHAConnection) conn).getSlaveId()).collect(Collectors.toList());

		for (Long syncId : currentSyncStateSet) {
			if (!idList.contains(syncId) && localBrokerId != null && !Objects.equals(syncId, localBrokerId)) {
				log.warn("Slave {} is still in syncStateSet, but has lost its connection. So new offset can't be compute.", syncId);
				return defaultMessageStore.getConfirmOffsetDirectly();
			}
		}

		for (HAConnection conn : connectionList) {
			Long slaveId = ((AutoSwitchHAConnection)conn).getSlaveId();
			if (currentSyncStateSet.contains(slaveId) && conn.getSlaveAckOffset() > 0) {
				newConfirmOffset = Math.min(newConfirmOffset, conn.getSlaveAckOffset());
			}
		}

		return newConfirmOffset;
	}

	public void setSyncStateSet(Set<Long> syncStateSet) {
		writeLock.lock();
		try {
			markSynchronizingSyncStateSetDone();
			this.syncStateSet.clear();
			this.syncStateSet.addAll(syncStateSet);
			this.defaultMessageStore.setConfirmOffset(computeConfirmOffset());
		}
		finally {
			writeLock.unlock();
		}
	}

	public Set<Long> getSyncStateSet() {
		readLock.lock();
		try {
			if (isSynchronizingSyncStateSet) {
				Set<Long> unionSyncStateSet = new HashSet<>(syncStateSet.size() + remoteSyncStateSet.size());
				unionSyncStateSet.addAll(syncStateSet);
				unionSyncStateSet.addAll(remoteSyncStateSet);
				return unionSyncStateSet;
			}
			else {
				Set<Long> syncStateSet = new HashSet<>(this.syncStateSet.size());
				syncStateSet.addAll(this.syncStateSet);
				return syncStateSet;
			}
		}
		finally {
			readLock.unlock();
		}
	}


	public Set<Long> getLocalSyncStateSet() {
		readLock.lock();
		try {
			Set<Long> localSyncStateSet = new HashSet<>(syncStateSet.size());
			localSyncStateSet.addAll(syncStateSet);
			return localSyncStateSet;
		}
		finally {
			readLock.unlock();
		}
	}

	public void truncateEpochFilePrefix(final long offset) {
		epochCache.truncatePrefixByOffset(offset);
	}

	public void truncateEpochFileSuffix(final long offset) {
		epochCache.truncateSuffixByOffset(offset);
	}

	public long truncateInvalidMsg() {
		long dispatchBehind = defaultMessageStore.dispatchBehindBytes();
		if (dispatchBehind <= 0) {
			log.info("Dispatch complete, skip truncate");
			return -1;
		}

		boolean doNext = true;
		long reputFromOffset = defaultMessageStore.getReputFromOffset();
		do {
			SelectMappedBufferResult result = defaultMessageStore.getCommitLog().getData(reputFromOffset);
			if (result == null) {
				break;
			}

			try {
				reputFromOffset = result.getStartOffset();

				int readSize = 0;
				while (readSize < result.getSize()) {
					DispatchRequest dispatchRequest = defaultMessageStore.getCommitLog().checkMessageAndReturnSize(result.getByteBuffer());
					if (dispatchRequest.isSuccess()) {
						int size = dispatchRequest.getMsgSize();
						if (size > 0) {
							reputFromOffset += size;
							readSize += size;
						}
						else {
							reputFromOffset = defaultMessageStore.getCommitLog().rollNextFile(reputFromOffset);
							break;
						}
					}
					else {
						doNext = false;
						break;
					}
				}
			}
			finally {
				result.release();
			}
		}
		while (reputFromOffset < defaultMessageStore.getMaxPhyOffset() && doNext);

		log.info("Truncate commitLog to {}", reputFromOffset);
		defaultMessageStore.truncateDirtyFiles(reputFromOffset);
		return reputFromOffset;
	}

	public int getLastEpoch() {
		return epochCache.lastEpoch();
	}

	public List<EpochEntry> getEpochEntries() {
		return epochCache.getAllEntries();
	}

	class AutoSwitchAcceptSocketService extends AcceptSocketService {

		public AutoSwitchAcceptSocketService(MessageStoreConfig messageStoreConfig) {
			super(messageStoreConfig);
		}

		@Override
		protected HAConnection createConnection(SocketChannel sc) throws IOException {
			return new AutoSwitchHAConnection(AutoSwitchHAService.class, sc, AutoSwitchHAService.this.epochCache);
		}

		@Override
		public String getServiceName() {
			if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
				return defaultMessageStore.getBrokerConfig().getIdentifier() + AcceptSocketService.class.getSimpleName();
			}
			return AutoSwitchAcceptSocketService.class.getSimpleName();
		}
	}
}
