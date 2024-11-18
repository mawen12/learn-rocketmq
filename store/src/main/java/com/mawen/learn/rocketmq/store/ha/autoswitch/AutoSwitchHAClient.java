package com.mawen.learn.rocketmq.store.ha.autoswitch;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;
import com.mawen.learn.rocketmq.remoting.common.RemotingHelper;
import com.mawen.learn.rocketmq.remoting.protocol.EpochEntry;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.ha.FlowMonitor;
import com.mawen.learn.rocketmq.store.ha.HAClient;
import com.mawen.learn.rocketmq.store.ha.HAConnectionState;
import com.mawen.learn.rocketmq.store.ha.io.AbstractHAReader;
import com.mawen.learn.rocketmq.store.ha.io.HAWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.rocksdb.RocksDBException;

/**
 * <pre>
 * Handshake header buffer size.
 * Schema: state ordinal + Two flags + slaveBrokerId.
 *  ┌───────────────┬────────────────────┬────────────────┬───────────────┐
 *  │ current state │                 Flags               │ slaveBrokerId │
 *  ├───────────────┼────────────────────┼────────────────┼───────────────┤
 *  │ -             │ isSyncFromLastFile │ isAsyncLearner │               │
 *  │ 4 bytes       │ 2 bytes            │ 2 bytes        │ 8 bytes       │
 *  └───────────────┴────────────────────┴────────────────┴───────────────┘
 * </pre>
 *
 * <pre>
 * Old version: Handshake header buffer size.
 * ┌──────────────────┬────────────────────┬────────────────┬────────────────────┬────────────────┐
 * │  current state   │                   Flags             │ slaveAddressLength │  slaveAddress  │
 * ├──────────────────┼────────────────────┼────────────────┼────────────────────┼────────────────┤
 * │ -                │ isSyncFromLastFile │ isAsyncLearner │                    │                │
 * │                               HANDSHAKE Header                              │ HANDSHAKE BODY │
 * │ 4 bytes          │ 2 bytes            │ 2 bytes        │ 4 bytes            │ 50 bytes       │
 * └──────────────────┴────────────────────┴────────────────┴────────────────────┴────────────────┘
 * </pre>
 *
 * <pre>
 * Transfer Header
 * ┌─────────────────┬───────────┐
 * │  current state  │ maxOffset │
 * ├─────────────────┼───────────┤
 * │ 4 bytes         │ 8 bytes   │
 * │          TRANSFER Header    │
 * └─────────────────┴───────────┘
 * </pre>
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/18
 */
public class AutoSwitchHAClient extends ServiceThread implements HAClient {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	public static final int HANDSHAKE_HEADER_SIZE = 4 + 4 + 8;
	public static final int HANDSHAKE_SIZE = HANDSHAKE_HEADER_SIZE + 50;
	public static final int TRANSFER_HEADER_SIZE = 4 + 8;
	public static final int MIN_HEADER_SIZE = Math.min(HANDSHAKE_HEADER_SIZE, TRANSFER_HEADER_SIZE);
	private static final int READ_MAX_BUFFER_SIZE = 4 * 1024 * 1024;

	private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
	private final AtomicReference<String> masterAddress = new AtomicReference<>();
	private final ByteBuffer handshakeHeaderBuffer = ByteBuffer.allocate(HANDSHAKE_HEADER_SIZE);
	private final ByteBuffer transferHeaderBuffer = ByteBuffer.allocate(TRANSFER_HEADER_SIZE);
	private final AutoSwitchHAService haService;
	private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
	private final DefaultMessageStore messageStore;
	private final EpochFileCache epochCache;

	private final Long brokerId;

	private SocketChannel socketChannel;
	private Selector selector;
	private AbstractHAReader haReader;
	private HAWriter haWriter;
	private FlowMonitor flowMonitor;

	private long lastReadTimestamp;
	private long lastWriteTimestamp;

	private long currentReportedOffset;
	private int processPosition;
	private volatile HAConnectionState currentState;
	private volatile int currentReceivedEpoch;

	public AutoSwitchHAClient(AutoSwitchService haService, DefaultMessageStore defaultMessageStore, EpochFileCache epochFileCache, Long brokerId) {
		this.haService = haService;
		this.messageStore = defaultMessageStore;
		this.epochCache = epochFileCache;
		this.brokerId = brokerId;
		init();
	}

	public void init() throws IOException {
		this.selector = NetworkUtil.openSelector();
		this.flowMonitor = new FlowMonitor(messageStore.getMessageStoreConfig());
		this.haReader = new HAClientReader();
		haReader.registerHook(readSize -> {
			if (readSize > 0) {
				flowMonitor.addByteCountTransferred(readSize);
				lastReadTimestamp = System.currentTimeMillis();
			}
		});
		this.haWriter = new HAWriter();
		haWriter.registerHook(writeSize -> {
			if (writeSize > 0) {
				lastWriteTimestamp = System.currentTimeMillis();
			}
		});
		changeCurrentState(HAConnectionState.READY);
		this.currentReceivedEpoch = -1;
		this.currentReportedOffset = 0;
		this.processPosition = 0;
		this.lastReadTimestamp = System.currentTimeMillis();
		this.lastWriteTimestamp = System.currentTimeMillis();
	}

	public void reOpen() throws IOException {
		shutdown();
		init();
	}

	@Override
	public String getServiceName() {
		if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
			return haService.getDefaultMessageStore().getBrokerIdentify() + AutoSwitchHAClient.class.getSimpleName();
		}
		return AutoSwitchHAClient.class.getSimpleName();
	}

	@Override
	public void updateMasterAddress(String newAddress) {
		String currentAddr = masterAddress.get();
		if (!StringUtils.equals(newAddress, currentAddr) && masterAddress.compareAndSet(currentAddr, newAddress)) {
			log.info("update master address, OLD: {} NEW: {}", currentAddr, newAddress);
		}
	}

	@Override
	public void updateHaMasterAddress(String newAddress) {
		String currentAddr = masterHaAddress.get();
		if (!StringUtils.equals(newAddress, currentAddr) && masterHaAddress.compareAndSet(currentAddr, newAddress)) {
			log.info("updater master ha address, OLD: {} NEW: {}", currentAddr, newAddress);
			wakeup();
		}
	}

	@Override
	public String getMasterAddress() {
		return masterAddress.get();
	}

	@Override
	public String getHaMasterAddress() {
		return masterHaAddress.get();
	}

	@Override
	public long getLastReadTimestamp() {
		return lastReadTimestamp;
	}

	@Override
	public long getLastWriteTimestamp() {
		return lastWriteTimestamp;
	}

	@Override
	public HAConnectionState getCurrentState() {
		return currentState;
	}

	@Override
	public void changeCurrentState(HAConnectionState state) {
		log.info("change state to {}", state);
		this.currentState = state;
	}

	@Override
	public void closeMaster() {
		if (socketChannel != null) {
			try {
				SelectionKey key = socketChannel.keyFor(selector);
				if (key != null) {
					key.cancel();
				}

				socketChannel.close();
				socketChannel = null;

				log.info("AutoSwitchHAClient close connection with master {}", masterHaAddress.get());
				changeCurrentState(HAConnectionState.READY);
			}
			catch (IOException e) {
				log.warn("CloseMaster exception.", e);
			}

			lastReadTimestamp = 0;
			processPosition = 0;

			byteBufferRead.position(0);
			byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
		}
	}

	@Override
	public long getTransferredByteInSecond() {
		return flowMonitor.getTransferredByteInSecond();
	}

	@Override
	public void run() {
		String serviceName = getServiceName();
		log.info("{} service started", serviceName);

		flowMonitor.start();
		while (!isStopped()) {
			try {
				switch (currentState) {
					case SHUTDOWN:
						flowMonitor.shutdown(true);
						break;
					case READY:
						long truncateOffset = haService.truncateInvalidMsg();
						if (truncateOffset > 0) {
							epochCache.truncateSuffixByOffset(truncateOffset);
						}

						if (!connectMaster()) {
							log.warn("AutoSwitchHAClient connect to master {} failed", masterHaAddress.get());
							waitForRunning(5 * 1000);
						}
						continue;
					case HANDSHAKE:
						handshakeWithMaster();
						continue;
					case TRANSFER:
						if (!transferFromMaster()) {
							closeMasterAndWait();
							continue;
						}
						break;
					case SUSPEND:
					default:
						waitForRunning(5 * 1000);
						continue;
				}

				long interval = messageStore.now() - lastReadTimestamp;
				if (interval > messageStore.getMessageStoreConfig().getHaHouseKeepingInterval()) {
					log.warn("AutoSwitchHAClient, housekeeping, found this connection[{}] expired, {}", masterHaAddress.get(), interval);
					closeMaster();
					log.warn("AutoSwitchHAClient, master not response some time, so close connection");
				}
			}
			catch (Exception e) {
				log.warn("{} service has exception.", serviceName, e);
				closeMasterAndWait();
			}
		}
		flowMonitor.shutdown(true);

		log.info("{} service end", serviceName);
	}

	@Override
	public void shutdown() {
		changeCurrentState(HAConnectionState.SHUTDOWN);
		flowMonitor.shutdown();
		super.shutdown();

		closeMaster();
		try {
			selector.close();
		}
		catch (IOException e) {
			log.warn("Close the selector of AutoSwitchHAClient error.", e);
		}
	}

	private boolean isTimeToReportOffset() {
		long interval = messageStore.now() - lastWriteTimestamp;
		return interval > messageStore.getMessageStoreConfig().getHaSendHeartbeartInterval();
	}

	private boolean sendHandshakeHeader() throws IOException {
		handshakeHeaderBuffer.position(0);
		handshakeHeaderBuffer.limit(HANDSHAKE_HEADER_SIZE);
		handshakeHeaderBuffer.putInt(HAConnectionState.HANDSHAKE.ordinal());
		short isSyncFromLastFile = haService.getDefaultMessageStore().getMessageStoreConfig().isSyncFromLastFile() ? 1 : 0;
		handshakeHeaderBuffer.putShort(isSyncFromLastFile);
		short isAsyncLearner = haService.getDefaultMessageStore().getMessageStoreConfig().isAsyncLearner() ? 1 : 0;
		handshakeHeaderBuffer.putShort(isAsyncLearner);
		handshakeHeaderBuffer.putLong(brokerId);
		handshakeHeaderBuffer.flip();

		return haWriter.write(socketChannel, handshakeHeaderBuffer);
	}

	private void handshakeWithMaster() {
		boolean result = sendHandshakeHeader();
		if (!result) {
			closeMasterAndWait();
		}

		selector.select(5000);

		result = haReader.read(socketChannel, byteBufferRead);
		if (!result) {
			closeMasterAndWait();
		}
	}

	private boolean reportSlaveOffset(HAConnectionState currentState, final long offsetToReport) throws IOException {
		transferHeaderBuffer.position(0);
		transferHeaderBuffer.limit(TRANSFER_HEADER_SIZE);
		transferHeaderBuffer.putInt(currentState.ordinal());
		transferHeaderBuffer.putLong(offsetToReport);
		transferHeaderBuffer.flip();
		return haWriter.write(socketChannel, transferHeaderBuffer);
	}

	private boolean reportSlaveMaxOffset(HAConnectionState currentState) throws IOException {
		boolean result = true;
		long maxPhyOffset = messageStore.getMaxPhyOffset();
		if (maxPhyOffset > currentReportedOffset) {
			currentReportedOffset = maxPhyOffset;
			result = reportSlaveOffset(currentState, currentReportedOffset);
		}
		return result;
	}

	public boolean connectMaster() throws ClosedChannelException {
		if (socketChannel == null) {
			String addr = masterHaAddress.get();
			if (StringUtils.isNotEmpty(addr)) {
				SocketAddress socketAddress = NetworkUtil.string2SocketAddress(addr);
				socketChannel = RemotingHelper.connect(socketAddress);
				if (socketChannel != null) {
					socketChannel.register(selector, SelectionKey.OP_READ);
					log.info("AutoSwitchHAClient connect to master {}", addr);
					changeCurrentState(HAConnectionState.HANDSHAKE);
				}
			}
			currentReportedOffset = messageStore.getMaxPhyOffset();
			lastReadTimestamp = System.currentTimeMillis();
		}
		return socketChannel != null;
	}

	private boolean transferFromMaster() throws IOException {
		boolean result;
		if (isTimeToReportOffset()) {
			log.info("Slave report current offset {}", currentReportedOffset);
			result = reportSlaveOffset(HAConnectionState.TRANSFER, currentReportedOffset);
			if (!result) {
				return false;
			}
		}

		selector.select(1000);

		result = haReader.read(socketChannel, byteBufferRead);
		if (!result) {
			return false;
		}

		return reportSlaveMaxOffset(HAConnectionState.TRANSFER);
	}

	private boolean doTruncate(List<EpochEntry> masterEpochEntries, long masterEndOffset) throws RocksDBException, IOException {
		if (epochCache.getEntrySize() == 0) {
			log.info("Slave local epochCache is empty, skip truncate log");
			changeCurrentState(HAConnectionState.TRANSFER);
			currentReportedOffset = 0;
		}
		else {
			EpochFileCache masterEpochCache = new EpochFileCache();
			masterEpochCache.initCacheFromEntries(masterEpochEntries);
			masterEpochCache.setLastEpochEntryEndOffset(masterEndOffset);

			List<EpochEntry> localEpochEntries = epochCache.getAllEntries();
			EpochFileCache localEpochCache = new EpochFileCache();
			localEpochCache.initCacheFromEntries(localEpochEntries);
			localEpochCache.setLastEpochEntryEndOffset(messageStore.getMaxPhyOffset());

			log.info("master epoch entries is {}", masterEpochCache.getAllEntries());
			log.info("local epoch entries is {}", localEpochEntries);

			long truncateOffset = localEpochCache.findConsistentPoint(masterEpochCache);
			log.info("truncateOffset is {}", truncateOffset);

			if (truncateOffset < 0) {
				log.error("Failed to find a consistent point between masterEpoch: {} and slaveEpoch: {}", masterEpochEntries, localEpochEntries);
				return false;
			}

			if (!messageStore.truncateFiles(truncateOffset)) {
				log.error("Failed to truncate slave log to {}", truncateOffset);
				return false;
			}

			epochCache.truncateSuffixByOffset(truncateOffset);
			log.info("Truncate slave log to {} success, change to transfer state", truncateOffset);
			changeCurrentState(HAConnectionState.TRANSFER);
			currentReportedOffset = truncateOffset;
		}

		if (!reportSlaveMaxOffset(HAConnectionState.TRANSFER)) {
			log.error("AutoSwitchHAClient report max offset to master failed");
			return false;
		}
		return true;
	}

	class HAClientReader extends AbstractHAReader {

		@Override
		protected boolean processReadResult(ByteBuffer buffer) {
			int readSocketPos = buffer.position();
			try {
				while (true) {
					int diff = buffer.position() - processPosition;
					if (diff >= HANDSHAKE_HEADER_SIZE) {
						int processPosition = AutoSwitchHAClient.this.processPosition;
						int masterState = buffer.getInt(processPosition + HANDSHAKE_HEADER_SIZE - 20);
						int bodySize = buffer.getInt(processPosition + HANDSHAKE_HEADER_SIZE - 16);
						long masterOffset = buffer.getLong(processPosition + HANDSHAKE_HEADER_SIZE - 12);
						int masterEpoch = buffer.getInt(processPosition + HANDSHAKE_HEADER_SIZE - 4);
						long masterEpochStartOffset = 0;
						long confirmOffset = 0;

						if (masterState == HAConnectionState.TRANSFER.ordinal() && diff >= TRANSFER_HEADER_SIZE) {
							masterEpochStartOffset = buffer.getLong(processPosition + TRANSFER_HEADER_SIZE - 16);
							confirmOffset = buffer.getLong(processPosition + TRANSFER_HEADER_SIZE - 8);
						}

						if (masterState != currentState.ordinal()) {
							int headerSize = masterState == HAConnectionState.TRANSFER.ordinal() ? TRANSFER_HEADER_SIZE : HANDSHAKE_HEADER_SIZE;
							processPosition += headerSize = bodySize;
							waitForRunning(1);
							log.error("State no matched, masterState:{}, slaveState:{}, bodySize:{}, offset:{}, masterEpoch:{}, masterEpochStartOffset:{}, confirmOffset:{}",
									HAConnectionState.values()[masterState], currentState, bodySize, masterOffset, masterEpoch, masterEpochStartOffset, confirmOffset);
							return false;
						}

						boolean isComplete = true;
						switch (currentState) {
							case HANDSHAKE: {
								if (diff < HANDSHAKE_HEADER_SIZE + bodySize) {
									isComplete = false;
									break;
								}
								processPosition += HANDSHAKE_HEADER_SIZE;
								int entrySize = AutoSwitchHAConnection.EPOCH_ENTRY_SIZE;
								int entryNums = bodySize / entrySize;
								List<EpochEntry> epochEntries = new ArrayList<>(entryNums);
								for (int i = 0; i < entryNums; i++) {
									int epoch = buffer.getInt(processPosition + i * entrySize);
									long startOffset = buffer.getLong(processPosition + i * entrySize + 4);
									epochEntries.add(new EpochEntry(epoch, startOffset));
								}
								buffer.position(readSocketPos);
								processPosition += bodySize;
								log.info("Receive handshake, masterMaxPosition: {}, masterEpochEntries: {}, try truncate log",
										masterOffset, epochEntries);
								if (!doTruncate(epochEntries, masterOffset)) {
									waitForRunning(2 * 1000);
									log.error("AutoSwitchHAClient truncate log failed in handshake state");
									return false;
								}
							}
							break;
							case TRANSFER: {
								if (diff < TRANSFER_HEADER_SIZE + bodySize) {
									isComplete = false;
									break;
								}
								byte[] bodyData = new byte[bodySize];
								buffer.position(processPosition + TRANSFER_HEADER_SIZE);
								buffer.get(bodyData);
								buffer.position(readSocketPos);
								processPosition += TRANSFER_HEADER_SIZE + bodySize;
								long slavePhyOffset = messageStore.getMaxPhyOffset();
								if (slavePhyOffset != 0) {
									if (slavePhyOffset != masterOffset) {
										log.error("master pushed offset not equal the max phy offset in slave, SLAVE: {}, MASTER: {}",
												slavePhyOffset, masterOffset);
										return false;
									}
								}

								if (masterEpoch != currentReceivedEpoch) {
									currentReceivedEpoch = masterEpoch;
									epochCache.appendEntry(new EpochEntry(masterEpoch, masterEpochStartOffset));
								}

								if (bodySize > 0) {
									messageStore.appendToCommitLog(masterOffset, bodyData, 0, bodyData.length);
								}

								haService.getDefaultMessageStore().setConfirmOffset(Math.min(confirmOffset, messageStore.getMaxPhyOffset()));

								if (!reportSlaveMaxOffset(HAConnectionState.TRANSFER)) {
									log.error("AutoSwitchHAClient report max offset to master failed");
									return false;
								}
								break;
							}
							default:
								break;
						}

						if (isComplete) {
							continue;
						}
					}

					if (!buffer.hasRemaining()) {
						buffer.position(processPosition);
						buffer.compact();
						processPosition = 0;
					}
					break;
				}
			}
			catch (Exception e) {
				log.error("Error when ha client process read request", e);
			}
			return true;
		}
	}
}
