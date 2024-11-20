package com.mawen.learn.rocketmq.store.ha.autoswitch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;
import com.mawen.learn.rocketmq.remoting.netty.NettySystemConfig;
import com.mawen.learn.rocketmq.remoting.protocol.EpochEntry;
import com.mawen.learn.rocketmq.store.SelectMappedBufferResult;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import com.mawen.learn.rocketmq.store.ha.FlowMonitor;
import com.mawen.learn.rocketmq.store.ha.HAConnection;
import com.mawen.learn.rocketmq.store.ha.HAConnectionState;
import com.mawen.learn.rocketmq.store.ha.io.AbstractHAReader;
import com.mawen.learn.rocketmq.store.ha.io.HAWriter;
import lombok.Getter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * Handshake data protocol in syncing msg from masters. Format:
 * <pre>
 * +---------------+-----------+---------+---------+-----------------------------------------------------------+
 * | current state | body size | offset  |  epoch  | EpochEntrySize * EpochEntryNums(12bytes * EpochEntryNums) |
 * +---------------+-----------+---------+---------+-----------------------------------------------------------+
 * | 4 bytes       | 4 bytes   | 8 bytes | 4 bytes | 12bytes * EpochEntryNums                                  |
 * |                        Header                 | Body                                                      |
 * +---------------+-----------+---------+---------+-----------------------------------------------------------+
 * </pre>
 * <p>
 * Transfer data protocol in syncing msg from master. Format:
 * <pre>
 * +---------------+-----------+---------+---------+------------------+---------------+-------------+
 * | current state | body size | offset  |  epoch  | epochStartOffset | confirmOffset |  log data   |
 * +---------------+-----------+---------+---------+------------------+---------------+-------------+
 * | 4 bytes       | 4 bytes   | 8 bytes | 4 bytes | 8 bytes          | 8 bytes       | (data size) |
 * |                                      Header                                      | Body        |
 * +---------------+-----------+---------+---------+------------------+---------------+-------------+
 * </pre>
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/20
 */
public class AutoSwitchHAConnection implements HAConnection {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	public static final int HANDSHAKE_HEADER_SIZE = 4 + 4 + 8 + 4;
	public static final int EPOCH_ENTRY_SIZE = 12;
	public static final int TRANSFER_HEADER_SIZE = HANDSHAKE_HEADER_SIZE + 8 + 8;

	private final AutoSwitchHAService haService;
	private final SocketChannel socketChannel;
	private final String clientAddress;
	private final EpochFileCache epochCache;
	private final AbstractWriteSocketService writeSocketService;
	private final ReadSocketService readSocketService;
	private final FlowMonitor flowMonitor;

	private volatile HAConnectionState currentState = HAConnectionState.HANDSHAKE;
	private volatile long slaveRequestOffset = -1;
	private volatile long slaveAckOffset = -1;
	private volatile boolean isSlaveSendHandshake = false;
	private volatile int currentTransferEpoch = -1;
	private volatile long currentTransferEpochEndOffset = 0;
	private volatile boolean isSyncFromLastFile = false;
	private volatile boolean isAsyncLearner = false;
	private volatile long slaveId = -1;
	private volatile long lastMasterMaxOffset = -1;
	private volatile long lastTransferTimeMs = 0;

	public AutoSwitchHAConnection(AutoSwitchHAService haService, SocketChannel socketChannel, EpochFileCache epochCache) throws IOException {
		this.haService = haService;
		this.socketChannel = socketChannel;
		this.epochCache = epochCache;
		this.clientAddress = socketChannel.socket().getRemoteSocketAddress().toString();
		this.socketChannel.configureBlocking(false);
		this.socketChannel.socket().setSoLinger(false, -1);
		this.socketChannel.socket().setTcpNoDelay(true);
		if (NettySystemConfig.socketSndbufSize > 0) {
			this.socketChannel.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
		}
		if (NettySystemConfig.socketRcvbufSize > 0) {
			this.socketChannel.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
		}
		this.writeSocketService = new WriteSocketService(socketChannel);
		this.readSocketService = new ReadSocketService(socketChannel);
		this.haService.getConnectionCount().incrementAndGet();
		this.flowMonitor = new FlowMonitor(haService.getDefaultMessageStore().getMessageStoreConfig());
	}

	public void changeCurrentState(HAConnectionState connectionState) {
		log.info("change state to {}", connectionState);
		this.currentState = connectionState;
	}

	@Override
	public void start() {
		changeCurrentState(HAConnectionState.HANDSHAKE);
		this.flowMonitor.start();
		this.readSocketService.start();
		this.writeSocketService.start();
	}

	@Override
	public void shutdown() {
		changeCurrentState(HAConnectionState.SHUTDOWN);
		this.flowMonitor.shutdown(true);
		this.writeSocketService.shutdown(true);
		this.readSocketService.shutdown(true);
		this.close();
	}

	@Override
	public void close() {
		if (socketChannel != null) {
			try {
				socketChannel.close();
			}
			catch (IOException e) {
				log.error("", e);
			}
		}
	}

	@Override
	public SocketChannel getSocketChannel() {
		return socketChannel;
	}

	@Override
	public HAConnectionState getCurrentState() {
		return currentState;
	}

	@Override
	public String getClientAddress() {
		return clientAddress;
	}

	@Override
	public long getTransferredByteInSecond() {
		return flowMonitor.getTransferredByteInSecond();
	}

	@Override
	public long getTransferFromWhere() {
		return writeSocketService.getNextTransferFromWhere();
	}

	@Override
	public long getSlaveAckOffset() {
		return slaveAckOffset;
	}

	private void changeTransferEpochToNext(final EpochEntry entry) {
		this.currentTransferEpoch = entry.getEpoch();
		this.currentTransferEpochEndOffset = entry.getEndOffset();
		if (entry.getEpoch() == epochCache.lastEpoch()) {
			this.currentTransferEpochEndOffset = -1;
		}
	}

	private synchronized void updateLastTransferInfo() {
		this.lastMasterMaxOffset = haService.getDefaultMessageStore().getMaxPhyOffset();
		this.lastTransferTimeMs = System.currentTimeMillis();
	}

	private synchronized void maybeExpandInSyncStateSet(long slaveMaxOffset) {
		if (!isAsyncLearner && slaveMaxOffset >= lastMasterMaxOffset) {
			long caughtUpTimeMs = haService.getDefaultMessageStore().getMaxPhyOffset() == slaveMaxOffset ? System.currentTimeMillis() : lastTransferTimeMs;
			haService.updateConnectionLastCaughtUpTime(slaveId, caughtUpTimeMs);
			haService.maybeExpandInSyncStateSet(slaveId,slaveMaxOffset);
		}
	}

	@Getter
	abstract class AbstractWriteSocketService extends ServiceThread {
		protected final Selector selector;
		protected final SocketChannel socketChannel;
		protected final HAWriter haWriter;

		protected final ByteBuffer byteBufferHeader = ByteBuffer.allocate(TRANSFER_HEADER_SIZE);
		private final ByteBuffer handshakeBuffer = ByteBuffer.allocate(EPOCH_ENTRY_SIZE * 1000);
		protected long nextTransferFromWhere = -1;
		protected boolean lastWriterOver = true;
		protected long lastWriteTimestamp = System.currentTimeMillis();
		protected long lastPrintTimestamp = System.currentTimeMillis();
		protected long transferOffset = 0;

		public AbstractWriteSocketService(final SocketChannel socketChannel) throws IOException {
			this.selector = NetworkUtil.openSelector();
			this.socketChannel = socketChannel;
			this.socketChannel.register(selector, SelectionKey.OP_WRITE);
			this.setDaemon(true);
			this.haWriter = new HAWriter();
			this.haWriter.registerHook(writeSize -> {
				flowMonitor.addByteCountTransferred(writeSize);
				if (writeSize > 0) {
					AbstractWriteSocketService.this.lastPrintTimestamp = haService.getDefaultMessageStore().getSystemClock().now();
				}
			});
		}

		private boolean buildHandshakeBuffer() {
			List<EpochEntry> epochEntries = AutoSwitchHAConnection.this.epochCache.getAllEntries();
			int lastEpoch = AutoSwitchHAConnection.this.epochCache.lastEpoch();
			long maxPhyOffset = AutoSwitchHAConnection.this.haService.getDefaultMessageStore().getMaxPhyOffset();

			byteBufferHeader.position(0);
			byteBufferHeader.limit(HANDSHAKE_HEADER_SIZE);
			byteBufferHeader.putInt(currentState.ordinal());
			byteBufferHeader.putInt(epochEntries.size() * EPOCH_ENTRY_SIZE);
			byteBufferHeader.putLong(maxPhyOffset);
			byteBufferHeader.putInt(lastEpoch);
			byteBufferHeader.flip();

			handshakeBuffer.position(0);
			handshakeBuffer.limit(EPOCH_ENTRY_SIZE * epochEntries.size());
			for (EpochEntry entry : epochEntries) {
				if (entry != null) {
					handshakeBuffer.putInt(entry.getEpoch());
					handshakeBuffer.putLong(entry.getStartOffset());
				}
			}
			handshakeBuffer.flip();
			log.info("Master build handshake header: maxEpoch:{}, maxOffset:{}, epochEntries:{}", lastEpoch, maxPhyOffset, epochEntries);
			return true;
		}

		private boolean handshakeWithSlave() throws IOException {
			boolean result = haWriter.write(socketChannel, byteBufferHeader);

			if (!result) {
				return false;
			}

			return haWriter.write(socketChannel, handshakeBuffer);
		}

		private void buildTransferHeaderBuffer(long nextOffset, int bodySize) {
			EpochEntry entry = AutoSwitchHAConnection.this.epochCache.getEntry(AutoSwitchHAConnection.this.currentTransferEpoch);

			if (entry == null) {
				if (nextOffset != -1 || currentTransferEpoch != -1 || bodySize > 0) {
					log.error("Failed to find epochEntry with epoch {} when build msg header", AutoSwitchHAConnection.this.currentTransferEpoch);
				}

				if (bodySize > 0) {
					return;
				}

				entry = AutoSwitchHAConnection.this.epochCache.firstEntry();
			}

			byteBufferHeader.position(0);
			byteBufferHeader.limit(TRANSFER_HEADER_SIZE);
			byteBufferHeader.putInt(currentState.ordinal());
			byteBufferHeader.putInt(bodySize);
			byteBufferHeader.putLong(nextOffset);
			byteBufferHeader.putInt(entry.getEpoch());
			byteBufferHeader.putLong(entry.getStartOffset());

			long confirmOffset = AutoSwitchHAConnection.this.haService.getDefaultMessageStore().getConfirmOffset();
			byteBufferHeader.putLong(confirmOffset);
			byteBufferHeader.flip();
		}

		private boolean sendHeartbeatIfNeeded() {
			long interval = haService.getDefaultMessageStore().getSystemClock().now() - lastWriteTimestamp;
			if (interval > haService.getDefaultMessageStore().getMessageStoreConfig().getHaSendHeartbeatInterval()) {
				buildTransferHeaderBuffer(nextTransferFromWhere, 0);
				return this.transferData(0);
			}
			return true;
		}

		private void transferToSlave() {
			if (lastWriterOver) {
				lastWriterOver = sendHeartbeatIfNeeded();
			}
			else {
				lastWriterOver = transferData(-1);
			}

			if (!lastWriterOver) {
				return;
			}

			int size = getNextTransferDataSize();
			if (size > 0) {
				if (size > haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
					size = haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
				}
				int canTransferMaxBytes = flowMonitor.canTransferMaxByteNum();
				if (size > canTransferMaxBytes) {
					if (System.currentTimeMillis() - lastPrintTimestamp > 1000) {
						log.warn("Trigger HA flow control, max transfer speed{}KB/s, current speed: {}KB/s",
								String.format("%.2f", flowMonitor.maxTransferByteInSecond() / 1024.0),
								String.format("%.2f", flowMonitor.getTransferredByteInSecond() / 1024.0));
						lastPrintTimestamp = System.currentTimeMillis();
					}
					size = canTransferMaxBytes;
				}

				if (size <= 0) {
					releaseData();
					waitForRunning(100);
					return;
				}

				long currentEpochEndOffset = AutoSwitchHAConnection.this.currentTransferEpochEndOffset;
				if (currentEpochEndOffset != -1 && nextTransferFromWhere + size > currentEpochEndOffset) {
					EpochEntry epochEntry = AutoSwitchHAConnection.this.epochCache.nextEntry(AutoSwitchHAConnection.this.currentTransferEpoch);
					if (epochEntry == null) {
						log.error("Can't find a bigger epochEntry than epoch {}", AutoSwitchHAConnection.this.currentTransferEpoch);
						waitForRunning(100);
						return;
					}
					size = (int) (currentEpochEndOffset - nextTransferFromWhere);
					changeTransferEpochToNext(epochEntry);
				}

				transferOffset = nextTransferFromWhere;
				nextTransferFromWhere += size;
				updateLastTransferInfo();

				buildTransferHeaderBuffer(transferOffset, size);

				lastWriterOver = transferData(size);
			}
			else {
				AutoSwitchHAConnection.this.haService.updateConnectionLastCaughtUpTime(AutoSwitchHAConnection.this.slaveId, System.currentTimeMillis());
				haService.getWaitNotifyObject().allWaitForRunning(100);
			}
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			log.info("{} service started", serviceName);

			while (!isStopped()) {
				try {
					selector.select(1000);

					switch (currentState) {
						case HANDSHAKE:
							if (!isSlaveSendHandshake) {
								waitForRunning(10);
								continue;
							}

							if (lastWriterOver) {
								if (!buildHandshakeBuffer()) {
									log.error("AutoSwitchHAConnection build handshake buffer failed");
									waitForRunning(5000);
									continue;
								}
							}

							lastWriterOver = handshakeWithSlave();
							if (lastWriterOver) {
								isSlaveSendHandshake = false;
							}
							break;
						case TRANSFER:
							if (-1 == slaveRequestOffset) {
								waitForRunning(10);
								continue;
							}

							if (-1 == nextTransferFromWhere) {
								if (0 == slaveRequestOffset) {
									MessageStoreConfig config = haService.getDefaultMessageStore().getMessageStoreConfig();
									if (AutoSwitchHAConnection.this.isSyncFromLastFile) {
										long masterOffset = haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
										masterOffset = masterOffset - (masterOffset % config.getMappedFileSizeCommitLog());
										if (masterOffset < 0) {
											masterOffset = 0;
										}
										nextTransferFromWhere = masterOffset;
									}
									else {
										nextTransferFromWhere = haService.getDefaultMessageStore().getCommitLog().getMinOffset();
									}
								}
								else {
									nextTransferFromWhere = slaveRequestOffset;
								}

								if (nextTransferFromWhere == -1) {
									sendHeartbeatIfNeeded();
									waitForRunning(500);
									break;
								}

								EpochEntry epochEntry = AutoSwitchHAConnection.this.epochCache.findEpochEntryByOffset(nextTransferFromWhere);
								if (epochEntry == null) {
									log.error("Failed to find an epochEntry to match nextTransferFromWhere {]", nextTransferFromWhere);
									sendHeartbeatIfNeeded();
									waitForRunning(500);
									break;
								}

								changeTransferEpochToNext(epochEntry);
								log.info("Master transfer data to slave {}, from offset:{}, currentEpoch:{}",
										AutoSwitchHAConnection.this.clientAddress, nextTransferFromWhere, epochEntry);
							}
							transferToSlave();
							break;
						default:
							throw new Exception("unexpected state " + currentState);

					}
				}
				catch (Exception e) {
					log.error("{} service has exception.", serviceName, e);
				}
			}

			onStop();

			changeCurrentState(HAConnectionState.SHUTDOWN);

			makeStop();

			readSocketService.makeStop();

			haService.removeConnection(AutoSwitchHAConnection.this);

			SelectionKey key = socketChannel.keyFor(selector);
			if (key != null) {
				key.cancel();
			}

			try {
				selector.close();
				socketChannel.close();
			}
			catch (IOException e) {
				log.error("", e);
			}

			flowMonitor.shutdown(true);

			log.info("{} service end", serviceName);
		}

		protected abstract int getNextTransferDataSize();

		protected abstract void releaseData();

		protected abstract boolean transferData(int maxTransferSize) throws Exception;

		protected abstract void onStop();
	}

	class WriteSocketService extends AbstractWriteSocketService {

		private SelectMappedBufferResult selectMappedBufferResult;

		public WriteSocketService(SocketChannel socketChannel) throws IOException {
			super(socketChannel);
		}

		@Override
		protected int getNextTransferDataSize() {
			SelectMappedBufferResult selectResult = haService.getDefaultMessageStore().getCommitLogData(nextTransferFromWhere);
			if (selectResult == null || selectResult.getSize() <= 0) {
				return 0;
			}
			selectMappedBufferResult = selectResult;
			return selectResult.getSize();
		}

		@Override
		protected void releaseData() {
			selectMappedBufferResult.release();
			selectMappedBufferResult = null;
		}

		@Override
		protected boolean transferData(int maxTransferSize) throws Exception {
			if (selectMappedBufferResult != null && maxTransferSize >= 0) {
				selectMappedBufferResult.getByteBuffer().limit(maxTransferSize);
			}

			boolean result = haWriter.write(socketChannel, byteBufferHeader);
			if (!result) {
				return false;
			}

			if (selectMappedBufferResult == null) {
				return false;
			}

			result = haWriter.write(socketChannel, selectMappedBufferResult.getByteBuffer());
			if (result) {
				releaseData();
			}
			return result;
		}

		@Override
		protected void onStop() {
			if (selectMappedBufferResult != null) {
				selectMappedBufferResult.release();
			}
		}

		@Override
		public String getServiceName() {
			if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
				return haService.getDefaultMessageStore().getBrokerIdentifier() + WriteSocketService.class.getSimpleName();
			}
			return WriteSocketService.class.getSimpleName();
		}
	}

	class ReadSocketService extends ServiceThread {
		private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
		private final Selector selector;
		private final SocketChannel socketChannel;
		private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
		private final AbstractHAReader haReader;
		private int processPosition = 0;
		private volatile long lastReadTimestamp = System.currentTimeMillis();

		public ReadSocketService(SocketChannel socketChannel) {
			this.selector = NetworkUtil.openSelector();
			this.socketChannel = socketChannel;
			this.socketChannel.register(selector, SelectionKey.OP_READ);
			setDaemon(true);
			this.haReader = new HAServerReader();
			this.haReader.registerHook(readSize -> {
				if (readSize > 0) {
					ReadSocketService.this.lastReadTimestamp = haService.getDefaultMessageStore().getSystemClock().now();
				}
			});
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			log.info("{} service started", serviceName);

			while (!isStopped()) {
				try {
					selector.select(1000);
					boolean ok = haReader.read(socketChannel, byteBufferRead);
					if (!ok) {
						log.error("processReadEvent error");
						break;
					}

					long interval =  haService.getDefaultMessageStore().getSystemClock().now() - lastReadTimestamp;
					if (interval > haService.getDefaultMessageStore().getMessageStoreConfig().getHaHoursekeepingInterval()) {
						log.warn("ha housekeeping, found this connection[{}] expired, {}", clientAddress, interval);
						break;
					}
				}
				catch (Exception e) {
					log.error("{} service has exception.", serviceName, e);
					break;
				}
			}

			makeStop();

			changeCurrentState(HAConnectionState.SHUTDOWN);

			writeSocketService.makeStop();

			haService.removeConnection(AutoSwitchHAConnection.this);

			haService.getConnectionCount().decrementAndGet();

			SelectionKey key = socketChannel.keyFor(selector);
			if (key != null) {
				key.cancel();
			}

			try {
				selector.close();
				socketChannel.close();
			}
			catch (IOException e) {
				log.error("", e);
			}

			flowMonitor.shutdown(true);

			log.info("{} service end", serviceName);
		}

		@Override
		public String getServiceName() {
			if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
				return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + ReadSocketService.class.getSimpleName();
			}
			return ReadSocketService.class.getSimpleName();
		}

		class HAServerReader extends AbstractHAReader {

			@Override
			protected boolean processReadResult(ByteBuffer buffer) {
				while (true) {
					boolean processSuccess = true;
					int readSocketPos = byteBufferRead.position();
					int diff = byteBufferRead.position() - ReadSocketService.this.processPosition;
					if (diff >= AutoSwitchHAClient.MIN_HEADER_SIZE) {
						int readPosition = ReadSocketService.this.processPosition;
						HAConnectionState slaveState = HAConnectionState.values()[byteBufferRead.getInt(readPosition)];

						switch (slaveState) {
							case HANDSHAKE:
								long slaveBrokerId = byteBufferRead.getLong(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 8);
								AutoSwitchHAConnection.this.slaveId = slaveBrokerId;

								short syncFromLastFileFlag = byteBufferRead.getShort(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 12);
								if (syncFromLastFileFlag == 1) {
									AutoSwitchHAConnection.this.isSyncFromLastFile = true;
								}

								short isAsyncLearner = byteBufferRead.getShort(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 10);
								if (isAsyncLearner == 1) {
									AutoSwitchHAConnection.this.isAsyncLearner = true;
								}

								isSlaveSendHandshake = true;
								byteBufferRead.position(readSocketPos);
								ReadSocketService.this.processPosition += AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE;
								log.info("Receive slave handshake, slaveBrokerId:{}, isSyncFromLastFile:{}, isAsyncLearner:{}", slaveId, isSyncFromLastFile, isAsyncLearner);
								break;
							case TRANSFER:
								long slaveMaxOffset = byteBufferRead.getLong(readPosition + 4);
								ReadSocketService.this.processPosition += AutoSwitchHAClient.TRANSFER_HEADER_SIZE;

								AutoSwitchHAConnection.this.slaveAckOffset = slaveMaxOffset;
								if (slaveRequestOffset < 0) {
									slaveRequestOffset = slaveMaxOffset;
								}

								byteBufferRead.position(readSocketPos);
								maybeExpandInSyncStateSet(slaveMaxOffset);
								AutoSwitchHAConnection.this.haService.updateConfirmOffsetWhenSlaveAck(AutoSwitchHAConnection.this.slaveId);
								AutoSwitchHAConnection.this.haService.notifyTransferSome(AutoSwitchHAConnection.this.slaveAckOffset);
								break;
							default:
								log.error("Current state illegal {}", currentState);
								return false;
						}

						if (!slaveState.equals(currentState)) {
							log.warn("Master change state from {} to {}", currentState, slaveState);
							changeCurrentState(slaveState);
						}
						if (processSuccess) {
							continue;
						}
					}

					if (!byteBufferRead.hasRemaining()) {
						byteBufferRead.position(ReadSocketService.this.processPosition);
						byteBufferRead.compact();
						ReadSocketService.this.processPosition = 0;
					}
					break;
				}
				return true;
			}
		}
	}


}
