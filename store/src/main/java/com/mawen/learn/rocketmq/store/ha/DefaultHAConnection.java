package com.mawen.learn.rocketmq.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;
import com.mawen.learn.rocketmq.remoting.netty.NettySystemConfig;
import com.mawen.learn.rocketmq.store.SelectMappedBufferResult;
import lombok.Getter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * Transfer header buffer, size: 8 + 4, schema: physic offset and body size. Format:
 * <pre>
 * ┌──────────────┬──────────┐
 * │ physicOffset │ bodySize │
 * ├──────────────┼──────────┤
 * │ 8 bytes      │ 4 bytes  │
 * └──────────────┴──────────┘
 * </pre>
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/8
 */
@Getter
public class DefaultHAConnection implements HAConnection {

	public static final int TRANSFER_HEADER_SIZE = 8 + 4;

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private final DefaultHAService haService;
	private final SocketChannel socketChannel;
	private final String clientAddress;

	private WriteSocketService writeSocketService;
	private ReadSocketService readSocketService;

	private volatile HAConnectionState currentState = HAConnectionState.TRANSFER;
	private volatile long slaveRequestOffset = -1;
	private volatile long slaveAckOffset = -1;

	private FlowMonitor flowMonitor;

	public DefaultHAConnection(DefaultHAService haService, SocketChannel socketChannel) throws IOException {
		this.haService = haService;
		this.socketChannel = socketChannel;
		this.clientAddress = socketChannel.socket().getRemoteSocketAddress().toString();
		this.socketChannel.configureBlocking(false);
		this.socketChannel.socket().setSoLinger(false, -1);
		this.socketChannel.socket().setTcpNoDelay(true);

		if (NettySystemConfig.socketSndbufSize > 0) {
			socketChannel.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
		}
		if (NettySystemConfig.socketRcvbufSize > 0) {
			socketChannel.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
		}

		this.writeSocketService = new WriteSocketService(socketChannel);
		this.readSocketService = new ReadSocketService(socketChannel);
		this.haService.getConnectionCount().incrementAndGet();
		this.flowMonitor = new FlowMonitor(haService.getDefaultMessageStore().getMessageStoreConfig());
	}

	@Override
	public void start() {
		changeCurrentState(HAConnectionState.TRANSFER);
		flowMonitor.start();
		readSocketService.start();
		writeSocketService.start();
	}

	@Override
	public void shutdown() {
		changeCurrentState(HAConnectionState.SHUTDOWN);
		writeSocketService.shutdown(true);
		readSocketService.shutdown(true);
		flowMonitor.shutdown(true);
		close();
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

	public void changeCurrentState(HAConnectionState state) {
		log.info("change state to {}", state);
		this.currentState = state;
	}

	class ReadSocketService extends ServiceThread {
		private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
		private final Selector selector;
		private final SocketChannel socketChannel;
		private final ByteBuffer bufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
		private int processPosition = 0;
		private volatile long lastReadTimestamp = System.currentTimeMillis();

		public ReadSocketService(SocketChannel socketChannel) throws IOException {
			this.selector = NetworkUtil.openSelector();
			this.socketChannel = socketChannel;
			this.socketChannel.register(selector, SelectionKey.OP_READ);
			setDaemon(true);
		}

		@Override
		public String getServiceName() {
			if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
				return haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer() + ReadSocketService.class.getSimpleName();
			}
			return ReadSocketService.class.getSimpleName();
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			log.info("{} service started", serviceName);

			while (!this.isStopped()) {
				try {
					selector.select(1000);
					boolean ok = processReadEvent();
					if (!ok) {
						log.error("processReadEvent error");
						break;
					}

					long interval = haService.getDefaultMessageStore().getSystemClock().now() - lastReadTimestamp;
					if (interval > haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
						log.warn("ha housekeeping, found this connection[{}] expired, {}", clientAddress, interval);
						break;
					}
				}
				catch (Exception e) {
					log.error("{} service has exception.", serviceName, e);
				}
			}

			changeCurrentState(HAConnectionState.HANDSHAKE);

			makeStop();

			writeSocketService.makeStop();

			haService.removeConnection(DefaultHAConnection.this);

			haService.getConnectionCount().decrementAndGet();

			SelectionKey sk = socketChannel.keyFor(selector);
			if (sk != null) {
				sk.cancel();
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


		private boolean processReadEvent() {
			int readSizeZeroTimes = 0;

			if (!bufferRead.hasRemaining()) {
				bufferRead.flip();
				processPosition = 0;
			}

			while (bufferRead.hasRemaining()) {
				try {
					int readSize = socketChannel.read(bufferRead);
					if (readSize > 0) {
						readSizeZeroTimes = 0;

						lastReadTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
						if ((bufferRead.position() - processPosition) >= DefaultHAClient.REPORT_HEAD_SIZE) {
							int pos = bufferRead.position() - (bufferRead.position() % DefaultHAClient.REPORT_HEAD_SIZE);
							long readOffset = bufferRead.getLong(pos - 8);
							processPosition = pos;

							slaveAckOffset = readOffset;
							if (slaveAckOffset < 0) {
								slaveAckOffset = readOffset;
								log.info("slave[{]] request offset {}", clientAddress, readOffset);
							}

							haService.notifyTransferSome(slaveAckOffset);
						}
					}
					else if (readSize == 0) {
						if (++readSizeZeroTimes >= 3) {
							break;
						}
					}
					else {
						log.error("read socket[{}] < 0", clientAddress);
						return false;
					}
				}
				catch (IOException e) {
					log.error("processReadEvent exception", e);
					return false;
				}
			}

			return true;
		}
	}

	class WriteSocketService extends ServiceThread {
		private final Selector selector;
		private final SocketChannel socketChannel;
		private final ByteBuffer bufferHeader = ByteBuffer.allocate(TRANSFER_HEADER_SIZE);

		@Getter
		private long nextTransferFromWhere = -1;
		private SelectMappedBufferResult selectMappedBufferResult;
		private boolean lastWriteOver = true;
		private long lastPrintTimestamp = System.currentTimeMillis();
		private long lastWriteTimestamp = System.currentTimeMillis();

		public WriteSocketService(SocketChannel socketChannel) throws IOException {
			this.selector = NetworkUtil.openSelector();
			this.socketChannel = socketChannel;
			this.socketChannel.register(selector, SelectionKey.OP_WRITE);
			this.setDaemon(true);
		}

		@Override
		public String getServiceName() {
			if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
				return haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer() + WriteSocketService.class.getSimpleName();
			}
			return WriteSocketService.class.getSimpleName();
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			log.info("{} service started", serviceName);

			while (!isStopped()) {
				try {
					selector.select(1000);

					if (slaveRequestOffset == -1) {
						Thread.sleep(10);
						continue;
					}

					if (nextTransferFromWhere == -1) {
						if (slaveRequestOffset == 0) {
							long masterOffset = haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
							masterOffset = masterOffset - (masterOffset % haService.getDefaultMessageStore().getMessageStoreConfig().getMappedFileSizeCommitLog());
							if (masterOffset < 0) {
								masterOffset = 0;
							}

							nextTransferFromWhere = masterOffset;
						}
						else {
							nextTransferFromWhere = slaveRequestOffset;
						}

						log.info("master transfer data from {} to slave[{}], and slave request {}", nextTransferFromWhere, clientAddress, slaveRequestOffset);
					}

					if (lastWriteOver) {
						long interval = haService.getDefaultMessageStore().getSystemClock().now - lastWriteTimestamp;
						if (interval > haService.getDefaultMessageStore().getMessageStoreConfig().getHaSendHeartbeatInterval()) {
							bufferHeader.position(0);
							bufferHeader.limit(TRANSFER_HEADER_SIZE);
							bufferHeader.putLong(nextTransferFromWhere);
							bufferHeader.putInt(0);
							bufferHeader.flip();

							lastWriteOver = transferData();
							if (!lastWriteOver) {
								continue;
							}
						}
					}
					else {
						lastWriteOver = transferData();
						if (!lastWriteOver) {
							continue;
						}
					}

					SelectMappedBufferResult selectResult = haService.getDefaultMessageStore().getCommitLogData(nextTransferFromWhere);
					if (selectResult != null) {
						int size = selectResult.getSize();
						if (size > haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
							size = haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
						}

						int canTransferMaxBytes = flowMonitor.canTransferMaxByteNum();
						if (size > canTransferMaxBytes) {
							if (System.currentTimeMillis() - lastPrintTimestamp > 1000) {
								log.warn("Trigger HA flow control, max transfer speed {}KB/s, current speed: {}KB/s",
										String.format("%.2f", flowMonitor.maxTransferByteInSecond() / 1024.0),
										String.format("%.2f", flowMonitor.getTransferByteInSecond() / 1024.0));
								lastPrintTimestamp = System.currentTimeMillis();
							}
							size = canTransferMaxBytes;
						}

						long thisOffset = nextTransferFromWhere;
						nextTransferFromWhere += size;

						selectResult.getByteBuffer().limit(size);
						selectMappedBufferResult = selectResult;

						bufferHeader.position(0);
						bufferHeader.limit(TRANSFER_HEADER_SIZE);
						bufferHeader.putLong(thisOffset);
						bufferHeader.putInt(size);
						bufferHeader.flip();

						lastWriteOver = transferData();
					}
					else {
						haService.getWaitNotifyObject().allWaitForRunning(100);
					}
				}
				catch (Exception e) {
					log.error("{} service has exception.", serviceName, e);
					break;
				}
			}

			haService.getWaitNotifyObject().removeFromWaitingThreadTable();

			if (selectMappedBufferResult != null) {
				selectMappedBufferResult.release();
			}

			changeCurrentState(HAConnectionState.SHUTDOWN);

			makeStop();

			readSocketService.makeStop();

			haService.removeConnection(DefaultHAConnection.this);

			SelectionKey sk = socketChannel.keyFor(selector);
			if (sk != null) {
				sk.cancel();
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

		private boolean transferData() throws Exception {
			int writeSizeZeroTimes = 0;

			while (bufferHeader.hasRemaining()) {
				int writeSize = socketChannel.write(bufferHeader);
				if (writeSize > 0) {
					flowMonitor.addByteCountTransferred(writeSize);
					writeSizeZeroTimes = 0;
					lastWriteTimestamp = haService.getDefaultMessageStore().getSystemClock().now();
				}
				else if (writeSize == 0) {
					if (++writeSizeZeroTimes >= 3) {
						break;
					}
				}
				else {
					throw new Exception("ha master write header error < 0");
				}
			}

			if (selectMappedBufferResult == null) {
				return !selectMappedBufferResult.hasReleased();
			}

			writeSizeZeroTimes = 0;

			if (!bufferHeader.hasRemaining()) {
				while (selectMappedBufferResult.getByteBuffer().hasRemaining()) {
					int writeSize = socketChannel.write(selectMappedBufferResult.getByteBuffer());
					if (writeSize > 0) {
						writeSizeZeroTimes = 0;
						lastWriteTimestamp = haService.getDefaultMessageStore().getSystemClock().now();
					}
					else if (writeSize == 0) {
						if (++writeSizeZeroTimes >= 3) {
							break;
						}
					}
					else {
						throw new Exception("ha master write body error < 0");
					}
				}
			}

			boolean result = !bufferHeader.hasRemaining() && !selectMappedBufferResult.getByteBuffer().hasRemaining();
			if (!selectMappedBufferResult.getByteBuffer().hasRemaining()) {
				selectMappedBufferResult.release();
				selectMappedBufferResult = null;
			}

			return result;
		}
	}
}
