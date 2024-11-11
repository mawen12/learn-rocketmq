package com.mawen.learn.rocketmq.store.ha;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;
import com.mawen.learn.rocketmq.remoting.common.RemotingHelper;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * Report header buffer, size: 8, schema slaveMaxOffset, Format:
 *
 * <pre>
 * ┌────────────────┐
 * │ slaveMaxOffset │
 * ├────────────────┤
 * │ 8 bytes        │
 * └────────────────┘
 * </pre>
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/8
 */
public class DefaultHAClient extends ServiceThread implements HAClient {

	public static final int REPORT_HEADER_SIZE = 8;

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
	private static final int READ_MAX_BUFFER_SIZE = 4 * 1024 * 1024;

	private final AtomicReference<String> masterAddress = new AtomicReference<>();
	private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
	private final ByteBuffer reportOffset = ByteBuffer.allocate(REPORT_HEADER_SIZE);

	private SocketChannel socketChannel;
	private Selector selector;

	private long lastReadTimestamp = System.currentTimeMillis();
	private long lastWriteTimestamp = System.currentTimeMillis();
	private long currentReportedOffset = 0;
	private int dispatchPosition = 0;
	private ByteBuffer buffedRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
	private ByteBuffer buffedBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
	private DefaultMessageStore defaultMessageStore;
	private volatile HAConnectionState currentState = HAConnectionState.READY;
	private FlowMonitor flowMonitor;

	public DefaultHAClient(DefaultMessageStore defaultMessageStore) throws IOException {
		this.selector = NetworkUtil.openSelector();
		this.defaultMessageStore = defaultMessageStore;
		this.flowMonitor = new FlowMonitor(defaultMessageStore.getMessageStoreConfig());
	}

	@Override
	public String getServiceName() {
		if (defaultMessageStore != null && defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
			return defaultMessageStore.getBrokeIdentify().getIdentifier() + DefaultHAClient.class.getSimpleName();
		}
		return DefaultHAClient.class.getSimpleName();
	}

	@Override
	public void updateMasterAddress(String newAddress) {
		String currentAddr = masterAddress.get();
		if (masterAddress.compareAndSet(currentAddr, newAddress)) {
			log.info("update master address, OL: {}, NEW: {}", currentAddr, newAddress);
		}
	}

	@Override
	public void updateHaMasterAddress(String newAddress) {
		String currentAddr = masterHaAddress.get();
		if (masterHaAddress.compareAndSet(currentAddr, newAddress)) {
			log.info("update master ha address, OL: {}, NEW: {}", currentAddr, newAddress);
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
				SelectionKey sk = socketChannel.keyFor(selector);
				if (sk != null) {
					sk.cancel();
				}

				socketChannel.close();
				socketChannel = null;

				log.info("HAClient close connection with master {}", masterHaAddress.get());
				changeCurrentState(HAConnectionState.READY);
			}
			catch (IOException e) {
				log.warn("close master exception.", e);
			}

			lastReadTimestamp = 0;
			dispatchPosition = 0;

			buffedBackup.position(0);
			buffedBackup.limit(READ_MAX_BUFFER_SIZE);

			buffedRead.position(0);
			buffedRead.limit(READ_MAX_BUFFER_SIZE);
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
						return;
					case READY:
						if (!connectMaster()) {
							log.warn("HAClient connect to master {} failed", masterHaAddress.get());
							waitForRunning(5 * 1000);
						}
						continue;
					case TRANSFER:
						if (!transferFromMaster()) {
							closeMasterAndWait();
							continue;
						}
						break;
					default:
						waitForRunning(2 * 1000);
						continue;
				}

				long interval = defaultMessageStore.now() - lastReadTimestamp;
				if (interval > defaultMessageStore.getMessageStoreConfig().getHaHouseKeepingInterval()) {
					log.warn("AutoRecoverHAClient, housekeeping, found this connection[{}] expired, {}",
							masterHaAddress.get(), interval);
					closeMaster();
					log.warn("AutoRecoverHAClient, master not response some time, so close connection");
				}
			}
			catch (Exception e) {
				log.warn("{} service has exception.", e);
				closeMasterAndWait();
			}
		}

		flowMonitor.shutdown(true);

		log.info("{} service end", serviceName);
	}

	private boolean transferFromMaster() throws IOException {
		boolean result;
		if (isTimeToReportOffset()) {
			log.info("slave report current offset {}", currentReportedOffset);
			result = reportSlaveMaxOffset(currentReportedOffset);
			if (!result) {
				return false;
			}
		}

		selector.select(1000);

		result = processReadEvent();
		if (!result) {
			return false;
		}

		return reportSlaveMaxOffsetPlus();
	}

	private boolean isTimeToReportOffset() {
		long interval = defaultMessageStore.now() - lastReadTimestamp;
		return interval > defaultMessageStore.getMessageStoreConfig().getHaSendHeartbeartInterval();
	}

	private boolean reportSlaveMaxOffset(final long maxOffset) {
		reportOffset.position(0);
		reportOffset.limit(REPORT_HEADER_SIZE);
		reportOffset.putLong(maxOffset);
		reportOffset.position(0);
		reportOffset.limit(REPORT_HEADER_SIZE);

		for (int i = 0; i < 3 && reportOffset.hasRemaining(); i++) {
			try {
				socketChannel.write(reportOffset);
			}
			catch (IOException e) {
				log.error("{} reportSlaveMaxOffset socketChannel.write exception", e);
				return false;
			}
		}

		lastReadTimestamp = defaultMessageStore.getSystemClock().now();
		return !reportOffset.hasRemaining();
	}

	private boolean processReadEvent() {
		int readSizeZeroTimes = 0;
		while (buffedRead.hasRemaining()) {
			try {
				int readSize = socketChannel.read(buffedRead);
				if (readSize > 0) {
					flowMonitor.addByteCountTransferred(readSize);
					readSizeZeroTimes = 0;
					boolean result = dispatchReadRequest();
					if (!result) {
						log.error("HAClient, dispatchReadRequest error");
						return false;
					}
					lastReadTimestamp = System.currentTimeMillis();
				}
				else if (readSize == 0) {
					if (++readSizeZeroTimes >= 3) {
						break;
					}
				}
				else {
					log.info("HAClient, processReadEvent read socket < 0");
					return false;
				}
			}
			catch (IOException e) {
				log.error("HAClient, processReadEvent read socket channel exception", e);
				return false;
			}
		}

		return true;
	}


	private boolean dispatchReadRequest() {
		int readSocketPos = buffedRead.position();

		while (true) {
			int diff = buffedRead.position() - dispatchPosition;
			if (diff >= DefaultHAConnection.TRANSFER_HEADER_SIZE) {
				long masterPhyOffset = buffedRead.getLong(dispatchPosition);
				int bodySize = buffedRead.getInt(dispatchPosition + 8);

				long slavePhyOffset = defaultMessageStore.getMaxPhyOffset();
				if (slavePhyOffset != 0) {
					if (slavePhyOffset != masterPhyOffset) {
						log.error("master pushed offset not equal the max phy offset in slave, SLAVE: {} MASTER: {}",
								slavePhyOffset, masterPhyOffset);
						return false;
					}
				}

				if (diff >= (DefaultHAConnection.TRANSFER_HEADER_SIZE + bodySize)) {
					byte[] bodyData = buffedRead.array();
					int dataStart = dispatchPosition + DefaultHAConnection.TRANSFER_HEADER_SIZE;

					defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData, dataStart, bodySize);

					buffedRead.position(readSocketPos);
					dispatchPosition += DefaultHAConnection.TRANSFER_HEADER_SIZE + bodySize;

					if (!reportSlaveMaxOffsetPlus()) {
						return false;
					}

					continue;
				}
			}

			if (!buffedRead.hasRemaining()) {
				reallocateByteBuffer();
			}

			break;
		}

		return true;
	}

	private void reallocateByteBuffer() {
		int remain = READ_MAX_BUFFER_SIZE + dispatchPosition;
		if (remain > 0) {
			buffedRead.position(dispatchPosition);

			buffedBackup.position(0);
			buffedBackup.limit(READ_MAX_BUFFER_SIZE);
			buffedBackup.put(buffedRead);
		}

		swapByteBuffer();

		buffedRead.position(remain);
		buffedRead.limit(READ_MAX_BUFFER_SIZE);
		dispatchPosition = 0;
	}

	private void swapByteBuffer() {
		ByteBuffer tmp = buffedRead;
		buffedRead = buffedBackup;
		buffedBackup = tmp;
	}

	private boolean reportSlaveMaxOffsetPlus() {
		boolean result = true;
		long currentPhyOffset = defaultMessageStore.getMaxPhyOffset();

		if (currentPhyOffset > currentReportedOffset) {
			currentReportedOffset = currentPhyOffset;
			result = reportSlaveMaxOffset(currentReportedOffset);
			if (!result) {
				closeMaster();
				log.error("HAClient, reportSlaveMaxOffset error {}", currentReportedOffset);
			}
		}

		return result;
	}

	public boolean connectMaster() throws ClosedChannelException {
		if (socketChannel == null) {
			String addr = masterHaAddress.get();
			if (addr != null) {
				SocketAddress socketAddress = NetworkUtil.string2SocketAddress(addr);
				socketChannel = RemotingHelper.connect(socketAddress);
				if (socketChannel != null) {
					socketChannel.register(selector, SelectionKey.OP_READ);
					log.info("HAClient connect to master {}", addr);
					changeCurrentState(HAConnectionState.TRANSFER);
				}
			}

			currentReportedOffset = defaultMessageStore.getMaxPhyOffset();

			lastReadTimestamp = System.currentTimeMillis();
		}

		return socketChannel != null;
	}

	public void closeMasterAndWait() {
		closeMaster();
		waitForRunning(5 * 1000);
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
			log.warn("Close the selector of AutoRecoverHAClient error", e);
		}
	}
}
