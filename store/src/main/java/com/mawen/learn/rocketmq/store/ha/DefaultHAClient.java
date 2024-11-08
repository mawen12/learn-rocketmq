package com.mawen.learn.rocketmq.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.graph.Network;
import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import io.netty.buffer.ByteBufAllocator;
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
	private final ByteBuffer reportBuffer = ByteBuffer.allocate(REPORT_HEADER_SIZE);

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
		return flowMonitor.getTransferredByteInSecond;
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
							closeMasterAndAwait();
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

	private boolean transferFromMaster() {
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

	public void closeMasterAndAwait() {
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
