package com.mawen.learn.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;
import com.mawen.learn.rocketmq.remoting.protocol.body.HARuntimeInfo;
import com.mawen.learn.rocketmq.store.CommitLog;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.config.BrokerRole;
import com.mawen.learn.rocketmq.store.config.MessageStoreConfig;
import lombok.Getter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/11
 */
@Getter
public class DefaultHAService implements HAService {

	private static final Logger log = LoggerFactory.getLogger(DefaultHAService.class);

	protected final AtomicInteger connectionCount = new AtomicInteger(0);

	protected final List<HAConnection> connectionList = new LinkedList<>();

	protected AcceptSocketService acceptSocketService;

	protected DefaultMessageStore defaultMessageStore;

	protected WaitNotifyObject waitNotifyObject = new WaitNotifyObject();

	protected AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

	protected GroupTransferService groupTransferService;

	protected HAClient haClient;

	protected HAConnectionStateNotificationService haConnectionStateNotificationService;

	@Override
	public void init(DefaultMessageStore defaultMessageStore) throws IOException {
		this.defaultMessageStore = defaultMessageStore;
		this.acceptSocketService = new DefaultAcceptSocketService(defaultMessageStore.getMessageStoreConfig());
		this.groupTransferService = new GroupTransferService(this, defaultMessageStore);
		if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
			this.haClient = new DefaultHAClient(this.defaultMessageStore);
		}
		this.haConnectionStateNotificationService = new HAConnectionStateNotificationService(this, defaultMessageStore);
	}

	@Override
	public void start() throws Exception {
		acceptSocketService.beginAccept();
		acceptSocketService.start();
		groupTransferService.start();
		haConnectionStateNotificationService.start();
		if (haClient != null) {
			haClient.start();
		}
	}

	public void addConnection(final HAConnection conn) {
		synchronized (connectionList) {
			connectionList.add(conn);
		}
	}

	public void removeConnection(final HAConnection conn) {
		haConnectionStateNotificationService.checkConnectionStateAndNotify(conn);
		synchronized (connectionList) {
			connectionList.remove(conn);
		}
	}

	@Override
	public void shutdown() {
		if (haClient != null) {
			haClient.shutdown();
		}
		acceptSocketService.shutdown(true);
		destroyConnections();
		groupTransferService.shutdown();
		haConnectionStateNotificationService.shutdown();
	}

	public void destroyConnections() {
		synchronized (connectionList) {
			for (HAConnection c : connectionList) {
				c.shutdown();
			}
			connectionList.clear();
		}
	}

	@Override
	public void updateMasterAddress(String newAddr) {
		if (haClient != null) {
			haClient.updateMasterAddress(newAddr);
		}
	}

	@Override
	public void updateHaMasterAddress(String newAddr) {
		if (haClient != null) {
			haClient.updateHaMasterAddress(newAddr);
		}
	}

	@Override
	public int inSyncReplicasNums(long masterPutWhere) {
		int inSyncNums = 1;
		for (HAConnection connection : connectionList) {
			if (isInSyncSlave(masterPutWhere, connection)) {
				inSyncNums++;
			}
		}
		return inSyncNums;
	}

	protected boolean isInSyncSlave(final long masterPutWhere, HAConnection connection) {
		if (masterPutWhere - connection.getSlaveAckOffset() < defaultMessageStore.getMessageStoreConfig().getHaMaxGapNotInSync()) {
			return true;
		}
		return false;
	}

	@Override
	public AtomicInteger getConnectionCount() {
		return connectionCount;
	}

	@Override
	public void putRequest(CommitLog.GroupCommitRequest request) {
		groupTransferService.putRequest(request);
	}

	@Override
	public void putGroupConnectionStateRequest(HAConnectionStateNotificationRequest request) {
		haConnectionStateNotificationService.setRequest(request);
	}

	@Override
	public List<HAConnection> getConnectionList() {
		return connectionList;
	}

	@Override
	public HAClient getHAClient() {
		return haClient;
	}

	@Override
	public AtomicLong getPush2SlaveMaxOffset() {
		return push2SlaveMaxOffset;
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
			int inSyncNums = 0;

			info.setMasterCommitLogMaxOffset(masterPutWhere);

			for (HAConnection connection : connectionList) {
				HARuntimeInfo.HAConnectionRuntimeInfo cinfo = new HARuntimeInfo.HAConnectionRuntimeInfo();

				long slaveAckOffset = connection.getSlaveAckOffset();
				cinfo.setSlaveAckOffset(slaveAckOffset);
				cinfo.setDiff(masterPutWhere - slaveAckOffset);
				cinfo.setAddr(connection.getClientAddress().substring(1));
				cinfo.setTransferredByteInSecond(connection.getTransferredByteInSecond());
				cinfo.setTransferFromWhere(connection.getTransferFromWhere());

				boolean isInSync = isInSyncSlave(masterPutWhere, connection);
				if (isInSync) {
					inSyncNums++;
				}
				cinfo.setInSync(isInSync);

				info.getHaConnectionInfo().add(cinfo);
			}
			info.setInSyncSlaveNums(inSyncNums);
		}
		return info;
	}

	@Override
	public WaitNotifyObject getWaitNotifyObject() {
		return null;
	}

	@Override
	public boolean isSlaveOK(long masterPutWhere) {
		return false;
	}

	protected abstract class AcceptSocketService extends ServiceThread {
		private final SocketAddress socketAddressListen;
		private ServerSocketChannel serverSocketChannel;
		private Selector selector;

		private final MessageStoreConfig messageStoreConfig;

		public AcceptSocketService(MessageStoreConfig messageStoreConfig) {
			this.messageStoreConfig = messageStoreConfig;
			this.socketAddressListen = new InetSocketAddress(messageStoreConfig.getHaListenPort());
		}

		public void beginAccept() throws Exception {
			this.serverSocketChannel = ServerSocketChannel.open();
			this.selector = NetworkUtil.openSelector();
			this.serverSocketChannel.socket().setReuseAddress(true);
			this.serverSocketChannel.socket().bind(this.socketAddressListen);
			if (messageStoreConfig.getHaListenPort() == 0) {
				messageStoreConfig.setHaListenPort(serverSocketChannel.socket().getLocalPort());
				log.info("OS picked up {} to listen for HA", messageStoreConfig.getHaListenPort());
			}
			this.serverSocketChannel.configureBlocking(false);
			this.serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
		}

		@Override
		public void shutdown() {
			super.shutdown();
		}

		@Override
		public void shutdown(boolean interrupt) {
			super.shutdown(interrupt);
			try {
				if (serverSocketChannel != null) {
					serverSocketChannel.close();
				}
				if (selector != null) {
					selector.close();
				}
			}
			catch (IOException e) {
				log.error("AcceptSocketService shutdown exception", e);
			}
		}

		@Override
		public void run() {
			String serviceName = getServiceName();
			log.info("{} service started", serviceName);

			while (!isStopped()) {
				try {
					selector.select(1000);
					Set<SelectionKey> selected = selector.selectedKeys();

					if (selected != null) {
						for (SelectionKey k : selected) {
							if (k.isAcceptable()) {
								SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();
								if (sc != null) {
									log.info("HAService receive new connection. {}", sc.socket().getRemoteSocketAddress());
									try {
										HAConnection conn = createConnection(sc);
										conn.start();
										addConnection(conn);
									}
									catch (Exception e) {
										log.error("new HAConnection exception", e);
										sc.close();
									}
								}
							}
							else {
								log.warn("Unexpected ops in select {}", k.readyOps());
							}
						}

						selected.clear();
					}
				}
				catch (Exception e) {
					log.error("{} service has exception", serviceName, e);
				}
			}

			log.info("{} service end", serviceName);
		}

		protected abstract HAConnection createConnection(final SocketChannel sc) throws IOException;
	}

	class DefaultAcceptSocketService extends AcceptSocketService {

		public DefaultAcceptSocketService(MessageStoreConfig messageStoreConfig) {
			super(messageStoreConfig);
		}

		@Override
		protected HAConnection createConnection(SocketChannel sc) throws IOException {
			return new DefaultHAConnection(DefaultHAService.this,sc);
		}

		@Override
		public String getServiceName() {
			if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
				return defaultMessageStore.getBrokerConfig().getIdentifier() + AcceptSocketService.class.getSimpleName();
			}
			return DefaultAcceptSocketService.class.getSimpleName();
		}
	}
}
