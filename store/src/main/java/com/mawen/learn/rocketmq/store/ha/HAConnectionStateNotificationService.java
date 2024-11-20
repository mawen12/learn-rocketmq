package com.mawen.learn.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/11
 */
public class HAConnectionStateNotificationService extends ServiceThread  {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private static final long CONNECTION_ESTABLISH_TIMEOUT = 10 * 1000;

	private volatile HAConnectionStateNotificationRequest request;
	private volatile long lastCheckTimestamp = -1;
	private HAService haService;
	private DefaultMessageStore defaultMessageStore;

	public HAConnectionStateNotificationService(HAService haService, DefaultMessageStore defaultMessageStore) {
		this.haService = haService;
		this.defaultMessageStore = defaultMessageStore;
	}

	@Override
	public String getServiceName() {
		if (defaultMessageStore != null && defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
			return defaultMessageStore.getBrokerIdentify().getIdentifier() + HAConnectionStateNotificationService.class.getSimpleName();
		}
		return HAConnectionStateNotificationService.class.getSimpleName();
	}

	@Override
	public void run() {
		String serviceName = getServiceName();
		log.info("{} service started", serviceName);

		while (!isStopped()) {
			try {
				waitForRunning(1000);
				doWaitConnectionState();
			}
			catch (Exception e) {
				log.warn("{} service has exception", serviceName, e);
			}
		}

		log.info("{} service end", serviceName);
	}

	public synchronized void setRequest(HAConnectionStateNotificationRequest request) {
		if (this.request != null) {
			this.request.getRequestFuture().cancel(true);
		}
		this.request = request;
		this.lastCheckTimestamp = System.currentTimeMillis();
	}

	private synchronized void doWaitConnectionState() {
		if (request == null || request.getRequestFuture().isDone()) {
			return;
		}

		if (defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
			if (haService.getHAClient().getCurrentState() == request.getExpectState()) {
				request.getRequestFuture().complete(true);
				request = null;
			}
			else if (haService.getHAClient().getCurrentState() == HAConnectionState.READY) {
				if ((System.currentTimeMillis() - lastCheckTimestamp) > CONNECTION_ESTABLISH_TIMEOUT) {
					log.error("Wait HA connection establish with {} timeout", request.getRemoteAddr());
					request.getRequestFuture().complete(false);
					request = null;
				}
			}
			else {
				lastCheckTimestamp = System.currentTimeMillis();
			}
		}
		else {
			boolean connectionFound = false;
			for (HAConnection connection : haService.getConnectionList()) {
				if (checkConnectionStateAndNotify(connection)) {
					connectionFound = true;
				}
			}

			if (connectionFound) {
				lastCheckTimestamp = System.currentTimeMillis();
			}

			if (!connectionFound && (System.currentTimeMillis() - lastCheckTimestamp) > CONNECTION_ESTABLISH_TIMEOUT) {
				log.error("Wait HA connection establish with {} timeout", request.getRemoteAddr());
				request.getRequestFuture().complete(false);
				request = null;
			}
		}
	}

	public synchronized boolean checkConnectionStateAndNotify(HAConnection connection) {
		if (request == null || connection == null) {
			return false;
		}

		try {
			String remoteAddress = ((InetSocketAddress)connection.getSocketChannel().getRemoteAddress())
					.getAddress().getHostAddress();
			if (remoteAddress.equals(request.getRemoteAddr())) {
				HAConnectionState currentState = connection.getCurrentState();

				if (currentState == request.getExpectState()) {
					request.getRequestFuture().complete(true);
					request = null;
				}
				else if (request.isNotifyWhenShutdown() && currentState == HAConnectionState.SHUTDOWN) {
					request.getRequestFuture().complete(false);
					request = null;
				}
				return true;
			}
		}
		catch (IOException e) {
			log.error("Check connection address exception: {}", e);
		}
		return false;
	}
}
