package com.mawen.learn.rocketmq.store.ha;

import java.util.LinkedList;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.store.CommitLog;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.PutMessageSpinLock;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/11
 */
public class GroupTransferService extends ServiceThread  {

	private static final Logger log = LoggerFactory.getLogger(GroupTransferService.class);

	private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
	private final PutMessageSpinLock lock = new PutMessageSpinLock();
	private final DefaultMessageStore defaultMessageStore;
	private final HAService haService;
	private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new LinkedList<>();
	private volatile List<CommitLog.GroupCommitRequest> requestsRead = new LinkedList<>();

	public GroupTransferService(final HAService haService, final DefaultMessageStore defaultMessageStore) {
		this.haService = haService;
		this.defaultMessageStore = defaultMessageStore;
	}

	public void putRequest(final CommitLog.GroupCommitRequest request) {
		lock.lock();
		try {
			requestsWrite.add(request);
		}
		finally {
			lock.unlock();
		}
		wakeup();
	}

	public void swapRequests() {
		lock.lock();
		try {
			List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
			this.requestsWrite = this.requestsRead;
			this.requestsRead = tmp;
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public String getServiceName() {
		if (defaultMessageStore != null && defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
			return defaultMessageStore.getBrokerIdentify().getIdentifier() + GroupTransferService.class.getSimpleName();
		}
		return GroupTransferService.class.getSimpleName();
	}

	@Override
	protected void onWaitEnd() {
		swapRequests();
	}

	@Override
	public void run() {
		String serviceName = getServiceName();
		log.info("{} service started", serviceName);

		while (!isStopped()) {
			try {
				waitForRunning(10);
				doWaitTransfer();
			}
			catch (Exception e) {
				log.warn("{} service has exception", serviceName, e);
			}
		}

		log.info("{} service end", serviceName);
	}

}
