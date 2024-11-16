package com.mawen.learn.rocketmq.store;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import com.mawen.learn.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
public class FlushDiskWatcher extends ServiceThread {

	private static final Logger log = LoggerFactory.getLogger(FlushDiskWatcher.class);

	private final LinkedBlockingQueue<CommitLog.GroupCommitRequest> commitRequests = new LinkedBlockingQueue<>();

	@Override
	public String getServiceName() {
		return FlushDiskWatcher.class.getSimpleName();
	}

	@Override
	public void run() {
		while (!isStopped()) {
			CommitLog.GroupCommitRequest req = null;
			try {
				req = commitRequests.take();
			}
			catch (InterruptedException e) {
				log.warn("take flush disk commit request, but interrupted, this may caused by shutdown");
				continue;
			}

			while (!req.future().isDone()) {
				long now = System.nanoTime();
				if (now - req.getDeadLine() >= 0) {
					req.wakeupCustomer(PutMessageStatus.FLUSH_DISK_TIMEOUT);
					break;
				}

				long sleepTime = (req.getDeadLine() - now) / 1_000_000;
				sleepTime = Math.min(10, sleepTime);
				if (sleepTime == 0) {
					req.wakeupCustomer(PutMessageStatus.FLUSH_DISK_TIMEOUT);
					break;
				}

				try {
					Thread.sleep(sleepTime);
				}
				catch (InterruptedException e) {
					log.warn("An exception occurred while watching for flushing disk to complete. this may caused by shutdown");
					break;
				}
			}
		}
	}

	public void add(CommitLog.GroupCommitRequest request) {
		commitRequests.add(request);
	}

	public int queueSize() {
		return commitRequests.size();
	}
}
