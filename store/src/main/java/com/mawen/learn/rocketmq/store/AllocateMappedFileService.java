package com.mawen.learn.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.store.config.BrokerRole;
import com.mawen.learn.rocketmq.store.logfile.DefaultMappedFile;
import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
public class AllocateMappedFileService extends ServiceThread {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private static int waitTimeOut = 5 * 1000;

	private ConcurrentMap<String, AllocateRequest> requestTable = new ConcurrentHashMap<>();

	private PriorityBlockingQueue<AllocateRequest> requestQueue = new PriorityBlockingQueue<>();

	private volatile boolean hasException = false;

	private DefaultMessageStore messageStore;

	public AllocateMappedFileService(DefaultMessageStore messageStore) {
		this.messageStore = messageStore;
	}

	public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {
		int canSubmitRequests = 2;
		if (messageStore.isTransientStorePoolEnable()) {
			if (messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
					&& BrokerRole.SLAVE != messageStore.getMessageStoreConfig().getBrokerRole()) {
				canSubmitRequests = messageStore.remainTransientStoreBufferNumbs() - requestQueue.size();
			}
		}

		AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
		boolean nextPutOK = requestTable.putIfAbsent(nextFilePath, nextReq) == null;

		if (nextPutOK) {
			if (canSubmitRequests <= 0) {
				log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, RequestQueueSize: {}, StorePoolSize: {}", requestQueue.size(), messageStore.remainTransientStoreBufferNumbs());
				requestTable.remove(nextFilePath);
				return null;
			}
			boolean offerOK = requestQueue.offer(nextReq);
			if (!offerOK) {
				log.warn("never expected here, add a request to preallocate queue failed");
			}
			canSubmitRequests--;
		}

		AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
		boolean nextNextPutOK = requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
		if (nextNextPutOK) {
			if (canSubmitRequests <= 0) {
				log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, RequestQueueSize: {}, StorePoolSize: {}",
						requestQueue.size(), messageStore.remainTransientStoreBufferNumbs());
				requestTable.remove(nextNextFilePath);
			}
			else {
				boolean offerOK = requestQueue.offer(nextNextReq);
				if (!offerOK) {
					log.warn("never expected here, add a request to preallocate queue failed");
				}
			}
		}

		if (hasException) {
			log.warn("{} service has exception, so return null", getServiceName());
			return null;
		}

		AllocateRequest result = requestTable.get(nextFilePath);
		try {
			if (result != null) {
				messageStore.getPerfCounter().startTick("WAIT_MAPFILE_TIME_MS");
				boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
				messageStore.getPerfCounter().endTick("WAIT_MAPFILE_TIME_MS");
				if (!waitOK) {
					log.warn("create mmap timeout {} {}", result.getFilePath(), result.getFileSize());
					return null;
				}
				else {
					requestTable.remove(nextFilePath);
					return result.getMappedFile();
				}
			}
			else {
				log.error("find preallocate mmap failed, this never happen");
			}
		}
		catch (InterruptedException e) {
			log.warn("{} service has exception.", getServiceName(), e);
		}
		return null;
	}

	@Override
	public void run() {
		String serviceName = getServiceName();
		log.info("{} service started", serviceName);

		while (!isStopped() && mmapOperation()) {

		}

		log.info("{} service end", serviceName);
	}

	@Override
	public String getServiceName() {
		if (messageStore != null && messageStore.getBrokerConfig().isInBrokerContainer()) {
			return messageStore.getBrokerIdentity().getIdentifier() + AllocateMappedFileService.class.getSimpleName();
		}
		return AllocateMappedFileService.class.getSimpleName();
	}

	@Override
	public void shutdown() {
		super.shutdown(true);
		for (AllocateRequest req : requestTable.values()) {
			if (req.mappedFile != null) {
				log.info("delete pre allocated mapped file, {}", req.mappedFile.getFileName());
				req.mappedFile.destroy(1000);
			}
		}
	}

	private boolean mmapOperation() {
		boolean isSuccess = false;
		AllocateRequest request = null;
		try {
			request = requestQueue.take();
			AllocateRequest expectedRequest = requestTable.get(request.getFilePath());
			if (expectedRequest == null) {
				log.warn("this mmap request expired, may cause timeout {} {}", request.getFilePath(), request.getFileSize());
				return true;
			}
			if (expectedRequest != request) {
				log.warn("never expected here, maybe cause timeout {} {}, req: {}, expectedRequest: {}", request.getFilePath(), request.getFileSize(), request, expectedRequest);
				return true;
			}

			if (request.getMappedFile() == null) {
				long begin = System.currentTimeMillis();
				MappedFile mappedFile;
				if (messageStore.isTransientStorePoolEnable()) {
					try {
						mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
						mappedFile.init(request.getFilePath(), request.getFileSize(), messageStore.getTransientStorePool());
					}
					catch (RuntimeException e) {
						log.warn("Use default implementation.");
						mappedFile = new DefaultMappedFile(request.getFilePath(), request.getFileSize(), messageStore.getTransientStorePool());
					}
				}
				else {
					mappedFile = new DefaultMappedFile(request.getFilePath(), request.getFileSize());
				}

				long elapsedTime = UtilAll.computeElapsedTimeMilliseconds(begin);
				if (elapsedTime > 10) {
					int queueSize = requestQueue.size();
					log.warn("create mappedFile spent time(ms) {} queue size {} {} {}",
							elapsedTime, queueSize, request.getFilePath(), request.getFileSize());
				}

				if (mappedFile.getFileSize() >= messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog()
						&& messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
					mappedFile.warmMappedFile(messageStore.getMessageStoreConfig().getFlushDiskType(), messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
				}

				request.setMappedFile(mappedFile);
				this.hasException = false;
				isSuccess = true;
			}
		}
		catch (InterruptedException e) {
			log.warn("{} interrupted, possibly by shutdown.", getServiceName());
			this.hasException = true;
			return false;
		}
		catch (IOException e) {
			log.warn("{} service has exception.", getServiceName(), e);
			this.hasException = true;
			if (request != null) {
				requestQueue.offer(request);
				try {
					Thread.sleep(1);
				}
				catch (InterruptedException ignored) {
				}
			}
		}
		finally {
			if (request != null && isSuccess) {
				request.getCountDownLatch().countDown();
			}
		}
		return true;
	}

	@Getter
	@Setter
	@EqualsAndHashCode(of = {"filePath", "fileSize"})
	static class AllocateRequest implements Comparable<AllocateRequest> {

		private String filePath;
		private int fileSize;
		private CountDownLatch countDownLatch = new CountDownLatch(1);
		private volatile MappedFile mappedFile;

		public AllocateRequest(String filePath, int fileSize) {
			this.filePath = filePath;
			this.fileSize = fileSize;
		}

		@Override
		public int compareTo(AllocateRequest other) {
			if (this.fileSize < other.fileSize)
				return 1;
			else if (this.fileSize > other.fileSize) {
				return -1;
			}
			else {
				int mIndex = this.filePath.lastIndexOf(File.separator);
				long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
				int oIndex = other.filePath.lastIndexOf(File.separator);
				long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
				if (mName < oName) {
					return -1;
				}
				else if (mName > oName) {
					return 1;
				}
				else {
					return 0;
				}
			}
		}
	}
}
