package com.mawen.learn.rocketmq.store;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
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
			} else {
				int mIndex = this.filePath.lastIndexOf(File.separator);
				long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
				int oIndex = other.filePath.lastIndexOf(File.separator);
				long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
				if (mName < oName) {
					return -1;
				} else if (mName > oName) {
					return 1;
				} else {
					return 0;
				}
			}
		}
	}
}
