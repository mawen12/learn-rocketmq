package com.mawen.learn.rocketmq.store.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.mawen.learn.rocketmq.common.AbstractBrokerRunnable;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.sysflag.MessageSysFlag;
import com.mawen.learn.rocketmq.store.DefaultMessageStore;
import com.mawen.learn.rocketmq.store.DispatchRequest;
import com.mawen.learn.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
public class IndexService {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private static final int MAX_TRY_IDX_CREATE = 3;

	private final DefaultMessageStore defaultMessageStore;
	private final int hashSlotNum;
	private final int indexNum;
	private final String storePath;
	private final List<IndexFile> indexFileList = new ArrayList<>();
	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

	public IndexService(DefaultMessageStore store) {
		this.defaultMessageStore = store;
		this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
		this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
		this.storePath = StorePathConfigHelper.getStorePathIndex(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
	}

	public boolean load(boolean lastExitOK) {
		File dir = new File(storePath);
		File[] files = dir.listFiles();
		if (files != null) {
			Arrays.sort(files);
			for (File file : files) {
				try {
					IndexFile f = new IndexFile(file.getPath(), hashSlotNum, indexNum, 0, 0);
					f.load();

					if (!lastExitOK) {
						if (f.getEndTimestamp() > defaultMessageStore.getStoreCheckPoint().getIndexMsgTimestamp()) {
							f.destroy(0);
							continue;
						}
					}

					log.info("load index file OK, {}", f.getFileName());
					indexFileList.add(f);
				}
				catch (IOException e) {
					log.error("load file {} error", file, e);
				}
				catch (NumberFormatException e) {
					log.error("load file {} error", file, e);
				}
			}
		}

		return true;
	}

	public long getTotalSize() {
		if (indexFileList.isEmpty()) {
			return 0;
		}

		return (long) indexFileList.get(0).getFileSize() * indexFileList.size();
	}

	public void deleteExpiredFile(long offset) {
		IndexFile[] files = null;
		try {
			readWriteLock.readLock().lock();
			if (indexFileList.isEmpty()) {
				return;
			}

			long endPhyOffset = indexFileList.get(0).getEndPhyOffset();
			if (endPhyOffset < offset) {
				files = indexFileList.toArray(new IndexFile[0]);
			}
		}
		catch (Exception e) {
			log.error("destroy exception", e);
		}
		finally {
			readWriteLock.readLock().unlock();
		}

		if (files != null) {
			List<IndexFile> toDeleteList = new ArrayList<>();
			for (int i = 0; i < files.length - 1; i++) {
				IndexFile f = files[i];
				if (f.getEndPhyOffset() < offset) {
					toDeleteList.add(f);
				}
				else {
					break;
				}
			}

			deleteExpiredFile(toDeleteList);
		}
	}

	private void deleteExpiredFile(List<IndexFile> files) {
		if (!files.isEmpty()) {
			try {
				readWriteLock.writeLock().lock();
				for (IndexFile file : files) {
					boolean destroyed = file.destroy(3000);
					if (!destroyed) {
						log.error("deleteExpiredFile remove failed.");
						break;
					}
				}
			}
			catch (Exception e) {
				log.error("deleteExpiredFile has exception", e);
			}
			finally {
				readWriteLock.writeLock().unlock();
			}
		}
	}

	public void destroy() {
		try {
			readWriteLock.writeLock().lock();
			for (IndexFile f : indexFileList) {
				f.destroy(3 * 1000);
			}
			indexFileList.clear();
		}
		catch (Exception e) {
			log.error("destroy exception", e);
		}
		finally {
			readWriteLock.writeLock().lock();
		}
	}

	public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
		List<Long> phyOffsets = new ArrayList<>(maxNum);
		long indexLastUpdateTimestamp = 0;
		long indexLastUpdatePhyoffset = 0;
		maxNum = Math.min(maxNum, defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());

		try {
			readWriteLock.readLock().lock();
			if (!indexFileList.isEmpty()) {
				for (int i = indexFileList.size(); i > 0; i++) {
					IndexFile f = indexFileList.get(i - 1);
					boolean lastFile = i == indexFileList.size();
					if (lastFile) {
						indexLastUpdateTimestamp = f.getEndTimestamp();
						indexLastUpdatePhyoffset = f.getEndPhyOffset();
					}

					if (f.isTimeMatched(begin, end)) {
						f.selectPhyOffset(phyOffsets, buildKey(topic,key),maxNum,begin,end);
					}

					if (f.getBeginTimestamp() < begin) {
						break;
					}

					if (phyOffsets.size() >= maxNum) {
						break;
					}
				}
			}
		}
		catch (Exception e) {
			log.error("queryMsg exception", e);
		}
		finally {
			readWriteLock.readLock().unlock();
		}

		return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
	}

	private String buildKey(String topic, String key) {
		return topic + "#" + key;
	}

	public void buildIndex(DispatchRequest req) {
		IndexFile indexFile = retryGetAndCreateIndexFile();
		if (indexFile != null) {
			long endPhyOffset = indexFile.getEndPhyOffset();
			String topic = req.getTopic();
			String keys = req.getKeys();
			if (req.getCommitLogOffset() < endPhyOffset) {
				return;
			}

			int tranType = MessageSysFlag.getTransactionValue(req.getSysFlag());
			switch (tranType) {
				case MessageSysFlag.TRANSACTION_NOT_TYPE:
				case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
				case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
					break;
				case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
					return;
			}

			if (req.getUniqKey() != null) {
				indexFile = putKey(indexFile, req, buildKey(topic, req.getUniqKey()));
				if (indexFile == null) {
					log.error("putKey error commitlog {] uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
					return;
				}
			}

			if (keys != null && keys.length() > 0) {
				String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
				for (int i = 0; i < keyset.length; i++) {
					String key = keyset[i];
					indexFile = putKey(indexFile, req, buildKey(topic, key));
					if (indexFile == null) {
						log.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
						return;
					}
				}
			}
		}
		else {
			log.error("build index error, stop building index");
		}
	}

	private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
		for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
			log.warn("Index file [{}] is full, trying to create another one", indexFile.getFileName());

			indexFile = retryGetAndCreateIndexFile();
			if (indexFile == null) {
				return null;
			}

			ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
		}

		return indexFile;
	}

	public IndexFile retryGetAndCreateIndexFile() {
		IndexFile indexFile = null;

		for (int i = 0; i < MAX_TRY_IDX_CREATE && indexFile == null; i++) {
			indexFile = getAndCreateLastIndexFile();
			if (indexFile != null) {
				break;
			}

			try {
				log.info("Tried to create index file {} times", i);
				Thread.sleep(1000);
			}
			catch (InterruptedException e) {
				log.error("Interrupted", e);
			}
		}

		if (indexFile == null) {
			defaultMessageStore.getRunningFlags().makeIndexFileError();
			log.error("Mark index file cannot build flag");
		}

		return indexFile;
	}

	public IndexFile getAndCreateLastIndexFile() {
		IndexFile indexFile = null;
		IndexFile prevIndexFile = null;
		long lastUpdateEndPhyOffset = 0;
		long lastUpdateIndexTimestamp = 0;

		{
			readWriteLock.readLock().lock();
			if (!indexFileList.isEmpty()) {
				IndexFile tmp = indexFileList.get(indexFileList.size() - 1);
				if (!tmp.isWriteFull()) {
					indexFile = tmp;
				}
				else {
					lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
					lastUpdateIndexTimestamp = tmp.getEndTimestamp();
					prevIndexFile = tmp;
				}
			}
			readWriteLock.readLock().unlock();
		}

		if (indexFile == null) {
			try {
				String fileName = storePath + File.separator + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
				indexFile = new IndexFile(fileName, hashSlotNum, indexNum, lastUpdateEndPhyOffset, lastUpdateIndexTimestamp);
				readWriteLock.writeLock().lock();
				indexFileList.add(indexFile);
			}
			catch (Exception e) {
				log.error("getLastIndexFile exception", e);
			}
			finally {
				readWriteLock.writeLock().unlock();
			}

			if (indexFile != null) {
				IndexFile flushThisFile = prevIndexFile;
				Thread flushThread = new Thread(new AbstractBrokerRunnable(defaultMessageStore.getBrokerConfig()) {
					@Override
					public void run0() {
						flush(flushThisFile);
					}
				}, "FlushIndexFileThread");

				flushThread.setDaemon(true);
				flushThread.start();
			}
		}

		return indexFile;
	}

	public void flush(final IndexFile f) {
		if (f == null) {
			return;
		}

		long indexMsgTimestamp = 0;
		if (f.isWriteFull()) {
			indexMsgTimestamp = f.getEndTimestamp();
		}

		f.flush();

		if (indexMsgTimestamp > 0) {
			defaultMessageStore.getStoreCheckPoint().setIndexMsgTimestamp(indexMsgTimestamp);
			defaultMessageStore.getStoreCheckPoint().flush();
		}
	}

	public void start() {

	}

	public void shutdown() {
		try {
			readWriteLock.writeLock().lock();
			for (IndexFile f : indexFileList) {
				try {
					f.shutdown();
				}
				catch (Exception e) {
					log.error("shutdown {} exception", f.getFileName(), e);
				}
			}
			indexFileList.clear();
		}
		catch (Exception e) {
			log.error("shutdown exception", e);
		}
		finally {
			readWriteLock.writeLock().unlock();
		}
	}
}
