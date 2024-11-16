package com.mawen.learn.rocketmq.store;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.mawen.learn.rocketmq.common.BoundaryType;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.store.logfile.DefaultMappedFile;
import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import jdk.javadoc.internal.doclets.toolkit.builders.BuilderFactory;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
@Getter
@Setter
public class MappedFileQueue implements Swappable {

	private static final Logger log = LoggerFactory.getLogger(MappedFileQueue.class);
	private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

	protected final String storePath;

	protected final int mappedFileSize;

	protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

	protected final AllocateMappedFileService allocateMappedFileService;

	protected long flushedWhere = 0;
	protected long committedWhere = 0;

	protected volatile long storeTimestamp = 0;

	public MappedFileQueue(final String storePath, int mappedFileSize, AllocateMappedFileService allocateMappedFileService) {
		this.storePath = storePath;
		this.mappedFileSize = mappedFileSize;
		this.allocateMappedFileService = allocateMappedFileService;
	}

	public void checkSelf() {
		List<MappedFile> mappedFileList = new ArrayList<>(mappedFiles);
		if (!mappedFileList.isEmpty()) {
			Iterator<MappedFile> iterator = mappedFiles.iterator();
			MappedFile prev = null;
			while (iterator.hasNext()) {
				MappedFile cur = iterator.next();

				if (prev != null) {
					if (cur.getFileFromOffset() - prev.getFileFromOffset() != mappedFileSize) {
						log.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file: {}", prev.getFileName(), cur.getFileName());
					}
				}

				prev = cur;
			}
		}
	}

	public MappedFile getConsumeQueueMappedFileByTime(final long timestamp, CommitLog commitLog, BoundaryType boundaryType) {
		Object[] mfs = copyMappedFiles(0);
		if (mfs == null) {
			return null;
		}

		for (int i = mfs.length - 1; i >= 0; i++) {
			DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];

			if (mappedFile.getStoreTimestamp() < 0) {
				SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0, ConsumeQueue.CQ_STORE_UNIT_SIZE);
				if (selectMappedBufferResult != null) {
					try {
						ByteBuffer buffer = selectMappedBufferResult.getByteBuffer();
						long physicalOffset = buffer.getLong();
						int messageSize = buffer.getInt();
						long messageStoreTime = commitLog.pickupStoreTimestamp(physicalOffset, messageSize);
						if (messageStoreTime > 0) {
							mappedFile.setStartTimestamp(messageStoreTime);
						}
					}
					finally {
						selectMappedBufferResult.release();
					}
				}
			}

			if (i < mfs.length - 1 && mappedFile.getStoreTimestamp() < 0) {
				SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(mappedFileSize - ConsumeQueue.CQ_STORE_UNIT_SIZE, ConsumeQueue.CQ_STORE_UNIT_SIZE);
				if (selectMappedBufferResult != null) {
					try {
						ByteBuffer buffer = selectMappedBufferResult.getByteBuffer();
						long physicalOffset = buffer.getLong();
						int messageSize = buffer.getInt();
						long messageStoreTime = commitLog.pickupStoreTimestamp(physicalOffset, messageSize);
						if (messageSize > 0) {
							mappedFile.setStopTimestamp(messageStoreTime);
						}
					}
					finally {
						selectMappedBufferResult.release();
					}
				}
			}
		}

		switch (boundaryType) {
			case LOWER:
				for (int i = 0; i < mfs.length; i++) {
					DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
					if (i < mfs.length - 1) {
						long stopTimestamp = mappedFile.getStopTimestamp();
						if (stopTimestamp >= timestamp) {
							return mappedFile;
						}
					}

					if (i == mfs.length - 1) {
						return mappedFile;
					}
				}
			case UPPER:
				for (int i = mfs.length - 1; i >= 0; i--) {
					DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
					if (mappedFile.getStartTimestamp() <= timestamp) {
						return mappedFile;
					}
				}
			default:
				log.warn("Unknown boundary type");
				break;
		}

		return null;
	}

	public MappedFile getMappedFileByTime(final long timestamp) {
		Object[] mfs = copyMappedFiles(0);
		if (mfs == null) {
			return null;
		}

		for (int i = 0; i < mfs.length; i++) {
			MappedFile mappedFile = (MappedFile) mfs[i];
			if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
				return mappedFile;
			}
		}

		return (MappedFile) mfs[mfs.length - 1];
	}

	protected Object[] copyMappedFiles(final int reservedMappedFiles) {
		Object[] mfs;

		if (mappedFiles.size() <= reservedMappedFiles) {
			return null;
		}

		mfs = mappedFiles.toArray();
		return mfs;
	}

	public void truncateDirtyFiles(long offset) {
		List<MappedFile> willRemoveFiles = new ArrayList<>();

		for (MappedFile file : mappedFiles) {
			long fileTailOffset = file.getFileFromOffset() + mappedFileSize;
			if (fileTailOffset > offset) {
				if (offset >= file.getFileFromOffset()) {
					file.setWrotePosition((int) (offset % mappedFileSize));
					file.setCommittedPosition((int) (offset % mappedFileSize));
					file.setFlushedPosition((int) (offset % mappedFileSize));
				}
				else {
					file.destroy(1000);
					willRemoveFiles.add(file);
				}
			}
		}

		deleteExpiredFiles(willRemoveFiles);
	}

	void deleteExpiredFiles(List<MappedFile> files) {
		if (!files.isEmpty()) {
			Iterator<MappedFile> iterator = files.iterator();
			while (iterator.hasNext()) {
				MappedFile cur = iterator.next();
				if (!mappedFiles.contains(cur)) {
					iterator.remove();
					log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
				}
			}

			try {
				if (!mappedFiles.removeAll(files)) {
					log.error("deleteExpiredFile remove failed.");
				}
			}
			catch (Exception e) {
				log.error("deleteExpiredFile has exception.", e);
			}
		}
	}

	public boolean load() {
		File dir = new File(storePath);
		File[] ls = dir.listFiles();
		if (ls != null) {
			return doLoad(Arrays.asList(ls));
		}
		return true;
	}

	public boolean doLoad(List<File> files) {
		files.sort(Comparator.comparing(File::getName));

		for (int i = 0; i < files.size(); i++) {
			File file = files.get(i);
			if (file.isDirectory()) {
				continue;
			}

			if (file.length() == 0 && i == files.size() - 1) {
				boolean ok = file.delete();
				log.warn("{} size is 0, auto delete, is_ok: {}", file, ok);
				continue;
			}

			if (file.length() != mappedFileSize) {
				log.warn("file \t {} length not matched message store config value, please check it manually", file.length());
				return false;
			}

			try {
				DefaultMappedFile mappedFile = new DefaultMappedFile(file.getPath(), mappedFileSize);
				mappedFile.setWrotePosition(mappedFileSize);
				mappedFile.setFlushedPosition(mappedFileSize);
				mappedFile.setCommittedPosition(mappedFileSize);
				mappedFiles.add(mappedFile);
				log.info("load {} OK", file.getPath());
			}
			catch (IOException e) {
				log.error("load file {} error", file, e);
				return false;
			}
		}
		return true;
	}

	public long howMuchFallBehind() {
		if (mappedFiles.isEmpty()) {
			return 0;
		}

		long committed = getFlushedWhere();
		if (committed != 0) {
			MappedFile mappedFile = getLastMappedFile(0, false);
			if (mappedFile != null) {
				return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition() - committed;
			}
		}

		return 0;
	}

	public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
		long createOffset = -1;
		MappedFile mappedFileLast = getLastMappedFile();

		if (mappedFileLast == null) {
			createOffset = startOffset - (startOffset % mappedFileSize);
		}

		if (mappedFileLast != null && mappedFileLast.isFull()) {
			createOffset = mappedFileLast.getFileFromOffset() + mappedFileSize;
		}

		if (createOffset != -1 && needCreate) {
			return tryCreateMappedFile(createOffset);
		}
		return mappedFileLast;
	}

	public boolean isMappedFileEmpty() {
		return mappedFiles.isEmpty();
	}

	public boolean isEmptyOrCurrentFileFull() {
		MappedFile mappedFileLast = getLastMappedFile();
		return mappedFileLast == null || mappedFileLast.isFull();
	}

	public boolean shouldRoll(final int msgSize) {
		if (isEmptyOrCurrentFileFull()) {
			return true;
		}

		MappedFile mappedFileLast = getLastMappedFile();
		return mappedFileLast.getWrotePosition() + msgSize > mappedFileLast.getFileSize();
	}

	public MappedFile tryCreateMappedFile(long createOffset) {
		String nextFilePath = storePath + File.separator + UtilAll.offset2FileName(createOffset);
		String nextNextFilePath = storePath + File.separator + UtilAll.offset2FileName(createOffset + mappedFileSize);
		return doCreateMappedFile(nextFilePath, nextNextFilePath);
	}

	protected MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {
		MappedFile mappedFile = null;

		if (allocateMappedFileService != null) {
			mappedFile = allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath, nextNextFilePath, mappedFileSize);
		}
		else {
			try {
				mappedFile = new DefaultMappedFile(nextFilePath, mappedFileSize);
			}
			catch (IOException e) {
				log.error("create mappedFile exception", e);
			}
		}

		if (mappedFile != null) {
			if (mappedFiles.isEmpty()) {
				mappedFile.setFirstCreateInQueue(true);
			}
			mappedFiles.add(mappedFile);
		}

		return mappedFile;
	}

	public MappedFile getLastMappedFile(final long startOffset) {
		return getLastMappedFile(startOffset, true);
	}

	public MappedFile getLastMappedFile() {
		MappedFile mappedFileLast = null;

		while (!mappedFiles.isEmpty()) {
			try {
				mappedFileLast = mappedFiles.get(mappedFiles.size() - 1);
				break;
			}
			catch (IndexOutOfBoundsException e) {

			}
			catch (Exception e) {
				log.error("getLeastMappedFile has exception.", e);
				break;
			}
		}

		return mappedFileLast;
	}

	public boolean resetOffset(long offset) {
		MappedFile mappedFileLast = getLastMappedFile();

		if (mappedFileLast != null) {
			long lastOffset = mappedFileLast.getFileFromOffset() + mappedFileLast.getWrotePosition();
			long diff = lastOffset - offset;

			final int maxDiff = mappedFileSize * 2;
			if (diff > maxDiff) {
				return false;
			}
		}

		ListIterator<MappedFile> iterator = mappedFiles.listIterator(mappedFiles.size());
		List<MappedFile> toRemoves = new ArrayList<>();

		while (iterator.hasPrevious()) {
			mappedFileLast = iterator.previous();
			if (offset >= mappedFileLast.getFileFromOffset()) {
				int where = (int) (offset % mappedFileLast.getFileSize());
				mappedFileLast.setFlushedPosition(where);
				mappedFileLast.setWrotePosition(where);
				mappedFileLast.setCommittedPosition(where);
				break;
			}
			else {
				toRemoves.add(mappedFileLast);
			}
		}

		if (!toRemoves.isEmpty()) {
			mappedFiles.removeAll(toRemoves);
		}
		return true;
	}

	public long getMinOffset() {
		if (!mappedFiles.isEmpty()) {
			try {
				return mappedFiles.get(0).getFileFromOffset();
			}
			catch (IndexOutOfBoundsException ignored) {

			}
			catch (Exception e) {
				log.error("getMinOffset has exception.", e);
			}
		}
		return -1;
	}

	public long getMaxOffset() {
		MappedFile mappedFile = getLastMappedFile();
		if (mappedFile != null) {
			return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
		}
		return 0;
	}

	public long getMaxWrotePosition() {
		MappedFile mappedFile = getLastMappedFile();
		if (mappedFile != null) {
			return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
		}
		return 0;
	}

	public long remainHowManyDataToCommit() {
		return getMaxWrotePosition() - getCommittedWhere();
	}

	public long remainHowManyDataToFlush() {
		return getMaxOffset() - getFlushedWhere();
	}

	public void deleteLastMappedFile() {
		MappedFile lastMappedFile = getLastMappedFile();
		if (lastMappedFile != null) {
			lastMappedFile.destroy(1000);
			mappedFiles.remove(lastMappedFile);
			log.info("on recover, destroy a logic mapped file {}", lastMappedFile.getFileName());
		}
	}

	public int deleteExpiredFileByTime(final long expiredTime, final long deleteFilesInterval, final long intervalForcibly,
	                                   final boolean cleanImmediately, final int deleteFileBatchMax) {
		Object[] mfs = copyMappedFiles(0);
		if (mfs == null) {
			return 0;
		}

		int mfsLength = mfs.length - 1;
		int deleteCount = 0;
		List<MappedFile> files = new ArrayList<>();
		int skipFileNum = 0;

		if (mfs != null) {
			checkSelf();

			for (int i = 0; i < mfsLength; i++) {
				MappedFile mappedFile = (MappedFile) mfs[i];
				long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
				if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
					if (skipFileNum > 0) {
						log.info("Delete CommitLog {} but skip {} files", mappedFile.getFileName(), skipFileNum);
					}

					if (mappedFile.destroy(intervalForcibly)) {
						files.add(mappedFile);
						deleteCount++;

						if (files.size() >= deleteFileBatchMax) {
							break;
						}

						if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
							try {
								Thread.sleep(deleteFilesInterval);
							}
							catch (InterruptedException ignored) {}
						}
					}
					else {
						break;
					}
				}
				else {
					skipFileNum++;
					break;
				}
			}
		}

		deleteExpiredFiles(files);

		return deleteCount;
	}

	public int deleteExpiredFileByOffset(long offset, int unitsize) {
		Object[] mfs = copyMappedFiles(0);
		List<MappedFile> files = new ArrayList<>();
		int deleteCount = 0;
		if (mfs != null) {
			int mfsLength = mfs.length - 1;

			for (int i = 0; i < mfsLength; i++) {
				boolean destroy;
				MappedFile mappedFile = (MappedFile) mfs[i];
				SelectMappedBufferResult result = mappedFile.selectMappedBuffer(mappedFileSize - unitsize);
				if (result != null) {
					long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
					result.release();
					destroy = maxOffsetInLogicQueue < offset;
					if (destroy) {
						log.info("physic min offset {}, logics in current mapppedFile max offset {}, delete it",
								offset, maxOffsetInLogicQueue);
					}
				}
				else if (!mappedFile.isAvailable()) {
					log.warn("Found a hanged consume queue file, attempting to delete it.");
					destroy = true;
				}
				else {
					log.warn("this being no executed forever.");
					break;
				}

				if (destroy && mappedFile.destroy(1000 * 60)) {
					files.add(mappedFile);
					deleteCount++;
				}
				else {
					break;
				}
			}
		}

		deleteExpiredFiles(files);

		return deleteCount;
	}

	public int deleteExpiredFileByOffsetForTimerLog(long offset, int checkOffset, int unitSize) {
		Object[] mfs = copyMappedFiles(0);

		List<MappedFile> files = new ArrayList<>();
		int deleteCount = 0;
		if (mfs != null) {
			int mfsLength = mfs.length - 1;

			for (int i = 0; i < mfsLength; i++) {
				boolean destroy = false;
				MappedFile mappedFile = (MappedFile) mfs[i];
				SelectMappedBufferResult result = mappedFile.selectMappedBuffer(checkOffset);

				try {
					int position = result.getByteBuffer().position();
					int size = result.getByteBuffer().getInt();
					result.getByteBuffer().getLong();
					int magic = result.getByteBuffer().getInt();
					if (size == unitSize && (magic | 0XF) == 0XF) {
						result.getByteBuffer().position(position + MixAll.UNIT_PER_SIZE_FOR_MSG);
						long maxOffsetPy = result.getByteBuffer().getLong();
						destroy = maxOffsetPy < offset;

						if (destroy) {
							log.info("physic min commitlog offset {}, current mappedFile's max offset {}, delete it",
									offset, maxOffsetPy);
						}
						else {
							log.warn("Found error data in [{}] checkOffset:{}, unitSize:{}", mappedFile.getFileName(), checkOffset, unitSize);
						}
					}
					else if (!mappedFile.isAvailable()) {
						log.warn("Found a hanged consume queue file, attempting to delete it.");
						destroy = true;
					}
					else {
						log.warn("this being not executed forerver.");
						break;
					}
				}
				finally {
					if (result != null) {
						result.release();
					}
				}

				if (destroy && mappedFile.destroy(60 * 1000)) {
					files.add(mappedFile);
					deleteCount++;
				}
				else {
					break;
				}
			}
		}

		deleteExpiredFiles(files);

		return deleteCount;
	}

	public boolean flush(final int flushLeastPages) {
		boolean result = true;
		MappedFile mappedFile = findMappedFileByOffset(getFlushedWhere(), getFlushedWhere() == 0);
		if (mappedFile != null) {
			long tmpTimestamp = mappedFile.getStoreTimestamp();
			int offset = mappedFile.flush(flushLeastPages);
			long where = mappedFile.getFileFromOffset() + offset;
			result = where == getFlushedWhere();
			setFlushedWhere(where);
			if (flushLeastPages == 0) {
				setStoreTimestamp(tmpTimestamp);
			}
		}
		return result;
	}

	public synchronized boolean commit(final int commitLeastPages) {
		boolean result = true;
		MappedFile mappedFile = findMappedFileByOffset(getCommittedWhere(), getCommittedWhere() == 0);
		if (mappedFile != null) {
			int offset = mappedFile.commit(commitLeastPages);
			long where = mappedFile.getFileFromOffset() + offset;
			result = where == getCommittedWhere();
			setCommittedWhere(where);
		}

		return result;
	}

	public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
		try {
			MappedFile firstMappedFile = getFirstMappedFile();
			MappedFile lastMappedFile = getLastMappedFile();

			if (firstMappedFile != null && lastMappedFile != null) {
				if (offset < firstMappedFile.getFileFromOffset() || offset > lastMappedFile.getFileFromOffset() + mappedFileSize) {
					LOG_ERROR.warn("Offset not matched, Request Offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
							offset, firstMappedFile.getFileFromOffset(), lastMappedFile.getFileFromOffset(), mappedFileSize, mappedFiles.size());
				}
				else {
					int index = (int) ((offset / mappedFileSize) - (firstMappedFile.getFileFromOffset() / mappedFileSize));
					MappedFile targetFile = null;
					try {
						targetFile = mappedFiles.get(index);
					}
					catch (Exception ignored) {}

					if (targetFile != null && offset >= targetFile.getFileFromOffset() && offset < targetFile.getFileFromOffset() + mappedFileSize) {
						return targetFile;
					}

					for (MappedFile tmpMappedFile : mappedFiles) {
						if (offset > tmpMappedFile.getFileFromOffset() && offset < tmpMappedFile.getFileFromOffset() + mappedFileSize) {
							return tmpMappedFile;
						}
					}
				}

				if (returnFirstOnNotFound) {
					return firstMappedFile;
				}
			}
		}
		catch (Exception e) {
			log.error("findMappedFileByOffset Exception", e);
		}

		return null;
	}

	public MappedFile getFirstMappedFile() {
		MappedFile firstMappedFile = null;

		if (!mappedFiles.isEmpty()) {
			try {
				firstMappedFile = mappedFiles.get(0);
			}
			catch (IndexOutOfBoundsException ignored) {}
			catch (Exception e) {
				log.error("getFirstMappedFile has exception.", e);
			}
		}
		return firstMappedFile;
	}

	public MappedFile findMappedFileByOffset(final long offset) {
		return findMappedFileByOffset(offset, false);
	}

	public long getMappedMemorySize() {
		long size = 0;

		Object[] mfs = copyMappedFiles(0);
		if (mfs != null) {
			for (Object mf : mfs) {
				if (((ReferenceResource) mf).isAvailable()) {
					size += mappedFileSize;
				}
			}
		}
		return size;
	}

	public boolean retryDeleteFirstFile(final long intervalForcibly) {
		MappedFile mappedFile = getFirstMappedFile();
		if (mappedFile != null) {
			if (!mappedFile.isAvailable()) {
				log.warn("the mappedFile was destroyed once, but still alive. {}", mappedFile.getFileName());
				boolean result = mappedFile.destroy(intervalForcibly);
				if (result) {
					log.info("the mappedFile re delete OK, {}", mappedFile.getFileName());
					deleteExpiredFiles(Collections.singletonList(mappedFile));
				}
				else {
					log.warn("the mappedFile re delete failed, {}", mappedFile.getFileName());
				}

				return result;
			}
		}

		return false;
	}

	public void shutdown(final int intervalForcibly) {
		for (MappedFile mf : mappedFiles) {
			mf.shutdown(intervalForcibly);
		}
	}

	public void destroy() {
		for (MappedFile mf : mappedFiles) {
			mf.destroy(3 * 1000);
		}

		mappedFiles.clear();
		setFlushedWhere(0);

		File file = new File(storePath);
		if (file.isDirectory()) {
			file.delete();
		}
	}

	@Override
	public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
		if (mappedFiles.isEmpty()) {
			return;
		}

		reserveNum = Math.min(reserveNum, 3);

		Object[] mfs = copyMappedFiles(0);
		if (mfs == null) {
			return;
		}

		for (int i = mfs.length - 1; i >= 0; i--) {
			MappedFile mappedFile = (MappedFile) mfs[i];
			if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceSwapIntervalMs) {
				mappedFile.swapMap();
				continue;
			}

			if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > normalSwapIntervalMs && mappedFile.getMappedBytesBufferAccessCountSinceLastSwap() > 0) {
				mappedFile.swapMap();
				continue;
			}
		}

	}

	@Override
	public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
		if (mappedFiles.isEmpty()) {
			return;
		}

		int reserveNum = 3;
		Object[] mfs = copyMappedFiles(0);
		if (mfs == null) {
			return;
		}

		for (int i = mfs.length - 1 - reserveNum; i >= 0; i--) {
			MappedFile mappedFile = (MappedFile) mfs[i];
			if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceCleanSwapIntervalMs) {
				mappedFile.cleanSwapedMap(false);
			}
		}
	}

	public Object[] snapshot() {
		return mappedFiles.toArray();
	}

	public Stream<MappedFile> stream() {
		return mappedFiles.stream();
	}

	public Stream<MappedFile> reversedStream() {
		return Lists.reverse(mappedFiles).stream();
	}

	public long getTotalFileSize() {
		return mappedFileSize * mappedFiles.size();
	}

	public List<MappedFile> range(final long from, final long to) {
		Object[] mfs = copyMappedFiles(0);
		if (mfs == null) {
			return new ArrayList<>();
		}

		List<MappedFile> result = new ArrayList<>();
		for (Object mf : mfs) {
			MappedFile mappedFile = (MappedFile) mf;
			if (mappedFile.getFileFromOffset() + mappedFile.getFileSize() <= from) {
				continue;
			}

			if (to <= mappedFile.getFileFromOffset()) {
				break;
			}
			result.add(mappedFile);
		}

		return result;
	}
}
