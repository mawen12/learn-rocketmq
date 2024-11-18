package com.mawen.learn.rocketmq.store.ha.autoswitch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.mawen.learn.rocketmq.common.utils.CheckpointFile;
import com.mawen.learn.rocketmq.remoting.protocol.EpochEntry;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/18
 */
public class EpochFileCache {
	private static final Logger log = LoggerFactory.getLogger(EpochFileCache.class);

	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock readLock = readWriteLock.readLock();
	private final Lock writeLock = readWriteLock.writeLock();
	private final TreeMap<Integer, EpochEntry> epochMap;
	private CheckpointFile<EpochEntry> checkpoint;

	public EpochFileCache() {
		this.epochMap = new TreeMap<>();
	}

	public EpochFileCache(final String path) {
		this.epochMap = new TreeMap<>();
		this.checkpoint = new CheckpointFile<>(path, new EpochEntrySerializer());
	}

	public boolean initCacheFromFile() {
		writeLock.lock();
		try {
			List<EpochEntry> entries = checkpoint.read();
			initEntries(entries);
			return true;
		}
		catch (IOException e) {
			log.error("Error happen when init epoch entries from epoch file", e);
			return false;
		}
		finally {
			writeLock.unlock();
		}
	}


	public void initCacheFromEntries(List<EpochEntry> entries) {
		writeLock.lock();
		try {
			initEntries(entries);
			flush();
		}
		finally {
			writeLock.unlock();
		}
	}

	private void initEntries(List<EpochEntry> entries) {
		epochMap.clear();
		EpochEntry preEntry = null;
		for (EpochEntry entry : entries) {
			epochMap.put(entry.getEpoch(), entry);
			if (preEntry != null) {
				preEntry.setEndOffset(entry.getStartOffset());
			}
			preEntry = entry;
		}
	}

	public int getEntrySize() {
		readLock.lock();
		try {
			return epochMap.size();
		}
		finally {
			readLock.unlock();
		}
	}

	public boolean appendEntry(final EpochEntry entry) {
		writeLock.lock();
		try {
			if (!epochMap.isEmpty()) {
				EpochEntry lastEntry = epochMap.lastEntry().getValue();
				if (lastEntry.getEpoch() >= entry.getEpoch() || lastEntry.getStartOffset() >= entry.getStartOffset()) {
					log.error("The appending entry's lastEpoch or endOffset {} is not bigger that lastEntry {}, append failed", entry, lastEntry);
					return false;
				}
				lastEntry.setEndOffset(entry.getStartOffset());
			}
			epochMap.put(entry.getEpoch(), new EpochEntry(entry));
			flush();
			return true;
		}
		finally {
			writeLock.unlock();
		}
	}

	public void setLastEpochEntryEndOffset(final long endOffset) {
		writeLock.lock();
		try {
			if (!epochMap.isEmpty()) {
				EpochEntry lastEntry = epochMap.lastEntry().getValue();
				if (lastEntry.getStartOffset() <= endOffset) {
					lastEntry.setEndOffset(endOffset);
				}
			}
		}
		finally {
			writeLock.unlock();
		}
	}

	public EpochEntry firstEntry() {
		readLock.lock();
		try {
			if (epochMap.isEmpty()) {
				return null;
			}
			return new EpochEntry(epochMap.firstEntry().getValue());
		}
		finally {
			readLock.unlock();
		}
	}

	public EpochEntry lastEntry() {
		writeLock.lock();
		try {
			if (epochMap.isEmpty()) {
				return null;
			}
			return new EpochEntry(epochMap.lastEntry().getValue());
		}
		finally {
			writeLock.unlock();
		}
	}

	public int lastEpoch() {
		EpochEntry entry = lastEntry();
		return entry != null ? entry.getEpoch() : -1;
	}

	public EpochEntry getEntry(final int epoch) {
		readLock.lock();
		try {
			if (epochMap.containsKey(epoch)) {
				EpochEntry entry = epochMap.get(epoch);
				return new EpochEntry(entry);
			}
			return null;
		}
		finally {
			readLock.unlock();
		}
	}

	public EpochEntry findEpochEntryByOffset(final long offset) {
		readLock.lock();
		try {
			if (!epochMap.isEmpty()) {
				for (Map.Entry<Integer, EpochEntry> entry : epochMap.entrySet()) {
					if (entry.getValue().getStartOffset() <= offset && entry.getValue().getEndOffset() > offset) {
						return new EpochEntry(entry.getValue());
					}
				}
			}
			return null;
		}
		finally {
			readLock.unlock();
		}
	}

	public EpochEntry nextEntry(final int epoch) {
		readLock.lock();
		try {
			Map.Entry<Integer, EpochEntry> entry = epochMap.ceilingEntry(epoch + 1);
			return entry != null ? new EpochEntry(entry.getValue()) : null;
		}
		finally {
			readLock.unlock();
		}
	}

	public List<EpochEntry> getAllEntries() {
		readLock.lock();
		try {
			return epochMap.values().stream().map(EpochEntry::new).collect(Collectors.toList());
		}
		finally {
			readLock.unlock();
		}
	}

	public long findConsistentPoint(final EpochFileCache compareCache) {
		readLock.lock();
		try {
			long consistentOffset = -1;
			Map<Integer, EpochEntry> descendingMap = new TreeMap<>(epochMap).descendingMap();
			Iterator<Map.Entry<Integer, EpochEntry>> iterator = descendingMap.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<Integer, EpochEntry> curLocalEntry = iterator.next();
				EpochEntry compareEntry = compareCache.getEntry(curLocalEntry.getKey());
				if (compareEntry != null && compareEntry.getStartOffset() == curLocalEntry.getValue().getStartOffset()) {
					consistentOffset = Math.min(curLocalEntry.getValue().getEndOffset(), compareEntry.getEndOffset());
					break;
				}
			}
			return consistentOffset;
		}
		finally {
			readLock.unlock();
		}
	}

	public void truncateSuffixByEpoch(final int truncateEpoch) {
		Predicate<EpochEntry> predict = entry -> entry.getEpoch() >= truncateEpoch;
		doTruncateSuffix(predict);
	}

	public void truncateSuffixByOffset(final long truncateOffset) {
		Predicate<EpochEntry> predict = entry -> entry.getStartOffset() >= truncateOffset;
		doTruncateSuffix(predict);
	}

	private void doTruncateSuffix(Predicate<EpochEntry> predict) {
		writeLock.lock();
		try {
			epochMap.entrySet().removeIf(entry -> predict.test(entry.getValue()));
			EpochEntry entry = lastEntry();
			if (entry != null) {
				entry.setEndOffset(Long.MAX_VALUE);
			}
			flush();
		}
		finally {
			writeLock.unlock();
		}
	}

	public void truncatePrefixByOffset(final long truncateOffset) {
		Predicate<EpochEntry> predict = entry -> entry.getEndOffset() <= truncateOffset;
		 writeLock.lock();
		 try {
			 epochMap.entrySet().removeIf(entry -> predict.test(entry.getValue()));
			 flush();
		 }
		 finally {
			 writeLock.unlock();
		 }
	}

	private void flush() {
		writeLock.lock();
		try {
			if (checkpoint != null) {
				List<EpochEntry> entries = new ArrayList<>(epochMap.values());
				checkpoint.write(entries);
			}
		}
		catch (IOException e) {
			log.error("Error happen when flush epochEntries to epochCheckpointFile", e);
		}
		finally {
			writeLock.unlock();
		}
	}

	static class EpochEntrySerializer implements CheckpointFile.CheckpointSerializer<EpochEntry> {

		@Override
		public String toLine(EpochEntry entry) {
			if (entry == null) {
				return null;
			}
			return String.format("%d-%d", entry.getEpoch(), entry.getStartOffset());
		}

		@Override
		public EpochEntry fromLine(String line) {
			String[] arr = line.split("-");
			if (arr.length == 2) {
				int epoch = Integer.parseInt(arr[0]);
				long startOffset = Long.parseLong(arr[1]);
				return new EpochEntry(epoch, startOffset);
			}
			return null;
		}
	}
}
