package com.mawen.learn.rocketmq.store.logfile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.mawen.learn.rocketmq.common.UtilAll;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.message.MessageExt;
import com.mawen.learn.rocketmq.common.message.MessageExtBatch;
import com.mawen.learn.rocketmq.common.message.MessageExtBrokerInner;
import com.mawen.learn.rocketmq.common.utils.NetworkUtil;
import com.mawen.learn.rocketmq.store.AppendMessageCallback;
import com.mawen.learn.rocketmq.store.AppendMessageResult;
import com.mawen.learn.rocketmq.store.AppendMessageStatus;
import com.mawen.learn.rocketmq.store.CompactionAppendMsgCallback;
import com.mawen.learn.rocketmq.store.PutMessageContext;
import com.mawen.learn.rocketmq.store.SelectMappedBufferResult;
import com.mawen.learn.rocketmq.store.TransientStorePool;
import com.mawen.learn.rocketmq.store.config.FlushDiskType;
import com.mawen.learn.rocketmq.store.util.LibC;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import lombok.Setter;
import org.apache.commons.lang3.SystemUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
@Setter
public class DefaultMappedFile extends AbstractMappedFile {

	public static final int OS_PAGE_SIZE = 4 * 1024;
	public static final Unsafe UNSAFE = getUnsafe();
	public static final int UNSAFE_PAGE_SIZE = UNSAFE == null ? OS_PAGE_SIZE : UNSAFE.pageSize();

	private static final Method IS_LOADED_METHOD;

	protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	protected static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

	protected static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

	protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;
	protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> COMMITTED_POSITION_UPDATER;
	protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> FLUSHED_POSITION_UPDATER;

	static {
		WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
		COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "committedPosition");
		FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "flushedPosition");

		Method isLoaded0method = null;
		if (!SystemUtils.IS_OS_WINDOWS) {
			try {
				isLoaded0method = MappedByteBuffer.class.getDeclaredMethod("isLoaded0", long.class, long.class, int.class);
				isLoaded0method.setAccessible(true);
			}
			catch (NoSuchMethodException ignored) {}
		}
		IS_LOADED_METHOD = isLoaded0method;
	}

	public static int getTotalMappedFiles() {
		return TOTAL_MAPPED_FILES.get();
	}

	public static long getTotalMappedVirtualMemory() {
		return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
	}

	protected volatile int wrotePosition;
	protected volatile int committedPosition;
	protected volatile int flushedPosition;
	protected int fileSize;
	protected FileChannel fileChannel;

	protected ByteBuffer writeBuffer;
	protected TransientStorePool transientStorePool;
	protected String fileName;
	protected long fileFromOffset;
	protected File file;
	protected MappedByteBuffer mappedByteBuffer;
	protected volatile long storeTimestamp = 0;
	protected boolean firstCreateInQueue = false;
	private long lastFlushTime = -1L;

	protected MappedByteBuffer mappedByteBufferWaitToClean;
	protected long swapMapTime = 0L;
	protected long mappedByteBufferAccessCountSinceLastSwap = 0L;

	private long startTimestamp = -1;

	private long stopTimestamp = -1;

	public DefaultMappedFile() {
	}

	public DefaultMappedFile(final String fileName, final int fileSize) throws IOException {
		init(fileName, fileSize);
	}

	public DefaultMappedFile(final String fileName, final int fileSize, final TransientStorePool transientStorePool) throws IOException {
		init(fileName, fileSize, transientStorePool);
	}

	@Override
	public void init(String fileName, int fileSize, TransientStorePool transientStorePool) throws IOException {
		init(fileName, fileSize);
		this.writeBuffer = transientStorePool.borrowBuffer();
		this.transientStorePool = transientStorePool;
	}

	private void init(final String fileName, final int fileSize) throws IOException {
		this.fileName = fileName;
		this.fileSize = fileSize;
		this.file = new File(fileName);
		this.fileFromOffset = Long.parseLong(file.getName());

		boolean ok = false;
		UtilAll.ensureDirOK(file.getParent());

		try {
			this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
			this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);

			TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
			TOTAL_MAPPED_FILES.incrementAndGet();

			ok = true;
		}
		catch (FileNotFoundException e) {
			log.error("Failed to create file {}", fileName, e);
			throw e;
		}
		catch (IOException e) {
			log.error("Failed to map file {}", fileName, e);
			throw e;
		}
		finally {
			if (!ok && fileChannel != null) {
				fileChannel.close();
			}
		}
	}

	@Override
	public boolean renameTo(String fileName) {
		File newFile = new File(fileName);
		boolean rename = file.renameTo(newFile);
		if (rename) {
			this.fileName = fileName;
			this.file = newFile;
		}

		return rename;
	}

	@Override
	public long getLastModifiedTimestamp() {
		return file.lastModified();
	}

	@Override
	public boolean cleanup(long currentRef) {
		return false;
	}

	@Override
	public String getFileName() {
		return "";
	}

	@Override
	public int getFileSize() {
		return fileSize;
	}

	@Override
	public FileChannel getFileChannel() {
		return fileChannel;
	}

	@Override
	public boolean isFull() {
		return false;
	}

	@Override
	public AppendMessageResult appendMessage(MessageExtBrokerInner message, AppendMessageCallback messageCallback, PutMessageContext context) {
		return appendMessagesInner(message, messageCallback, context);
	}

	@Override
	public AppendMessageResult appendMessage(MessageExtBatch message, AppendMessageCallback messageCallback, PutMessageContext context) {
		return appendMessagesInner(message, messageCallback, context);
	}

	public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback callback, final PutMessageContext context) {
		assert messageExt != null;
		assert callback != null;

		int currentPos = WROTE_POSITION_UPDATER.get(this);

		if (currentPos < fileSize) {
			ByteBuffer buffer = appendMessageBuffer().slice();
			buffer.position(currentPos);
			AppendMessageResult result;
			if (messageExt instanceof MessageExtBatch && !((MessageExtBatch) messageExt).isInnerBatch()) {
				result = callback.doAppend(getFileFromOffset(), buffer, fileSize - currentPos, (MessageExtBatch) messageExt, context);
			}
			else if (messageExt instanceof MessageExtBrokerInner) {
				result = callback.doAppend(getFileFromOffset(), buffer, fileSize - currentPos, (MessageExtBrokerInner) messageExt, context);
			}
			else {
				return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
			}

			WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
			storeTimestamp = result.getStoreTimestamp();
			return result;
		}

		log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, fileSize);
		return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
	}

	@Override
	public AppendMessageResult appendMessage(ByteBuffer bufferMsg, CompactionAppendMsgCallback cb) {
		assert bufferMsg != null;
		assert cb != null;

		int currentPos = WROTE_POSITION_UPDATER.get(this);
		if (currentPos < fileSize) {
			ByteBuffer buffer = appendMessageBuffer().slice();
			AppendMessageResult result = cb.doAppend(buffer, fileFromOffset, fileSize - currentPos, bufferMsg);
			WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
			storeTimestamp = result.getStoreTimestamp();
			return result;
		}
		log.error("MappedFile.appendMessage return null, wrotePosition: {}, fileSize: {}", currentPos, fileSize);
		return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
	}

	protected ByteBuffer appendMessageBuffer() {
		mappedByteBufferAccessCountSinceLastSwap++;
		return writeBuffer != null ? writeBuffer : mappedByteBuffer;
	}

	@Override
	public boolean appendMessage(byte[] data) {
		return appendMessage(data, 0, data.length);
	}

	@Override
	public boolean appendMessage(ByteBuffer data) {
		int currentPos = WROTE_POSITION_UPDATER.get(this);
		int remaining = data.remaining();

		if (currentPos + remaining <= fileSize) {
			try {
				fileChannel.position(currentPos);
				while (data.hasRemaining()) {
					fileChannel.write(data);
				}
			}
			catch (Throwable e) {
				log.error("Error occurred when append message to mappedFile.", e);
			}
			WROTE_POSITION_UPDATER.addAndGet(this, remaining);
			return true;
		}
		return false;
	}

	@Override
	public boolean appendMessage(byte[] data, int offset, int length) {
		int currentPos = WROTE_POSITION_UPDATER.get(this);

		if (currentPos + length <= fileSize) {
			try {
				MappedByteBuffer buf = mappedByteBuffer.slice();
				buf.position(currentPos);
				buf.put(data, offset, length);
			}
			catch (Throwable e) {
				log.error("Error occurred when append message to mappedFile.", e);
			}
			WROTE_POSITION_UPDATER.addAndGet(this, length);
			return true;
		}
		return false;
	}

	@Override
	public long getFileFromOffset() {
		return fileFromOffset;
	}

	@Override
	public int flush(int flushLeastPages) {
		if (isAbleToFlush(flushLeastPages)) {
			if (hold()) {
				int value = getReadPosition();

				try {
					mappedByteBufferAccessCountSinceLastSwap++;

					if (writeBuffer != null && fileChannel.position() != 0) {
						fileChannel.force(false);
					}
					else {
						mappedByteBuffer.force();
					}
					lastFlushTime = System.currentTimeMillis();
				}
				catch (Throwable e) {
					log.error("Error occurred when force data to disk.", e);
				}

				FLUSHED_POSITION_UPDATER.set(this, value);
				release();
			}
			else {
				log.warn("in flush, hold failed, flush offset = {}", FLUSHED_POSITION_UPDATER.get(this));
				FLUSHED_POSITION_UPDATER.set(this, getReadPosition());
			}
		}
		return getFlushedPosition();
	}

	@Override
	public int commit(int commitLeastPages) {
		if (writeBuffer == null) {
			return WROTE_POSITION_UPDATER.get(this);
		}

		if (transientStorePool != null && !transientStorePool.isRealCommit()) {
			COMMITTED_POSITION_UPDATER.set(this, WROTE_POSITION_UPDATER.get(this));
		}
		else if (isAbleToCommit(commitLeastPages)) {
			if (hold()) {
				commit0();
				release();
			}
			else {
				log.warn("in commit, hold failed, commit offset = {}", COMMITTED_POSITION_UPDATER.get(this));
			}
		}

		if (writeBuffer != null && transientStorePool != null && fileSize == COMMITTED_POSITION_UPDATER.get(this)) {
			transientStorePool.returnBuffer(writeBuffer);
			writeBuffer = null;
		}

		return COMMITTED_POSITION_UPDATER.get(this);
	}

	protected void commit0() {
		int writePos = WROTE_POSITION_UPDATER.get(this);
		int lastCommittedPosition = COMMITTED_POSITION_UPDATER.get(this);

		if (writePos - lastCommittedPosition > 0) {
			try {
				ByteBuffer buffer = writeBuffer.slice();
				buffer.position(lastCommittedPosition);
				buffer.limit(writePos);

				fileChannel.position(lastCommittedPosition);
				fileChannel.write(buffer);
				COMMITTED_POSITION_UPDATER.set(this, writePos);
			}
			catch (Throwable e) {
				log.error("Error occurred when commit data to FileChannel.", e);
			}
		}
	}

	private boolean isAbleToFlush(final int flushLeastPages) {
		int flush = FLUSHED_POSITION_UPDATER.get(this);
		int write = getReadPosition();

		if (isFull()) {
			return true;
		}

		if (flushLeastPages > 0) {
			return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
		}

		return write > flush;
	}

	protected boolean isAbleToCommit(final int commitLeastPages) {
		int commit = COMMITTED_POSITION_UPDATER.get(this);
		int write = WROTE_POSITION_UPDATER.get(this);

		if (isFull()) {
			return true;
		}

		if (commitLeastPages > 0) {
			return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
		}

		return write > commit;
	}

	@Override
	public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
		int readPosition = getReadPosition();
		if ((pos + size) <= readPosition) {
			if (hold()) {
				mappedByteBufferAccessCountSinceLastSwap++;

				MappedByteBuffer buffer = mappedByteBuffer.slice();
				buffer.position(pos);
				MappedByteBuffer bufferNew = buffer.slice();
				bufferNew.limit(size);

				return new SelectMappedBufferResult(fileFromOffset + pos, bufferNew, size, this);
			}
			else {
				log.warn("matched, but hold failed, request pos: {}, fileFromOffset: {}", pos, fileFromOffset);
			}
		}
		else {
			log.warn("selectMappedBuffer request pos invalid, request post: {}, size: {}, fileFromOffset: {}", pos, size, fileFromOffset);
		}
		return null;
	}

	@Override
	public SelectMappedBufferResult selectMappedBuffer(int pos) {
		int readPosition = getReadPosition();
		if (pos < readPosition && pos >= 0) {
			if (hold()) {
				mappedByteBufferAccessCountSinceLastSwap++;
				MappedByteBuffer buffer = mappedByteBuffer.slice();
				buffer.position(pos);

				int size = readPosition - pos;
				MappedByteBuffer bufferNew = buffer.slice();
				bufferNew.limit(size);

				return new SelectMappedBufferResult(fileFromOffset + pos, bufferNew, size, this);
			}
		}
		return null;
	}

	@Override
	public MappedByteBuffer getMappedByteBuffer() {
		mappedByteBufferAccessCountSinceLastSwap++;
		return mappedByteBuffer;
	}

	@Override
	public ByteBuffer sliceByteBuffer() {
		mappedByteBufferAccessCountSinceLastSwap++;
		return mappedByteBuffer.slice();
	}

	@Override
	public long getStoreTimestamp() {
		return storeTimestamp;
	}

	@Override
	public boolean getData(int pos, int size, ByteBuffer buffer) {
		if (buffer.remaining() < size) {
			return false;
		}

		int readPosition = getReadPosition();
		if (pos + size <= readPosition) {
			if (hold()) {
				try {
					int readNum = fileChannel.read(buffer, pos);
					return readNum == size;
				}
				catch (Throwable t) {
					log.warn("Get data failed post: {} size: {} fileFromOffset: {}", pos, size, fileFromOffset);
					return false;
				}
				finally {
					release();
				}
			}
			else {
				log.debug("matched, but hold failed, request pos: {}, fileFromOffset: {}",
						pos, fileFromOffset);
			}
		}
		else {
			log.warn("selectMappedBuffer request pos invalid, request pos: {}, size: {}, fileFromOffset: {}",
					pos, size, fileFromOffset);
		}
		return false;
	}

	@Override
	public boolean destroy(long intervalForcibly) {
		shutdown(intervalForcibly);

		if (isCleanupOver()) {
			try {
				long lastModifier = getLastModifiedTimestamp();
				fileChannel.close();
				log.info("close file channel {} OK.", fileName);

				long begin = System.currentTimeMillis();
				boolean result = file.delete();
				log.info("delete file[REF:{}] {} {} W: {} M: {}, {}, {}",
						getRefCount(), fileName, (result ? "OK" : "Failed"), getWrotePosition(), getFlushedPosition(),
						UtilAll.computeElapsedTimeMilliseconds(begin), (System.currentTimeMillis() - lastModifier));
			}
			catch (Exception e) {
				log.warn("close file channel {} Failed.", fileName, e);
			}
			return true;
		}
		log.warn("destroy mapped file[REF:{}] {} Failed. cleanupOver", getRefCount(), fileName, cleanupOver);
		return false;
	}

	@Override
	public boolean isFirstCreateInQueue() {
		return firstCreateInQueue;
	}

	@Override
	public void setFirstCreateInQueue(boolean firstCreateInQueue) {
		this.firstCreateInQueue = firstCreateInQueue;
	}

	@Override
	public int getFlushedPosition() {
		return FLUSHED_POSITION_UPDATER.get(this);
	}

	@Override
	public void setFlushedPosition(int flushedPosition) {
		FLUSHED_POSITION_UPDATER.set(this, flushedPosition);
	}

	@Override
	public int getWrotePosition() {
		return WROTE_POSITION_UPDATER.get(this);
	}

	@Override
	public void setWrotePosition(int wrotePosition) {
		WROTE_POSITION_UPDATER.set(this, wrotePosition);
	}

	@Override
	public int getReadPosition() {
		return transientStorePool == null || !transientStorePool.isRealCommit() ? WROTE_POSITION_UPDATER.get(this) : COMMITTED_POSITION_UPDATER.get(this);
	}

	@Override
	public void setCommittedPosition(int committedPosition) {
		COMMITTED_POSITION_UPDATER.set(this, committedPosition);
	}

	@Override
	public void mlock() {
		long begin = System.currentTimeMillis();
		long address = ((DirectBuffer) (mappedByteBuffer)).address();
		Pointer pointer = new Pointer(address);

		int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));
		log.info("mlock {} {} {} ret = {} time consuming = {}", address, fileName, fileSize, ret, System.currentTimeMillis() - begin);

		ret = LibC.INSTANCE.madvise(pointer, new NativeLong(fileSize), LibC.MADV_DONTNEED);
		log.info("madvise {} {} {} ret = {} time consuming = {}", address, fileName, fileSize, ret, System.currentTimeMillis() - begin);
	}

	@Override
	public void munlock() {
		long begin = System.currentTimeMillis();
		long address = ((DirectBuffer) (mappedByteBuffer)).address();
		Pointer pointer = new Pointer(address);
		int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
		log.info("munlock {} {} {} ret = {} time consuming = {}", address, fileName, fileSize, ret, System.currentTimeMillis() - begin);
	}

	@Override
	public void warmMappedFile(FlushDiskType type, int pages) {
		mappedByteBufferAccessCountSinceLastSwap++;

		long begin = System.currentTimeMillis();
		MappedByteBuffer buffer = mappedByteBuffer.slice();
		long flush = 0;

		for (long i = 0, j = 0; i < fileSize; i += OS_PAGE_SIZE, j++) {
			buffer.put((int) i, (byte) 0);
			if (type == FlushDiskType.SYNC_FLUSH) {
				if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
					flush = i;
					mappedByteBuffer.force();
				}
			}
		}

		if (type == FlushDiskType.SYNC_FLUSH) {
			log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}", getFileName(), System.currentTimeMillis() - begin);
			mappedByteBuffer.force();
		}
		log.info("mapped file warm-up done. mappedFile={}, costTime={}", getFileName(), System.currentTimeMillis() - begin);

		mlock();
	}

	@Override
	public boolean swapMap() {
		if (getRefCount() == 1 && mappedByteBufferWaitToClean == null) {
			if (!hold()) {
				log.warn("in swapMap, hold failed, fileName: {}", fileName);
				return false;
			}

			try {
				mappedByteBufferWaitToClean = mappedByteBuffer;
				mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
				mappedByteBufferAccessCountSinceLastSwap = 0L;
				swapMapTime = System.currentTimeMillis();
				log.info("swap file {} success.", fileName);
				return true;
			}
			catch (Exception e) {
				log.error("swapMap file {} Failed.", fileName, e);
			}
			finally {
				release();
			}
		}
		else {
			log.info("Will not swap file {}, ref = {}", fileName, getRefCount());
		}
		return false;
	}

	@Override
	public void cleanSwapedMap(boolean force) {
		try {
			if (mappedByteBufferWaitToClean == null) {
				return;
			}

			long minGapTime = 120 * 1000L;
			long gapTime = System.currentTimeMillis() - swapMapTime;
			if (!force && gapTime < minGapTime) {
				Thread.sleep(minGapTime - gapTime);
			}

			UtilAll.cleanBuffer(mappedByteBufferWaitToClean);
			mappedByteBufferWaitToClean = null;
			log.info("cleanSwapedMap file {} success.", fileName);
		}
		catch (Exception e) {
			log.error("cleanSwapedMap file {} Failed.", fileName, e);
		}
	}

	@Override
	public long getRecentSwapMapTime() {
		return 0;
	}

	@Override
	public long getMappedBytesBufferAccessCountSinceLastSwap() {
		return mappedByteBufferAccessCountSinceLastSwap;
	}

	@Override
	public File getFile() {
		return file;
	}

	@Override
	public void renameToDelete() {
		if (!fileName.endsWith(".delete")) {
			String newFileName = fileName + ".delete";

			try {
				Path newFilePath = Paths.get(newFileName);
				if (NetworkUtil.isWindowsPlatform() && mappedByteBuffer == null) {
					long position = fileChannel.position();
					UtilAll.cleanBuffer(mappedByteBuffer);
					fileChannel.close();

					Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
					try (RandomAccessFile file = new RandomAccessFile(newFileName, "rw")) {
						fileChannel = file.getChannel();
						fileChannel.position(position);
						mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
					}
				}
				else {
					Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
				}
				fileName = newFileName;
				file = new File(newFileName);
			}
			catch (IOException e) {
				log.error("move file {} failed", fileName, e);
			}
		}
	}

	@Override
	public void moveToParent() throws IOException {
		Path currentPath = Paths.get(fileName);
		String baseName = currentPath.getFileName().toString();
		Path parentPath = currentPath.getParent().getParent().resolve(baseName);

		if (NetworkUtil.isWindowsPlatform() && mappedByteBuffer != null) {
			long position = fileChannel.position();
			UtilAll.cleanBuffer(mappedByteBuffer);
			fileChannel.close();
			Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);

			try (RandomAccessFile file = new RandomAccessFile(parentPath.toFile(), "rw")) {
				fileChannel = file.getChannel();
				fileChannel.position(position);
				mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
			}
		}
		else {
			Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);
		}
		file = parentPath.toFile();
		fileName = parentPath.toString();
	}

	@Override
	public long getLastFlushTime() {
		return lastFlushTime;
	}

	@Override
	public boolean isLoaded(long position, int size) {
		if (IS_LOADED_METHOD == null) {
			return true;
		}

		try {
			long addr = ((DirectBuffer)mappedByteBuffer).address() + position;
			return (boolean) IS_LOADED_METHOD.invoke(mappedByteBuffer, mappingAddr(addr), size, pageCount(size));
		}
		catch (Exception e) {
			log.info("invoke isLoaded0 of file {} error: {}", file.getAbsolutePath(), e);
		}
		return true;
	}

	public static long mappingAddr(long addr) {
		long offset = addr % UNSAFE_PAGE_SIZE;
		offset = (offset >= 0) ? offset : UNSAFE_PAGE_SIZE + offset;
		return addr - offset;
	}

	public static int pageCount(long size) {
		return (int) (size + (long) UNSAFE_PAGE_SIZE - 1L) / UNSAFE_PAGE_SIZE;
	}

	public static Unsafe getUnsafe() {
		try {
			Field f = Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			return (Unsafe) f.get(null);
		}
		catch (Exception ignored) {}
		return null;
	}

	private class Itr implements Iterator<SelectMappedBufferResult> {
		private int start;
		private int current;
		private ByteBuffer buffer;

		public Itr (int pos) {
			this.start = pos;
			this.current = pos;
			this.buffer = mappedByteBuffer.slice();
			this.buffer.position(start);
		}

		@Override
		public boolean hasNext() {
			return current < getReadPosition();
		}

		@Override
		public SelectMappedBufferResult next() {
			int readPosition = getReadPosition();
			if (current < readPosition && current >= 0) {
				if (hold()) {
					ByteBuffer byteBuffer = buffer.slice();
					byteBuffer.position(current);
					int size = byteBuffer.getInt(current);

					ByteBuffer bufferResult = byteBuffer.slice();
					bufferResult.limit(size);
					current += size;

					return new SelectMappedBufferResult(fileFromOffset + current, bufferResult, size, DefaultMappedFile.this);
				}
			}
			return null;
		}

		@Override
		public void forEachRemaining(Consumer<? super SelectMappedBufferResult> action) {
			Iterator.super.forEachRemaining(action);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}