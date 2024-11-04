package com.mawen.learn.rocketmq.store.logfile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import com.mawen.learn.rocketmq.common.message.MessageExtBatch;
import com.mawen.learn.rocketmq.common.message.MessageExtBrokerInner;
import com.mawen.learn.rocketmq.store.AppendMessageCallback;
import com.mawen.learn.rocketmq.store.AppendMessageResult;
import com.mawen.learn.rocketmq.store.CompactionAppendMsgCallback;
import com.mawen.learn.rocketmq.store.PutMessageContext;
import com.mawen.learn.rocketmq.store.SelectMappedBufferResult;
import com.mawen.learn.rocketmq.store.TransientStorePool;
import com.mawen.learn.rocketmq.store.config.FlushDiskType;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/4
 */
public interface MappedFile {

	String getFileName();

	boolean renameTo(String fileName);

	int getFileSize();

	FileChannel getFileChannel();

	boolean isFull();

	boolean isAvailable();

	AppendMessageResult appendMessage(MessageExtBrokerInner message, AppendMessageCallback messageCallback, PutMessageContext context);

	AppendMessageResult appendMessage(MessageExtBatch message, AppendMessageCallback messageCallback, PutMessageContext context);

	AppendMessageResult appendMessage(final ByteBuffer buffer, final CompactionAppendMsgCallback cb);

	boolean appendMessage(byte[] data);

	boolean appendMessage(ByteBuffer data);

	boolean appendMessage(byte[] data, int offset, int length);

	long getFileFromOffset();

	int flush(int flushLeastPages);

	int commit(int commitLeastPages);

	SelectMappedBufferResult selectMappedBuffer(int pos, int size);

	SelectMappedBufferResult selectMappedBuffer(int pos);

	MappedByteBuffer getMappedByteBuffer();

	ByteBuffer sliceByteBuffer();

	long getStoreTimestamp();

	long getLastModifiedTimestamp();

	boolean getData(int pos, int size, ByteBuffer buffer);

	boolean destroy(long intervalForcibly);

	void shutdown(long intervalForcibly);

	void release();

	boolean hold();

	boolean isFirstCreateInQueue();

	void setFirstCreateInQueue(boolean firstCreateInQueue);

	int getFlushedPosition();

	void setFlushedPosition(int flushedPosition);

	int getWrotePosition();

	void setWrotePosition(int wrotePosition);

	int getReadPosition();

	void setCommittedPosition(int committedPosition);

	void mlock();

	void munlock();

	void warmMappedFile(FlushDiskType type, int pages);

	boolean swapMap();

	void cleanSwapedMap(boolean force);

	long getRecentSwapMapTime();

	long getMappedBytesBufferAccessCountSinceLastSwap();

	File getFile();

	void renameToDelete();

	void moveToParent() throws IOException;

	long getLastFlushTime();

	void init(String fileName, int fileSize, TransientStorePool transientStorePool) throws IOException;

	boolean isLoaded(long position, int size);
}
