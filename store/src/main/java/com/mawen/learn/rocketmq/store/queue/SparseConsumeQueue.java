package com.mawen.learn.rocketmq.store.queue;

import java.nio.ByteBuffer;
import java.util.concurrent.CopyOnWriteArrayList;

import com.mawen.learn.rocketmq.store.MessageStore;
import com.mawen.learn.rocketmq.store.logfile.MappedFile;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
public class SparseConsumeQueue extends BatchConsumeQueue {


	public SparseConsumeQueue(String topic, int queueId, String storePath, int mappedFileSize, MessageStore messageStore) {
		super(topic, queueId, storePath, mappedFileSize, messageStore);
	}

	public SparseConsumeQueue(String topic, int queueId, String storePath, int mappedFileSize, MessageStore messageStore, String subfolder) {
		super(topic, queueId, storePath, mappedFileSize, messageStore, subfolder);
	}

	@Override
	public void recover() {
		CopyOnWriteArrayList<MappedFile> mappedFiles = mappedFileQueue.getMappedFiles();
		if (!mappedFiles.isEmpty()) {
			int index = mappedFiles.size() - 1;
			if (index < 0) {
				index = 0;
			}

			MappedFile mappedFile = mappedFiles.get(index);
			ByteBuffer buffer = mappedFile.sliceByteBuffer();
			int mappedFileOffset = 0;
			long processOffset = mappedFile.getFileFromOffset();

			while (true) {

			}
		}
	}
}
