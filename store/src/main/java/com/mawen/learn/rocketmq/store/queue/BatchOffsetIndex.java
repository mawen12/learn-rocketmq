package com.mawen.learn.rocketmq.store.queue;

import com.mawen.learn.rocketmq.store.logfile.MappedFile;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/16
 */
@Getter
@AllArgsConstructor
public class BatchOffsetIndex {

	private final MappedFile mappedFile;
	private final int indexPos;
	private final long msgOffset;
	private final short batchSize;
	private final long storeTimestamp;
}
