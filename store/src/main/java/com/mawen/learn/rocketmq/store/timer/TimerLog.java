package com.mawen.learn.rocketmq.store.timer;

import com.mawen.learn.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
public class TimerLog {

	private static final Logger log = LoggerFactory.getLogger(TimerLog.class);

	public static final int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;

	private static final int MIN_BLANK_LEN = 4 + 8 + 4;
	/**
	 * <pre>
	 * +--------+----------+-------------+-----------------+--------------+-----------+--------+-------------------------+----------------+
	 * |  size  | prev pos | magic value | curr write time | delayed time | offsetPy  | sizePy | hash code of real topic | reversed value |
	 * +--------+----------+-------------+-----------------+--------------+-----------+--------+-------------------------+----------------+
	 * | 4bytes | 8bytes   | 4bytes      | 8bytes          | 4bytes       | 8bytes    | 4bytes | 4bytes                  | 8bytes         |
	 * +--------+----------+-------------+-----------------+--------------+-----------+--------+-------------------------+----------------+
	 * </pre>
	 */
	public static final int UNIT_SIZE = 4 + 8 + 4 + 8 + 4 + 8 + 4 + 4 + 4 + 8;

	public static final int UNIT_PER_SIZE_FOR_MSG = 28;
	public static final int UNIT_PER_SIZE_FOR_METRIC = 40;

	private final MappedFileQueue mappedFileQueue;

	private final int fileSize;

	public TimerLog(String storePath, int fileSize) {
		this.mappedFileQueue = new MappedFileQueue(storePath, fileSize, null);
		this.fileSize = fileSize;
	}

	public boolean load() {
		return mappedFileQueue.load();
	}


}
