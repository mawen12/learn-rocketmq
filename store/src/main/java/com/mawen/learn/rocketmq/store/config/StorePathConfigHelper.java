package com.mawen.learn.rocketmq.store.config;

import java.io.File;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/5
 */
public class StorePathConfigHelper {

	public static String getStorePathConsumeQueue(final String rootDir) {
		return rootDir + File.separator + "consumequeue";
	}

	public static String getStorePathConsumeQueueExt(final String rootDir) {
		return rootDir + File.separator + "consumequeue_ext";
	}

	public static String getStorePathBatchConsumeQueue(final String rootDir) {
		return rootDir + File.separator + "batchconsumequeue";
	}

	public static String getStorePathIndex(final String rootDir) {
		return rootDir + File.separator + "index";
	}

	public static String getStoreCheckPoint(final String rootDir) {
		return rootDir + File.separator + "checkpoint";
	}

	public static String getAbortFile(final String rootDir) {
		return rootDir + File.separator + "abort";
	}

	public static String getLockFile(final String rootDir) {
		return rootDir + File.separator + "lock";
	}

	public static String getDelayOffsetStorePath(final String rootDir) {
		return rootDir + File.separator + "config" + File.separator + "delayOffset.json";
	}

	public static String getTranStateOffsetStorePath(final String rootDir) {
		return rootDir + File.separator + "transaction" + File.separator + "stateable";
	}

	public static String getTranRedoLogStorePath(final String rootDir) {
		return rootDir + File.separator + "transaction" + File.separator + "redolog";
	}
}
