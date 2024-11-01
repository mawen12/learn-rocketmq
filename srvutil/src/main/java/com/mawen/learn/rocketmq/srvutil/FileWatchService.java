package com.mawen.learn.rocketmq.srvutil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Strings;
import com.mawen.learn.rocketmq.common.LifeCycleAwareServiceThread;
import com.mawen.learn.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public class FileWatchService extends LifeCycleAwareServiceThread {

	private static final Logger log = LoggerFactory.getLogger(FileWatchService.class);

	private static final int WATCH_INTERVAL = 500;

	private final Map<String, String> currentHash = new HashMap<>();
	private final Listener listener;
	private final MessageDigest md = MessageDigest.getInstance("MD5");

	public FileWatchService(String[] watchFiles, Listener listener) throws Exception {
		this.listener = listener;
		for (String file : watchFiles) {
			if (!Strings.isNullOrEmpty(file) && new File(file).exists()) {
				currentHash.put(file, md5Digest(file));
			}
		}
	}

	@Override
	public String getServiceName() {
		return "FileWatchService";
	}

	@Override
	public void run0() {
		log.info("{} service started", getServiceName());

		while (!this.isStopped()) {
			try {
				waitForRunning(WATCH_INTERVAL);

				for (Map.Entry<String, String> entry : currentHash.entrySet()) {
					String fileName = entry.getKey();
					String newHash = md5Digest(fileName);

					if (!newHash.equals(currentHash.get(fileName))) {
						entry.setValue(newHash);
						listener.onChange(fileName);
					}
				}
			}
			catch (Exception e) {
				log.warn("{} service raised an unexpected exception.", e);
			}
		}
		log.info("{} service end", getServiceName());
	}

	private String md5Digest(String filePath) {
		Path path = Paths.get(filePath);
		if (!path.toFile().exists()) {
			return currentHash.getOrDefault(filePath, "");
		}

		byte[] raw;
		try {
			raw = Files.readAllBytes(path);
		}
		catch (IOException e) {
			log.info("Failed to read content of {}", filePath);
			return currentHash.getOrDefault(filePath, "");
		}

		md.update(raw);
		byte[] hash = md.digest();
		return UtilAll.bytes2string(hash);
	}

	public interface Listener {
		void onChange(String path);
	}
}
