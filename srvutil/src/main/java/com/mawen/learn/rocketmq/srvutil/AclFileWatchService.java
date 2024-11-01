package com.mawen.learn.rocketmq.srvutil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/1
 */
public class AclFileWatchService extends ServiceThread {

	private static final Logger log = LoggerFactory.getLogger(AclFileWatchService.class);

	private static final int WATCH_INTERVAL =  5000;

	private final String aclPath;
	private int aclFilesNum;
	private final Map<String, String> fileCurrentHash;
	private Map<String, Long> fileLastModifiedTime;
	private List<String> fileList = new ArrayList<>();
	private final AclFileWatchService.Listener listener;
	private MessageDigest md = MessageDigest.getInstance("MD5");
	private String defaultAclFile;

	public AclFileWatchService(String path, String defaultAclFile, final AclFileWatchService.Listener listener) throws Exception {
		this.aclPath = path;
		this.defaultAclFile = defaultAclFile;
		this.fileCurrentHash = new HashMap<>();
		this.fileLastModifiedTime = new HashMap<>();
		this.listener = listener;

		getAllAclFiles(path);
		if (new File(defaultAclFile).exists() && !fileList.contains(defaultAclFile)) {
			fileList.add(defaultAclFile);
		}

		aclFilesNum = fileList.size();
		for (int i = 0; i < aclFilesNum; i++) {
			String fileAbsolutePath = fileList.get(i);
			fileLastModifiedTime.put(fileAbsolutePath, new File(fileAbsolutePath).lastModified());
		}
	}

	public void getAllAclFiles(String path) {
		File file = new File(path);
		if (!file.exists()) {
			log.info("The default acl dir {} is not exist", path);
			return;
		}

		File[] files = file.listFiles();
		for (int i = 0; i < file.length(); i++) {
			String fileName = files[i].getAbsolutePath();
			File f = new File(fileName);
			if (fileName.equals(aclPath + File.separator + "tools.yml")) {
				continue;
			}
			else if (fileName.endsWith(".yml") || fileName.endsWith(".yaml")) {
				fileList.add(fileName);
			}
			else if (f.isDirectory()) {
				getAllAclFiles(fileName);
			}
		}
	}

	@Override
	public String getServiceName() {
		return "AclFileWatchService";
	}

	@Override
	public void run() {
		log.info("{} service started", getServiceName());

		while (!this.isStopped()) {
			try {
				waitForRunning(WATCH_INTERVAL);

				if (fileList.size() > 0) {
					fileList.clear();
				}
				getAllAclFiles(aclPath);

				if (new File(defaultAclFile).exists() && !fileList.contains(defaultAclFile)) {
					fileList.add(defaultAclFile);
				}

				int realAclFilesNum = fileList.size();

				if (aclFilesNum != realAclFilesNum) {
					log.info("aclFilesNum: {} realAclFilesNum: {}", aclFilesNum, realAclFilesNum);
					aclFilesNum = realAclFilesNum;
					log.info("aclFilesNum: {} realAclFilesNum: {}", aclFilesNum, realAclFilesNum);
					Map<String, Long> fileLastModifiedTime = new HashMap<>(realAclFilesNum);
					for (int i = 0; i < realAclFilesNum; i++) {
						String fileAbsolutePath = fileList.get(i);
						fileLastModifiedTime.put(fileAbsolutePath, new File(fileAbsolutePath).lastModified());
					}
					this.fileLastModifiedTime = fileLastModifiedTime;
					listener.onFileNumChanged(aclPath);
				}
				else {
					for (int i = 0; i < aclFilesNum; i++) {
						String fileName = fileList.get(i);
						Long newLastModifiedTime = new File(fileName).lastModified();
						if (!newLastModifiedTime.equals(fileLastModifiedTime.get(fileName))) {
							fileLastModifiedTime.put(fileName, newLastModifiedTime);
							listener.onFileChanged(fileName);
						}
					}
				}
			}
			catch (Exception e) {
				log.warn("{} service has exception", getServiceName(), e);
			}
		}
		log.info("{} service end", getServiceName());
	}

	private String hash(String filePath) throws IOException {
		Path path = Paths.get(filePath);
		md.update(Files.readAllBytes(path));
		byte[] hash = md.digest();
		return UtilAll.bytes2string(hash);
	}

	public interface Listener {

		void onFileChanged(String aclFileName);

		void onFileNumChanged(String path);
	}
}
