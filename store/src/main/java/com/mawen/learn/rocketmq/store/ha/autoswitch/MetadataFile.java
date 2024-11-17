package com.mawen.learn.rocketmq.store.ha.autoswitch;

import java.io.File;
import java.io.IOException;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.UtilAll;
import lombok.Getter;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/17
 */
@Getter
public abstract class MetadataFile {

	protected String filePath;

	public abstract String encodeToStr();

	public abstract void decodeFromStr(String dataStr);

	public abstract boolean isLoaded();

	public abstract void clearInMem();

	public void writeToFile() throws IOException {
		UtilAll.deleteFile(new File(filePath));
		MixAll.string2File(encodeToStr(),filePath);
	}

	public void readFromFile() throws IOException {
		String dataStr = MixAll.file2String(filePath);
		decodeFromStr(dataStr);
	}

	public boolean fileExists() {
		File file = new File(filePath);
		return file.exists();
	}

	public void clear() {
		clearInMem();
		UtilAll.deleteFile(new File(filePath));
	}
}
