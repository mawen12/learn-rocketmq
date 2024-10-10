package com.mawen.learn.rocketmq.common.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.UtilAll;
import org.apache.commons.collections.CollectionUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class CheckpointFile<T> {

	private static final int NOT_CHECK_CRC_MAGIC_CODE = 0;

	private final String filePath;
	private final CheckpointSerializer<T> serializer;

	public CheckpointFile(String filePath, CheckpointSerializer<T> serializer) {
		this.filePath = filePath;
		this.serializer = serializer;
	}

	public String getBackFilePath() {
		return this.filePath + ".bak";
	}

	public void write(List<T> entries) throws IOException {
		if (entries.isEmpty()) {
			return;
		}

		synchronized (this) {
			StringBuilder entryContent = new StringBuilder();
			for (T entry : entries) {
				String line = this.serializer.toLine(entry);
				if (line != null && !line.isEmpty()) {
					entryContent.append(line);
					entryContent.append(System.lineSeparator());
				}
			}

			int crc32 = UtilAll.crc32(entryContent.toString().getBytes(StandardCharsets.UTF_8));

			String content = entries.size() + System.lineSeparator() + crc32 + System.lineSeparator() + entryContent;

			MixAll.string2File(content, this.filePath);
		}
	}

	public List<T> read() throws IOException {
		try {
			List<T> result = this.read(this.filePath);
			if (CollectionUtils.isEmpty(result)) {
				result = this.read(this.getBackFilePath());
			}
			return result;
		}
		catch (IOException e) {
			return this.read(this.getBackFilePath());
		}
	}

	public List<T> read(String filePath) throws IOException {
		final List<T> result = new ArrayList<>();
		synchronized (this) {
			File file = new File(filePath);
			if (!file.exists()) {
				return result;
			}

			try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
				int expectedLines = Integer.parseInt(reader.readLine());

				int expectedCrc32 = Integer.parseInt(reader.readLine());

				StringBuilder sb = new StringBuilder();
				String line = reader.readLine();
				while (line != null) {
					sb.append(line).append(System.lineSeparator());
					T entry = this.serializer.fromLine(line);
					if (entry != null) {
						result.add(entry);
					}
					line = reader.readLine();
				}

				int truthCrc32 = UtilAll.crc32(sb.toString().getBytes(StandardCharsets.UTF_8));
				if (result.size() != expectedLines) {
					String err = String.format("Expect %d entries, only found %d entries", expectedLines, result.size());
					throw new IOException(err);
				}

				if (NOT_CHECK_CRC_MAGIC_CODE != expectedCrc32 && truthCrc32 != expectedCrc32) {
					String str = String.format("Entries crc32 not match, file=%s, truth=%s", expectedCrc32, truthCrc32);
					throw new IOException(str);
				}

				return result;
			}
		}
	}

	public interface CheckpointSerializer<T> {
		String toLine(T entry);

		T fromLine(String line);
	}
}
