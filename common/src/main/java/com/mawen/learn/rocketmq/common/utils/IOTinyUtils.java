package com.mawen.learn.rocketmq.common.utils;

import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/1
 */
public class IOTinyUtils {

	public static String toString(InputStream in, String encoding) throws IOException {
		return null == encoding ? toString(new InputStreamReader(in, StandardCharsets.UTF_8)) :
				toString(new InputStreamReader(in, encoding));
	}

	public static String toString(Reader reader) throws IOException {
		CharArrayWriter sw = new CharArrayWriter();
		copy(reader, sw);
		return sw.toString();
	}

	public static long copy(Reader input, Writer output) throws IOException {
		char[] buffer = new char[1 << 12];
		long count = 0;
		for (int i = 0; (i = input.read(buffer)) >= 0;) {
			output.write(buffer, 0, i);
			count += i;
		}
		return count;
	}

	public static List<String> readLines(Reader input) throws IOException {
		BufferedReader reader = toBufferReader(input);
		List<String> list = new ArrayList<>();
		String line;
		for (; ; ) {
			line = reader.readLine();
			if (line != null) {
				list.add(line);
			}
			else {
				break;
			}
		}
		return list;
	}

	private static BufferedReader toBufferReader(Reader reader) {
		return reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
	}

	public static void copyFile(String source, String target) throws IOException {
		File sf = new File(source);
		if (!sf.exists()) {
			throw new IllegalArgumentException("source file does not exist.");
		}

		File tf = new File(target);
		tf.getParentFile().mkdirs();
		if (!tf.exists() && !tf.createNewFile()) {
			throw new RuntimeException("Failed to create target file.");
		}

		try (FileChannel sc = new FileOutputStream(tf).getChannel();
		     FileChannel tc = new FileInputStream(sf).getChannel()) {
			sc.transferTo(0, sc.size(), tc);
		}
	}

	public static void delete(File fileOrDir) throws IOException {
		if (fileOrDir == null) {
			return;
		}

		if (fileOrDir.isDirectory()) {
			cleanDirectory(fileOrDir);
		}

		fileOrDir.delete();
	}

	public static void cleanDirectory(File directory) throws IOException {
		if (!directory.exists()) {
			String message = directory + " does not exist";
			throw new IllegalArgumentException(message);
		}

		if (!directory.isDirectory()) {
			String message = directory + " is not a directory";
			throw new IllegalArgumentException(message);
		}

		File[] files = directory.listFiles();
		if (files == null) {
			throw new IOException("Failed to list content of " + directory);
		}

		IOException exception = null;
		for (File file : files) {
			try {
				delete(file);
			}
			catch (IOException ioe) {
				exception = ioe;
			}
		}

		if (exception != null) {
			throw exception;
		}
	}

	public static void writeStringToFile(File file, String data, String encoding) throws IOException {
		try (OutputStream os = new FileOutputStream(file)) {
			os.write(data.getBytes(encoding));
		}
	}
}
