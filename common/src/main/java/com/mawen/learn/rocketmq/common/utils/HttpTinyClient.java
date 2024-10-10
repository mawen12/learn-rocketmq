package com.mawen.learn.rocketmq.common.utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import com.alibaba.fastjson.util.IOUtils;
import com.mawen.learn.rocketmq.common.MQVersion;
import com.mawen.learn.rocketmq.common.MixAll;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class HttpTinyClient {

	public static HttpResult httpGet(String url, List<String> headers, List<String> paramValues, String encoding, long readTimeoutMs) throws IOException {
		String encodedContent = encodingParams(paramValues, encoding);
		url += (encodedContent == null) ? "" : ("?" + encodedContent);

		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) new URL(url).openConnection();
			conn.setRequestMethod("GET");
			conn.setConnectTimeout((int) readTimeoutMs);
			conn.setReadTimeout((int) readTimeoutMs);

			setHeaders(conn, headers, encoding);

			conn.connect();
			int respCode = conn.getResponseCode();
			String resp = null;

			if (HttpURLConnection.HTTP_OK == respCode) {
				resp = IOTinyUtils.toString(conn.getInputStream(), encoding);
			}
			else {
				resp = IOTinyUtils.toString(conn.getErrorStream(), encoding);
			}
			return new HttpResult(respCode, resp);
		}
		finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

	public static HttpResult httpPost(String url, List<String> headers, List<String> paramValues, String encoding, long readTimeoutMs) throws IOException {
		String encodedContent = encodingParams(paramValues, encoding);

		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) new URL(url).openConnection();
			conn.setRequestMethod("POST");
			conn.setConnectTimeout(3000);
			conn.setReadTimeout((int) readTimeoutMs);
			conn.setDoOutput(true);
			conn.setDoInput(true);

			setHeaders(conn, headers, encoding);

			conn.getOutputStream().write(encodedContent.getBytes(MixAll.DEFAULT_CHARSET));

			int respCode = conn.getResponseCode();
			String resp = null;

			if (HttpURLConnection.HTTP_OK == respCode) {
				resp = IOTinyUtils.toString(conn.getInputStream(), encoding);
			}
			else {
				resp = IOTinyUtils.toString(conn.getErrorStream(), encoding);
			}
			return new HttpResult(respCode, resp);
		}
		finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

	private static void setHeaders(HttpURLConnection conn, List<String> headers, String encoding) {
		if (headers != null) {
			for (Iterator<String> iter = headers.iterator(); iter.hasNext(); ) {
				conn.addRequestProperty(iter.next(), iter.next());
			}
		}
		conn.addRequestProperty("Client-Version", MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));
		conn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + encoding);

		String ts = String.valueOf(System.currentTimeMillis());
		conn.addRequestProperty("Metaq-Client-RequestTS", ts);
	}

	private static String encodingParams(List<String> paramValues, String encoding) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();
		if (paramValues == null) {
			return null;
		}

		for (Iterator<String> iter = paramValues.iterator(); iter.hasNext(); ) {
			sb.append(iter.next()).append("=").append(URLEncoder.encode(iter.next(), encoding));
			if (iter.hasNext()) {
				sb.append("&");
			}
		}
		return sb.toString();
	}

	public static class HttpResult {
		public final int code;
		public final String content;

		public HttpResult(int code, String content) {
			this.code = code;
			this.content = content;
		}
	}
}
