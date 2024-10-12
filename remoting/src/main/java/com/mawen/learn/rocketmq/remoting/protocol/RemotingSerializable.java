package com.mawen.learn.rocketmq.remoting.protocol;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public class RemotingSerializable {

	private static final Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

	public static byte[] encode(final Object obj) {
		if (obj == null) {
			return null;
		}

		String json = toJson(obj, false);
		return json.getBytes(CHARSET_UTF8);
	}

	public static String toJson(final Object obj, boolean prettyFormat) {
		return JSON.toJSONString(obj, prettyFormat);
	}

	public static <T> T decode(final byte[] data, Class<T> classOfT) {
		if (data == null) {
			return null;
		}

		return fromJson(data, classOfT);
	}

	public static <T> List<T> decodeList(final byte[] data, Class<T> classOfT) {
		if (data == null) {
			return null;
		}

		String json = new String(data, CHARSET_UTF8);
		return JSON.parseArray(json, classOfT);
	}

	public static <T> T fromJson(String json, Class<T> classOfT) {
		return JSON.parseObject(json, classOfT);
	}

	public static <T> T fromJson(byte[] data, Class<T> classOfT) {
		return JSON.parseObject(data, classOfT);
	}


	public byte[] encode() {
		String json = this.toJson();
		if (json != null) {
			return json.getBytes(CHARSET_UTF8);
		}
		return null;
	}

	public byte[] encode(SerializerFeature... features) {
		String json = JSON.toJSONString(this, features);
		return json.getBytes(CHARSET_UTF8);
	}

	public String toJson() {
		return toJson(false);
	}

	public String toJson(boolean prettyFormat) {
		return toJson(this, prettyFormat);
	}



}
