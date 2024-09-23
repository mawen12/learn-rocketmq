package com.mawen.learn.rocketmq.common.attribute;

import java.security.Key;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class AttributeParser {

	public static final String ATTR_ARRAY_SEPARATOR_COMMA = ",";

	public static final String ATTR_KEY_VALUE_EQUAL_SIGN = "=";

	public static final String ATTR_ADD_PLUS_SIGN = "+";

	public static final String ATTR_DELETE_MINUS_SIGN = "-";

	public static Map<String, String> parseToMap(String attributesModification) {
		if (Strings.isNullOrEmpty(attributesModification)) {
			return new HashMap<>();
		}

		Map<String, String> attributes = new HashMap<>();
		String[] kvs = attributesModification.split(ATTR_ARRAY_SEPARATOR_COMMA);
		for (String kv : kvs) {
			String key;
			String value;
			if (kv.contains(ATTR_KEY_VALUE_EQUAL_SIGN)) {
				String[] splits = kv.split(ATTR_KEY_VALUE_EQUAL_SIGN);
				key = splits[0];
				value = splits[1];

				if (!key.contains(ATTR_ADD_PLUS_SIGN)) {
					throw new RuntimeException("add/alter attribute format is wrong: " + key);
				}
			}
			else {
				key = kv;
				value = "";

				if (!key.contains(ATTR_DELETE_MINUS_SIGN)) {
					throw new RuntimeException("delete attribute format is wrong: " + key);
				}
			}

			String old = attributes.put(key, value);
			if (old != null) {
				throw new RuntimeException("key duplication: " + key);
			}
		}
		return attributes;
	}

	public static String parseToString(Map<String, String> attributes) {
		if (attributes == null || attributes.size() == 0) {
			return "";
		}

		List<String> kvs = new ArrayList<>();
		for (Map.Entry<String, String> entry : attributes.entrySet()) {
			String value = entry.getValue();
			if (Strings.isNullOrEmpty(value)) {
				kvs.add(entry.getKey());
			}
			else {
				kvs.add(entry.getKey() + ATTR_KEY_VALUE_EQUAL_SIGN + entry.getValue());
			}
		}

		return String.join(ATTR_ARRAY_SEPARATOR_COMMA, kvs);
	}
}
