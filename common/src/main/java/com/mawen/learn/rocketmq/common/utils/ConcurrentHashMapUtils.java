package com.mawen.learn.rocketmq.common.utils;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class ConcurrentHashMapUtils {

	private static boolean isJdk8;

	static {
		try {
			isJdk8 = System.getProperty("java.version").startsWith("1.8.");
		}
		catch (Exception ignored) {
			isJdk8 = true;
		}
	}

	public static <K, V> V computeIfAbsent(ConcurrentMap<K, V> map, K key, Function<? super K, ? extends V> func) {
		Objects.requireNonNull(func);

		if (isJdk8) {
			V v = map.get(key);
			if (v == null) {
				v = func.apply(key);
				if (v == null) {
					return null;
				}
				V res = map.putIfAbsent(key, v);
				if (res != null) {
					return res;
				}
			}
			return v;
		}
		else {
			return map.computeIfAbsent(key, func);
		}
	}
}
