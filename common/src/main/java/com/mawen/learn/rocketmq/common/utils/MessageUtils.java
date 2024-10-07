package com.mawen.learn.rocketmq.common.utils;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.hash.Hashing;
import com.mawen.learn.rocketmq.common.message.MessageConst;
import com.mawen.learn.rocketmq.common.message.MessageExt;

import static com.mawen.learn.rocketmq.common.message.MessageDecoder.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/6
 */
public class MessageUtils {

	public static Set<Integer> getShardingKeyIndexes(Collection<MessageExt> msgs, int indexSize) {
		Set<Integer> indexSet = new HashSet<>(indexSize);
		for (MessageExt msg : msgs) {
			indexSet.add(getShardingKeyIndexByMsg(msg, indexSize));
		}
		return indexSet;
	}

	public static int getShardingKeyIndexByMsg(MessageExt msg, int indexSize) {
		String shardingKey = msg.getProperty(MessageConst.PROPERTY_SHARDING_KEY);
		if (shardingKey == null) {
			shardingKey = "";
		}
		return getShardingKeyIndex(shardingKey, indexSize);
	}

	public static int getShardingKeyIndex(String shardingKey, int indexSize) {
		return Math.abs(Hashing.murmur3_32().hashBytes(shardingKey.getBytes(StandardCharsets.UTF_8)).asInt() % indexSize);
	}

	public static String deleteProperty(String propertiesString, String name) {
		if (propertiesString != null) {
			int idx0 = 0;
			int idx1;
			int idx2;

			idx1 = propertiesString.indexOf(name, idx0);
			if (idx1 != -1) {
				StringBuilder sb = new StringBuilder(propertiesString.length());
				while (true) {
					int startIndex = idx0;

					while (true) {
						idx1 = propertiesString.indexOf(name, startIndex);
						if (idx1 == -1) {
							break;
						}

						startIndex = idx1 + name.length();
						if (idx1 == 0 || propertiesString.charAt(idx1 - 1) == PROPERTY_SEPARATOR) {
							if (propertiesString.length() > idx1 + name.length() && propertiesString.charAt(idx1 + name.length()) == NAME_VALUE_SEPARATOR) {
								break;
							}
						}
					}

					if (idx1 == -1) {
						sb.append(propertiesString, idx0, propertiesString.length());
						break;
					}

					sb.append(propertiesString, idx0, idx1);
					idx2 = propertiesString.indexOf(PROPERTY_SEPARATOR, idx1 + name.length() + 1);
					if (idx2 == -1) {
						break;
					}
					idx0 = idx2 + 1;
				}
				return sb.toString();
			}
		}
		return propertiesString;
	}
}
