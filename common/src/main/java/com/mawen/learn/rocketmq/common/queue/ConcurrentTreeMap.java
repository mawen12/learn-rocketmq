package com.mawen.learn.rocketmq.common.queue;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/6
 */
public class ConcurrentTreeMap<K, V> {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

	private final ReentrantLock lock;

	private TreeMap<K, V> tree;

	private RoundQueue<K> roundQueue;

	public ConcurrentTreeMap(int capacity, Comparator<? super K> comparator) {
		this.lock = new ReentrantLock(true);
		this.tree = new TreeMap<>(comparator);
		this.roundQueue = new RoundQueue<>(capacity);
	}

	public Map.Entry<K, V> pollFirstEntry() {
		lock.lock();
		try {
			return tree.pollFirstEntry();
		}
		finally {
			lock.unlock();
		}
	}

	public V putIfAbsentAndRetExist(K key, V value) {
		lock.lock();
		try {
			if (roundQueue.put(key)) {
				V exist = tree.get(key);
				if (exist == null) {
					tree.put(key, value);
					exist = value;
				}
				log.warn("putIfAbsentAndRetExist success.", key);
				return exist;
			}
			else {
				V exist = tree.get(key);
				return exist;
			}
		}
		finally {
			lock.unlock();
		}
	}
}
