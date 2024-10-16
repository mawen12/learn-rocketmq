package com.mawen.learn.rocketmq.remoting;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.logging.org.slf4j.Logger;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/16
 */
public class Configuration {

	private final Logger log;

	private List<Object> configObjectList = new ArrayList<>(4);

	private String storePath;

	private boolean storePathFromConfig = false;

	private Object storePathObject;

	private Field storePathField;

	private DataVersion dataVersion = new DataVersion();

	private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

	private Properties allConfigs = new Properties();

	public Configuration(Logger log) {
		this.log = log;
	}

	public Configuration(Logger log, Object... configObjects) {
		this.log = log;
		if (configObjects == null || configObjects.length == 0) {
			return;
		}
		for (Object configObject : configObjects) {
			if (configObject == null) {
				continue;
			}
			registerConfig(configObject);
		}
	}

	public Configuration(Logger log, String storePath, Object... configObjects) {
		this(log, configObjects);
		this.storePath = storePath;
	}

	public Configuration registerConfig(Object configObject) {
		try {
			readWriteLock.writeLock().lockInterruptibly();

			try {
				Properties properties = MixAll.object2Properties(configObject);

				merge(properties, this.allConfigs);

				configObjectList.add(configObject);
			}
			finally {
				readWriteLock.writeLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("registerConfig lock error");
		}
		return this;
	}

	public Configuration registerConfig(Properties properties) {
		if (properties == null) {
			return this;
		}

		try {
			readWriteLock.writeLock().lockInterruptibly();

			try {
				merge(properties, this.allConfigs);
			}
			finally {
				readWriteLock.writeLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("register lock error, {}", properties);
		}
		return this;
	}

	public void setStorePathFromConfig(Object object, String fieldName) {
		assert object != null;

		lockWrite(() -> {
			this.storePathFromConfig = true;
			this.storePathObject = object;
			try {
				this.storePathField = object.getClass().getDeclaredField(fieldName);
			}
			catch (NoSuchFieldException e) {
				throw new RuntimeException(e);
			}
			assert this.storePathField != null && !Modifier.isStatic(this.storePathField.getModifiers());

			this.storePathField.setAccessible(true);

			return null;
		});
	}

	public String getStorePath() {
		return lockRead(() -> {

			if (this.storePathFromConfig) {
				try {
					return (String) storePathField.get(this.storePathObject);
				}
				catch (IllegalAccessException e) {
					log.error("getStorePath error", e);
				}
			}
			return this.storePath;
		});
	}

	public void setStorePath(String storePath) {
		this.storePath = storePath;
	}

	public void update(Properties properties) {
		lockWrite(() -> {
			mergeIfExist(properties, this.allConfigs);

			for (Object configObject : configObjectList) {
				MixAll.properties2Object(properties, configObject);
			}

			this.dataVersion.nextVersion();

			return null;
		});

		persist();
	}

	public void persist() {
		try {
			readWriteLock.readLock().lockInterruptibly();

			try {
				String allConfigs = getAllConfigsInternal();

				MixAll.string2File(allConfigs, getStorePath());
			}
			catch (IOException e) {
				log.error("persist string2File error", e);
			}
			finally {
				readWriteLock.readLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("persist lock error");
		}
	}

	public String getAllConfigsFormatString() {
		try {
			readWriteLock.readLock().lockInterruptibly();

			try {
				return getAllConfigsInternal();
			}
			finally {
				readWriteLock.readLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("getAllConfigsFormatString lock error");
		}
		return null;
	}

	public String getClientConfigsFormatString(List<String> clientKeys) {
		try {
			readWriteLock.readLock().lockInterruptibly();

			try {
				return getClientConfigsInternal(clientKeys);
			}
			finally {
				readWriteLock.readLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("getClientConfigsFormatString lock error");
		}
		return null;
	}

	public String getDataVersionJson() {
		return this.dataVersion.toJson();
	}

	public Properties getAllConfigs() {
		try {
			readWriteLock.readLock().lockInterruptibly();

			try {
				return this.allConfigs;
			}
			finally {
				readWriteLock.readLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("getAllConfigs lock error");
		}
		return null;
	}

	private String getAllConfigsInternal() {
		StringBuilder stringBuilder = new StringBuilder();

		for (Object configObject : this.configObjectList) {
			Properties properties = MixAll.object2Properties(configObject);
			if (properties != null) {
				merge(properties, this.allConfigs);
			}
			else {
				log.warn("getAllConfigsInternal object2Properties is null, {}", configObject.getClass());
			}
		}

		stringBuilder.append(MixAll.properties2String(this.allConfigs, true));

		return stringBuilder.toString();
	}

	private String getClientConfigsInternal(List<String> clientConfigKeys) {
		StringBuilder stringBuilder = new StringBuilder();
		Properties clientProperties = new Properties();

		for (Object configObject : this.configObjectList) {
			Properties properties = MixAll.object2Properties(configObject);

			for (String nameNow : clientConfigKeys) {
				if (properties.containsKey(nameNow)) {
					clientProperties.put(nameNow, properties.get(nameNow));
				}
			}
		}

		stringBuilder.append(MixAll.properties2String(clientProperties));

		return stringBuilder.toString();
	}


	private void merge(Properties from, Properties to) {
		for (Map.Entry<Object, Object> entry : from.entrySet()) {
			Object fromObject = entry.getValue(), toObject = to.get(entry.getKey());
			if (toObject != null && !toObject.equals(fromObject)) {
				log.info("Replace, key: {}, value: {} -> {}", entry.getKey(), toObject, fromObject);
			}
			to.put(entry.getKey(), fromObject);
		}
	}

	private void mergeIfExist(Properties from, Properties to) {
		for (Map.Entry<Object, Object> entry : from.entrySet()) {
			if (!to.containsKey(entry.getKey())) {
				continue;
			}

			Object fromObject = entry.getValue(), toObject = to.get(entry.getKey());
			if (toObject != null && !toObject.equals(fromObject)) {
				log.info("Replace, key: {}, value: {} -> {}", entry.getKey(), toObject, fromObject);
			}
			to.put(entry.getKey(), fromObject);
		}
	}

	private <T> T lockWrite(Supplier<T> supplier) {
		try {
			readWriteLock.writeLock().lockInterruptibly();

			try {
				return supplier.get();
			}
			finally {
				readWriteLock.writeLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("lock error");
		}
		return null;
	}

	private <T> T lockRead(Supplier<T> supplier) {
		try {
			readWriteLock.readLock().lockInterruptibly();

			try {
				return supplier.get();
			}
			finally {
				readWriteLock.readLock().unlock();
			}
		}
		catch (InterruptedException e) {
			log.error("lock error");
		}
		return null;
	}
}
