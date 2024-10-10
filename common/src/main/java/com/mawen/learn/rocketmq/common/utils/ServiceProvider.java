package com.mawen.learn.rocketmq.common.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class ServiceProvider {

	private static final Logger log = LoggerFactory.getLogger(ServiceProvider.class);

	private static ClassLoader thisClassLoader;

	public static final String PREFIX = "META-INF/service/";

	static {
		thisClassLoader = getClassLoader(ServiceProvider.class);
	}

	public static <T> T loadClass(Class<?> clazz) {
		String fullName = PREFIX + clazz.getName();
		return loadClass(fullName, clazz);
	}

	public static <T> T loadClass(String name, Class<?> clazz) {
		log.info("Looking for a resource file of name [{}] ...", name);

		T s = null;
		InputStream is = getResourceAsStream(getContextClassLoader(), name);
		if (is == null) {
			log.warn("No resource file with name [{}] found.", name);
			return null;
		}

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
			String serviceName = reader.readLine();
			if (serviceName != null && !"".equals(serviceName)) {
				s = initService(getContextClassLoader(), serviceName, clazz);
			}
			else {
				log.warn("ServiceName is empty!");
			}
		}
		catch (Exception e) {
			log.warn("Error occurred when looking for resource file " + name, e);
		}
		return s;
	}

	protected static String objectId(Object o) {
		if (o == null) {
			return "null";
		}
		else {
			return o.getClass().getName() + "@" + System.identityHashCode(o);
		}
	}

	protected static ClassLoader getClassLoader(Class<?> clazz) {
		try {
			return clazz.getClassLoader();
		}
		catch (SecurityException e) {
			log.error("Unable to get classloader for class {} due to security restrictions, error info {}", clazz, e.getMessage());
			throw e;
		}
	}

	protected static ClassLoader getContextClassLoader() {
		ClassLoader classLoader = null;
		try {
			classLoader = Thread.currentThread().getContextClassLoader();
		}
		catch (SecurityException ignored) {

		}
		return classLoader;
	}

	protected static InputStream getResourceAsStream(ClassLoader classLoader, String name) {
		if (classLoader != null) {
			return classLoader.getResourceAsStream(name);
		}
		return ClassLoader.getSystemResourceAsStream(name);
	}

	protected static <T> T initService(ClassLoader classLoader, String serviceName, Class<?> clazz) {
		Class<?> serviceClazz = null;
		try {
			if (classLoader != null) {
				try {
					serviceClazz = classLoader.loadClass(serviceName);
					if (clazz.isAssignableFrom(serviceClazz)) {
						log.info("Loaded class {} from classLoader {}", serviceClazz.getName(), objectId(classLoader));
					}
					else {
						log.error("Class {} loaded from classloader {} does not extend {} as loaded by this classloader.", serviceClazz.getName(), objectId(serviceClazz.getClassLoader()), clazz.getName());
					}
				}
				catch (ClassNotFoundException e) {
					if (classLoader == thisClassLoader) {
						log.warn("Unable to locate ant class {} via classloader {}", serviceName, objectId(classLoader));
						throw e;
					}
				}
				catch (NoClassDefFoundError e) {
					if (classLoader == thisClassLoader) {
						log.warn("Class {} cannot be loaded via classloader {}. it depends on some other class that cannot be found.", serviceClazz, objectId(classLoader));
						throw e;
					}
				}
			}
		}
		catch (Exception e) {
			log.error("Unable to init service.", e);
		}
		return (T) serviceClazz;
	}
}
