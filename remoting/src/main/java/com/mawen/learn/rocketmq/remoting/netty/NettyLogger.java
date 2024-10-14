package com.mawen.learn.rocketmq.remoting.netty;

import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/14
 */
public class NettyLogger {

	private static AtomicBoolean nettyLoggerSeted = new AtomicBoolean(false);

	private static InternalLogLevel nettyLogLevel = InternalLogLevel.ERROR;

	public static void initNettyLogger() {
		if (!nettyLoggerSeted.get()) {
			try {
				InternalLoggerFactory.setDefaultFactory(new NettyBridgeLoggerFactory());
			}
			catch (Throwable ignored) {

			}
			nettyLoggerSeted.set(true);
		}
	}

	private static class NettyBridgeLoggerFactory extends InternalLoggerFactory {
		@Override
		protected InternalLogger newInstance(String s) {
			return new NettyBridgeLogger(s);
		}
	}

	private static class NettyBridgeLogger implements InternalLogger {

		private static final String EXCEPTION_MESSAGE = "Unexpected exception:";

		private Logger log = null;

		public NettyBridgeLogger(String name) {
			this.log = LoggerFactory.getLogger(name);
		}

		@Override
		public String name() {
			return log.getName();
		}

		@Override
		public boolean isEnabled(InternalLogLevel internalLogLevel) {
			return nettyLogLevel.ordinal() <= internalLogLevel.ordinal();
		}

		@Override
		public void log(InternalLogLevel internalLogLevel, String s) {
			if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
				log.debug(s);
			}
			if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
				log.trace(s);
			}
			if (internalLogLevel.equals(InternalLogLevel.INFO)) {
				log.info(s);
			}
			if (internalLogLevel.equals(InternalLogLevel.WARN)) {
				log.warn(s);
			}
			if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
				log.error(s);
			}
		}

		@Override
		public void log(InternalLogLevel internalLogLevel, String s, Object o) {
			if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
				log.debug(s, o);
			}
			if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
				log.trace(s, o);
			}
			if (internalLogLevel.equals(InternalLogLevel.INFO)) {
				log.info(s, o);
			}
			if (internalLogLevel.equals(InternalLogLevel.WARN)) {
				log.warn(s, o);
			}
			if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
				log.error(s, o);
			}
		}

		@Override
		public void log(InternalLogLevel internalLogLevel, String s, Object o, Object o1) {
			if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
				log.debug(s, o, o1);
			}
			if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
				log.trace(s, o, o1);
			}
			if (internalLogLevel.equals(InternalLogLevel.INFO)) {
				log.info(s, o, o1);
			}
			if (internalLogLevel.equals(InternalLogLevel.WARN)) {
				log.warn(s, o, o1);
			}
			if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
				log.error(s, o, o1);
			}
		}

		@Override
		public void log(InternalLogLevel internalLogLevel, String s, Object... objects) {
			if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
				log.debug(s, objects);
			}
			if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
				log.trace(s, objects);
			}
			if (internalLogLevel.equals(InternalLogLevel.INFO)) {
				log.info(s, objects);
			}
			if (internalLogLevel.equals(InternalLogLevel.WARN)) {
				log.warn(s, objects);
			}
			if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
				log.error(s, objects);
			}
		}

		@Override
		public void log(InternalLogLevel internalLogLevel, String s, Throwable throwable) {
			if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
				log.debug(s, throwable);
			}
			if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
				log.trace(s, throwable);
			}
			if (internalLogLevel.equals(InternalLogLevel.INFO)) {
				log.info(s, throwable);
			}
			if (internalLogLevel.equals(InternalLogLevel.WARN)) {
				log.warn(s, throwable);
			}
			if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
				log.error(s, throwable);
			}
		}

		@Override
		public void log(InternalLogLevel internalLogLevel, Throwable throwable) {
			if (internalLogLevel.equals(InternalLogLevel.DEBUG)) {
				log.debug(EXCEPTION_MESSAGE, throwable);
			}
			if (internalLogLevel.equals(InternalLogLevel.TRACE)) {
				log.trace(EXCEPTION_MESSAGE, throwable);
			}
			if (internalLogLevel.equals(InternalLogLevel.INFO)) {
				log.info(EXCEPTION_MESSAGE, throwable);
			}
			if (internalLogLevel.equals(InternalLogLevel.WARN)) {
				log.warn(EXCEPTION_MESSAGE, throwable);
			}
			if (internalLogLevel.equals(InternalLogLevel.ERROR)) {
				log.error(EXCEPTION_MESSAGE, throwable);
			}
		}

		@Override
		public boolean isTraceEnabled() {
			return isEnabled(InternalLogLevel.TRACE);
		}

		@Override
		public void trace(String s) {
			log.trace(s);
		}

		@Override
		public void trace(String s, Object o) {
			log.trace(s, o);
		}

		@Override
		public void trace(String s, Object o, Object o1) {
			log.trace(s, o, o1);
		}

		@Override
		public void trace(String s, Object... objects) {
			log.trace(s, objects);
		}

		@Override
		public void trace(String s, Throwable throwable) {
			log.trace(s, throwable);
		}

		@Override
		public void trace(Throwable throwable) {
			log.trace(EXCEPTION_MESSAGE, throwable);
		}

		@Override
		public boolean isDebugEnabled() {
			return isEnabled(InternalLogLevel.DEBUG);
		}

		@Override
		public void debug(String s) {
			log.debug(s);
		}

		@Override
		public void debug(String s, Object o) {
			log.debug(s, o);
		}

		@Override
		public void debug(String s, Object o, Object o1) {
			log.debug(s, o, o1);
		}

		@Override
		public void debug(String s, Object... objects) {
			log.debug(s, objects);
		}

		@Override
		public void debug(String s, Throwable throwable) {
			log.debug(s, throwable);
		}

		@Override
		public void debug(Throwable throwable) {
			log.debug(EXCEPTION_MESSAGE, throwable);
		}

		@Override
		public boolean isInfoEnabled() {
			return isEnabled(InternalLogLevel.INFO);
		}

		@Override
		public void info(String s) {
			log.info(s);
		}

		@Override
		public void info(String s, Object o) {
			log.info(s, o);
		}

		@Override
		public void info(String s, Object o, Object o1) {
			log.info(s, o, o1);
		}

		@Override
		public void info(String s, Object... objects) {
			log.info(s, objects);
		}

		@Override
		public void info(String s, Throwable throwable) {
			log.info(s, throwable);
		}

		@Override
		public void info(Throwable throwable) {
			log.info(EXCEPTION_MESSAGE, throwable);
		}

		@Override
		public boolean isWarnEnabled() {
			return isEnabled(InternalLogLevel.WARN);
		}

		@Override
		public void warn(String s) {
			log.warn(s);
		}

		@Override
		public void warn(String s, Object o) {
			log.warn(s, o);
		}

		@Override
		public void warn(String s, Object... objects) {
			log.warn(s, objects);
		}

		@Override
		public void warn(String s, Object o, Object o1) {
			log.warn(s, o, o1);
		}

		@Override
		public void warn(String s, Throwable throwable) {
			log.warn(s, throwable);
		}

		@Override
		public void warn(Throwable throwable) {
			log.warn(EXCEPTION_MESSAGE, throwable);
		}

		@Override
		public boolean isErrorEnabled() {
			return isEnabled(InternalLogLevel.ERROR);
		}

		@Override
		public void error(String s) {
			log.error(s);
		}

		@Override
		public void error(String s, Object o) {
			log.error(s, o);
		}

		@Override
		public void error(String s, Object o, Object o1) {
			log.error(s, o, o1);
		}

		@Override
		public void error(String s, Object... objects) {
			log.error(s, objects);
		}

		@Override
		public void error(String s, Throwable throwable) {
			log.error(s, throwable);
		}

		@Override
		public void error(Throwable throwable) {
			log.error(EXCEPTION_MESSAGE, throwable);
		}
	}
}
