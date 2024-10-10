package com.mawen.learn.rocketmq.common;

import java.io.File;

import org.apache.rocketmq.logging.org.slf4j.MDC;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public abstract class AbstractBrokerRunnable implements Runnable{

	private static final String MDC_BROKER_CONTAINER_LOG_DIR = "brokerContainerLogDir";

	protected final BrokerIdentity brokerIdentity;

	public AbstractBrokerRunnable(BrokerIdentity brokerIdentity) {
		this.brokerIdentity = brokerIdentity;
	}

	public abstract void run0();

	@Override
	public void run() {
		try {
			if (brokerIdentity.isInBrokerContainer()) {
				MDC.put(MDC_BROKER_CONTAINER_LOG_DIR, File.separator + brokerIdentity.getCanonicalName());
			}
			run0();
		}
		finally {
			MDC.clear();
		}
	}
}
