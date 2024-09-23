package com.mawen.learn.rocketmq.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.mawen.learn.rocketmq.common.annotation.ImportantField;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/9/22
 */
public class BrokerIdentity {

	private static final Logger log = LoggerFactory.getLogger(BrokerIdentity.class);

	private static final String DEFAULT_CLUSTER_NAME = "DefaultCluster";

	private static String localHostName;

	public static final BrokerIdentity BROKER_CONTAINER_IDENTIFY = new BrokerIdentity(true);

	static {
		try {
			localHostName = InetAddress.getLocalHost().getHostName();
		}
		catch (UnknownHostException e) {
			log.error("Failed to obtain the host name", e);
		}
	}

	@ImportantField
	private String brokerName = defaultBrokerName();
	@ImportantField
	private String brokerClusterName = DEFAULT_CLUSTER_NAME;
	@ImportantField
	private volatile long brokerId = MixAll.MASTER_ID;

	private boolean isBrokerContainer = false;

	private boolean isInBrokerContainer = false;

	public BrokerIdentity() {}

	public BrokerIdentity(boolean isBrokerContainer) {
		this.isBrokerContainer = isBrokerContainer;
	}

	public BrokerIdentity(String brokerClusterName, String brokerName, long brokerId) {
		this.brokerClusterName = brokerClusterName;
		this.brokerName = brokerName;
		this.brokerId = brokerId;
	}

	public BrokerIdentity(String brokerName, String brokerClusterName, long brokerId, boolean isInBrokerContainer) {
		this.brokerName = brokerName;
		this.brokerClusterName = brokerClusterName;
		this.brokerId = brokerId;
		this.isInBrokerContainer = isInBrokerContainer;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public String getBrokerClusterName() {
		return brokerClusterName;
	}

	public void setBrokerClusterName(String brokerClusterName) {
		this.brokerClusterName = brokerClusterName;
	}

	public long getBrokerId() {
		return brokerId;
	}

	public void setBrokerId(long brokerId) {
		this.brokerId = brokerId;
	}

	public boolean isInBrokerContainer() {
		return isInBrokerContainer;
	}

	public void setInBrokerContainer(boolean inBrokerContainer) {
		isInBrokerContainer = inBrokerContainer;
	}

	public String defaultBrokerName() {
		return StringUtils.isEmpty(localHostName) ? "DEFAULT_BROKER" : localHostName;
	}

	public String getCanonicalName() {
		return isBrokerContainer ? "BrokerContainer" : String.format("%s_%s_%d", brokerClusterName, brokerName, brokerId);
	}

	public String getIdentifier() {
		return "#" + getCanonicalName() + "#";
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		final BrokerIdentity identity = (BrokerIdentity) o;

		return new EqualsBuilder()
				.append(brokerId, identity.brokerId)
				.append(brokerName, identity.brokerName)
				.append(brokerClusterName, identity.brokerClusterName)
				.isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37)
				.append(brokerName)
				.append(brokerClusterName)
				.append(brokerId)
				.toHashCode();
	}
}
