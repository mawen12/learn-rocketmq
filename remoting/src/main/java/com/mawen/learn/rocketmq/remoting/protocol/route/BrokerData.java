package com.mawen.learn.rocketmq.remoting.protocol.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.mawen.learn.rocketmq.common.MixAll;
import org.apache.commons.lang3.StringUtils;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/13
 */
public class BrokerData implements Comparable<BrokerData>{

	private String cluster;

	private String brokerName;

	private Map<Long, String> brokerAddrs;

	private String zoneName;

	private final Random random = new Random();

	private boolean enableActingMaster = false;

	public BrokerData() {
	}

	public BrokerData(BrokerData brokerData) {
		this.cluster = brokerData.cluster;
		this.brokerName = brokerData.brokerName;
		if (brokerData.brokerAddrs != null) {
			this.brokerAddrs = new HashMap<>(brokerData.brokerAddrs);
		}
		this.zoneName = brokerData.zoneName;
		this.enableActingMaster = brokerData.enableActingMaster;
	}

	public BrokerData(String cluster, String brokerName, Map<Long, String> brokerAddrs) {
		this.cluster = cluster;
		this.brokerName = brokerName;
		this.brokerAddrs = brokerAddrs;
	}

	public BrokerData(String cluster, String brokerName, Map<Long, String> brokerAddrs, boolean enableActingMaster) {
		this.cluster = cluster;
		this.brokerName = brokerName;
		this.brokerAddrs = brokerAddrs;
		this.enableActingMaster = enableActingMaster;
	}

	public BrokerData(String cluster, String brokerName, Map<Long, String> brokerAddrs, boolean enableActingMaster, String zoneName) {
		this.cluster = cluster;
		this.brokerName = brokerName;
		this.brokerAddrs = brokerAddrs;
		this.zoneName = zoneName;
		this.enableActingMaster = enableActingMaster;
	}

	public String selectBrokerAddr() {
		String masterAddress = this.brokerAddrs.get(MixAll.MASTER_ID);

		if (masterAddress == null) {
			List<String> addrs = new ArrayList<>(brokerAddrs.values());
			return addrs.get(random.nextInt(addrs.size()));
		}

		return masterAddress;
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public Map<Long, String> getBrokerAddrs() {
		return brokerAddrs;
	}

	public void setBrokerAddrs(Map<Long, String> brokerAddrs) {
		this.brokerAddrs = brokerAddrs;
	}

	public String getZoneName() {
		return zoneName;
	}

	public void setZoneName(String zoneName) {
		this.zoneName = zoneName;
	}

	public Random getRandom() {
		return random;
	}

	public boolean isEnableActingMaster() {
		return enableActingMaster;
	}

	public void setEnableActingMaster(boolean enableActingMaster) {
		this.enableActingMaster = enableActingMaster;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((brokerAddrs == null) ? 0 : brokerAddrs.hashCode());
		result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		BrokerData other = (BrokerData) obj;
		if (brokerAddrs == null) {
			if (other.brokerAddrs != null) {
				return false;
			}
		} else if (!brokerAddrs.equals(other.brokerAddrs)) {
			return false;
		}
		return StringUtils.equals(brokerName, other.brokerName);
	}

	@Override
	public String toString() {
		return "BrokerData [brokerName=" + brokerName + ", brokerAddrs=" + brokerAddrs + ", enableActingMaster=" + enableActingMaster + "]";
	}

	@Override
	public int compareTo(BrokerData o) {
		return this.brokerName.compareTo(o.getBrokerName());
	}
}
