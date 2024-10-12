package com.mawen.learn.rocketmq.remoting.protocol.body;

import java.util.Map;

import com.mawen.learn.rocketmq.remoting.protocol.DataVersion;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/12
 */
public class ClusterAclVersionInfo extends RemotingSerializable {

	private String brokerName;

	private String brokerAddr;

	private DataVersion aclConfigDataVersion;

	private Map<String, DataVersion> allAclConfigDataVersion;

	private String clusterName;

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public String getBrokerAddr() {
		return brokerAddr;
	}

	public void setBrokerAddr(String brokerAddr) {
		this.brokerAddr = brokerAddr;
	}

	public DataVersion getAclConfigDataVersion() {
		return aclConfigDataVersion;
	}

	public void setAclConfigDataVersion(DataVersion aclConfigDataVersion) {
		this.aclConfigDataVersion = aclConfigDataVersion;
	}

	public Map<String, DataVersion> getAllAclConfigDataVersion() {
		return allAclConfigDataVersion;
	}

	public void setAllAclConfigDataVersion(Map<String, DataVersion> allAclConfigDataVersion) {
		this.allAclConfigDataVersion = allAclConfigDataVersion;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}
}
