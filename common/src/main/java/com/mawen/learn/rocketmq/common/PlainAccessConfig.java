package com.mawen.learn.rocketmq.common;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class PlainAccessConfig implements Serializable {

	private static final long serialVersionUID = 7129146824916807847L;

	private String accessKey;

	private String secretKey;

	private String whiteRemoteAddress;

	private boolean admin;

	private String defaultTopicPerm;

	private String defaultGroupPerm;

	private List<String> topicPerms;

	private List<String> groupPerms;

	public String getAccessKey() {
		return accessKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public String getWhiteRemoteAddress() {
		return whiteRemoteAddress;
	}

	public void setWhiteRemoteAddress(String whiteRemoteAddress) {
		this.whiteRemoteAddress = whiteRemoteAddress;
	}

	public boolean isAdmin() {
		return admin;
	}

	public void setAdmin(boolean admin) {
		this.admin = admin;
	}

	public String getDefaultTopicPerm() {
		return defaultTopicPerm;
	}

	public void setDefaultTopicPerm(String defaultTopicPerm) {
		this.defaultTopicPerm = defaultTopicPerm;
	}

	public String getDefaultGroupPerm() {
		return defaultGroupPerm;
	}

	public void setDefaultGroupPerm(String defaultGroupPerm) {
		this.defaultGroupPerm = defaultGroupPerm;
	}

	public List<String> getTopicPerms() {
		return topicPerms;
	}

	public void setTopicPerms(List<String> topicPerms) {
		this.topicPerms = topicPerms;
	}

	public List<String> getGroupPerms() {
		return groupPerms;
	}

	public void setGroupPerms(List<String> groupPerms) {
		this.groupPerms = groupPerms;
	}

	@Override
	public String toString() {
		return "PlainAccessConfig{" +
				"accessKey='" + accessKey + '\'' +
				", whiteRemoteAddress='" + whiteRemoteAddress + '\'' +
				", admin=" + admin +
				", defaultTopicPerm='" + defaultTopicPerm + '\'' +
				", defaultGroupPerm='" + defaultGroupPerm + '\'' +
				", topicPerms=" + topicPerms +
				", groupPerms=" + groupPerms +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		PlainAccessConfig config = (PlainAccessConfig) o;
		return admin == config.admin && Objects.equals(accessKey, config.accessKey) && Objects.equals(secretKey, config.secretKey) && Objects.equals(whiteRemoteAddress, config.whiteRemoteAddress) && Objects.equals(defaultTopicPerm, config.defaultTopicPerm) && Objects.equals(defaultGroupPerm, config.defaultGroupPerm) && Objects.equals(topicPerms, config.topicPerms) && Objects.equals(groupPerms, config.groupPerms);
	}

	@Override
	public int hashCode() {
		return Objects.hash(accessKey, secretKey, whiteRemoteAddress, admin, defaultTopicPerm, defaultGroupPerm, topicPerms, groupPerms);
	}
}
