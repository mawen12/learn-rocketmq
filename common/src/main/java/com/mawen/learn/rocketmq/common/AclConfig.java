package com.mawen.learn.rocketmq.common;

import java.util.List;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/10
 */
public class AclConfig {

	private List<String> globalWhiteAddrs;

	private List<PlainAccessConfig> plainAccessConfigs;

	public List<String> getGlobalWhiteAddrs() {
		return globalWhiteAddrs;
	}

	public void setGlobalWhiteAddrs(List<String> globalWhiteAddrs) {
		this.globalWhiteAddrs = globalWhiteAddrs;
	}

	public List<PlainAccessConfig> getPlainAccessConfigs() {
		return plainAccessConfigs;
	}

	public void setPlainAccessConfigs(List<PlainAccessConfig> plainAccessConfigs) {
		this.plainAccessConfigs = plainAccessConfigs;
	}

	@Override
	public String toString() {
		return "AclConfig{" +
				"globalWhiteAddrs=" + globalWhiteAddrs +
				", plainAccessConfigs=" + plainAccessConfigs +
				'}';
	}
}
