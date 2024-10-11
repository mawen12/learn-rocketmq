package com.mawen.learn.rocketmq.remoting.rpc;

import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/11
 */
public abstract class RpcRequestHeader implements CommandCustomHeader {

	protected String namespace;

	protected Boolean namespaced;

	protected String brokerName;

	protected Boolean oneway;

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public Boolean getNamespaced() {
		return namespaced;
	}

	public void setNamespaced(Boolean namespaced) {
		this.namespaced = namespaced;
	}

	public String getBrokerName() {
		return brokerName;
	}

	public void setBrokerName(String brokerName) {
		this.brokerName = brokerName;
	}

	public Boolean getOneway() {
		return oneway;
	}

	public void setOneway(Boolean oneway) {
		this.oneway = oneway;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RpcRequestHeader header = (RpcRequestHeader) o;
		return Objects.equals(namespace, header.namespace) && Objects.equals(namespaced, header.namespaced) && Objects.equals(brokerName, header.brokerName) && Objects.equals(oneway, header.oneway);
	}

	@Override
	public int hashCode() {
		return Objects.hash(namespace, namespaced, brokerName, oneway);
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this)
				.add("namespace", namespace)
				.add("namespaced", namespaced)
				.add("brokerName", brokerName)
				.add("oneway", oneway)
				.toString();
	}
}
