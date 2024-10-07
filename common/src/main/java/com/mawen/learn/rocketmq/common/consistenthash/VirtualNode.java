package com.mawen.learn.rocketmq.common.consistenthash;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/10/7
 */
public class VirtualNode<T extends Node> implements Node {

	final T physicalNode;

	final int replicaIndex;

	public VirtualNode(T physicalNode, int replicaIndex) {
		this.physicalNode = physicalNode;
		this.replicaIndex = replicaIndex;
	}

	@Override
	public String getKey() {
		return physicalNode.getKey() + "-" + replicaIndex;
	}

	public boolean isVirtualNodeOf(T pNode) {
		return physicalNode.getKey().equals(pNode.getKey());
	}

	public T getPhysicalNode() {
		return physicalNode;
	}
}
