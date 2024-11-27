package com.mawen.learn.rocketmq.controller.impl.heartbeat;

import java.io.Serializable;

import com.mawen.learn.rocketmq.common.UtilAll;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class BrokerIdentityInfo implements Serializable {

	private static final long serialVersionUID = -88129859318838154L;

	private final String clusterName;

	private final String brokerName;

	private final Long brokerId;

	public boolean isEmpty() {
		return UtilAll.isBlank(clusterName) && UtilAll.isBlank(brokerName) && brokerId == null;
	}
}
