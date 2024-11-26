package com.mawen.learn.rocketmq.controller.impl;

import java.util.List;
import java.util.function.Consumer;

import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.NodeId;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
public class JRaftControllerStateMachine implements StateMachine {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

	private final List<Consumer<Long>> onLeaderStartCallbacks;
	private final List<Consumer<Status>> onLeaderStopCallbacks;
	private final RaftReplicasInfoManager replicasInfoManager;
	private final NodeId nodeId;
}
