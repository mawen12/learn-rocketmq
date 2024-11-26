package com.mawen.learn.rocketmq.controller.impl;

import java.util.List;
import java.util.Map;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.entity.PeerId;
import com.mawen.learn.rocketmq.common.ControllerConfig;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.controller.Controller;
import com.mawen.learn.rocketmq.controller.helper.BrokerLifecycleListener;
import com.mawen.learn.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
public class JRaftController implements Controller {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

	private final RaftGroupService raftGroupService;
	private Node node;
	private final JRaftControllerStateMachine stateMachine;
	private final ControllerConfig controllerConfig;
	private final List<BrokerLifecycleListener> brokerLifecycleListeners;
	private final Map<PeerId, String> peerIdToAddr;
	private final NettyRemotingServer remotingServer;




}
