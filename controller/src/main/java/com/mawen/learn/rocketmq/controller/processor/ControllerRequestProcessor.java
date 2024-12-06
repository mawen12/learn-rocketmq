package com.mawen.learn.rocketmq.controller.processor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Stopwatch;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.controller.BrokerHeartbeatManger;
import com.mawen.learn.rocketmq.controller.ControllerManager;
import com.mawen.learn.rocketmq.controller.metrics.ControllerMetricsConstant;
import com.mawen.learn.rocketmq.controller.metrics.ControllerMetricsManager;
import com.mawen.learn.rocketmq.remoting.common.RemotingHelper;
import com.mawen.learn.rocketmq.remoting.netty.NettyRequestProcessor;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import io.opentelemetry.api.common.Attributes;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import static com.mawen.learn.rocketmq.controller.metrics.ControllerMetricsConstant.*;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/28
 */
public class ControllerRequestProcessor implements NettyRequestProcessor {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

	private static final int WAIT_TIMEOUT_OUT = 5;

	private final ControllerManager controllerManager;
	private final BrokerHeartbeatManger heartbeatManger;
	protected Set<String> configBlackList = new HashSet<>();

	public ControllerRequestProcessor(final ControllerManager controllerManager) {
		this.controllerManager = controllerManager;
		this.heartbeatManger = controllerManager.getHeartbeatManger();
		initConfigBlackList();
	}

	private void initConfigBlackList() {
		configBlackList.add("configBlackList");
		configBlackList.add("configStorePath");
		configBlackList.add("rocketmqHome");

		String[] configArray = controllerManager.getControllerConfig().getConfigBlackList().split(";");
		configBlackList.addAll(Arrays.asList(configArray));
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
		if (ctx != null) {
			log.debug("Receive request, {} {} {}", request.getCode(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()), request);
		}

		Stopwatch stopwatch = Stopwatch.createStarted();
		try {
			RemotingCommand resp = handleRequest(ctx, request);
			Attributes attributes = ControllerMetricsManager.newAttributesBuilder()
					.put(LABEL_REQUEST_TYPE, RequestType.getLowerCaseNameByCode(request.getCode()))
					.put(LABEL_REQUEST_HANDLE_STATUS, RequestHandleStatus.SUCCESS.getLowerCaseName())
					.build();
			ControllerMetricsManager.requestTotal.add(1, attributes);
			attributes = ControllerMetricsManager.newAttributesBuilder()
					.put(LABEL_REQUEST_TYPE, ControllerMetricsConstant.RequestType.getLowerCaseNameByCode(request.getCode()))
					.build();
			ControllerMetricsManager.requestLatency.record(stopwatch.elapsed(TimeUnit.MICROSECONDS), attributes);
			return resp;
		}
		catch (Exception e) {
			log.error("process request: {} error, ", request, e);
			Attributes attributes;
			if (e instanceof TimeoutException) {
				attributes = ControllerMetricsManager.newAttributesBuilder()
						.put(LABEL_REQUEST_TYPE, ControllerMetricsConstant.RequestType.getLowerCaseNameByCode(request.getCode()))
						.put(LABEL_REQUEST_HANDLE_STATUS, RequestHandleStatus.TIMEOUT.getLowerCaseName())
						.build();
			}
			else {
				attributes = ControllerMetricsManager.newAttributesBuilder()
						.put(LABEL_REQUEST_TYPE, ControllerMetricsConstant.RequestType.getLowerCaseNameByCode(request.getCode()))
						.put(LABEL_REQUEST_HANDLE_STATUS, RequestHandleStatus.FAILED.getLowerCaseName())
						.build();
			}
			ControllerMetricsManager.requestTotal.add(1, attributes);
			throw e;
		}
	}



	@Override
	public boolean rejectRequest() {

	}
}
