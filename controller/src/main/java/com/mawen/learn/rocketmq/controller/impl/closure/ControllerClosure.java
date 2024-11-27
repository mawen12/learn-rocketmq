package com.mawen.learn.rocketmq.controller.impl.closure;

import java.util.concurrent.CompletableFuture;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.controller.impl.event.ControllerResult;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
@Getter
@Setter
public class ControllerClosure implements Closure {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

	private final RemotingCommand requestEvent;
	private final CompletableFuture<RemotingCommand> future;
	private ControllerResult<?> controllerResult;
	private Task task;

	public ControllerClosure(RemotingCommand requestEvent) {
		this.requestEvent = requestEvent;
		this.future = new CompletableFuture<>();
		this.task = null;
	}

	@Override
	public void run(Status status) {
		if (status.isOk()) {
			RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(controllerResult.getResponseCode(), (CommandCustomHeader) controllerResult.getResponse());
			if (controllerResult.getBody() != null) {
				response.setBody(controllerResult.getBody());
			}
			if (controllerResult.getRemark() != null) {
				response.setRemark(controllerResult.getRemark());
			}
			future.complete(response);
		}
		else {
			log.error("Failed to append to jRaft node, error is {}.", status);
			future.complete(RemotingCommand.createResponseCommand(ResponseCode.CONTROLLER_JRAFT_INTERNAL_ERROR, status.getErrorMsg()));
		}
	}

	public Task taskWithThisClosure() {
		if (task != null) {
			return task;
		}

		task = new Task();
		task.setDone(this);
		task.setData(requestEvent.encode());
		return task;
	}
}
