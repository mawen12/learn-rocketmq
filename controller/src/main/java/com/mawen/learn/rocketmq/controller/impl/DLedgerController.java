package com.mawen.learn.rocketmq.controller.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.mawen.learn.rocketmq.common.ControllerConfig;
import com.mawen.learn.rocketmq.common.ServiceThread;
import com.mawen.learn.rocketmq.common.ThreadFactoryImpl;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.common.utils.ThreadUtils;
import com.mawen.learn.rocketmq.controller.Controller;
import com.mawen.learn.rocketmq.controller.impl.event.ControllerResult;
import com.mawen.learn.rocketmq.controller.impl.event.EventMessage;
import com.mawen.learn.rocketmq.controller.impl.manager.ReplicasInfoManager;
import com.mawen.learn.rocketmq.remoting.CommandCustomHeader;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerLeaderElector;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.BatchAppendEntryRequest;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
public class DLedgerController implements Controller {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

	private final DLedgerServer dLedgerServer;
	private final ControllerConfig controllerConfig;
	private final DLedgerConfig dLedgerConfig;
	private final ReplicasInfoManager replicasInfoManager;
	private final EventScheduler scheduler;


	interface EventHandler<T> {
		void run() throws Throwable;

		CompletableFuture<RemotingCommand> future();

		void handleException(final Throwable e);
	}

	class EventScheduler extends ServiceThread {
		private final BlockingQueue<EventHandler> eventQueue;

		public EventScheduler() {
			this.eventQueue = new LinkedBlockingQueue<>(1024);
		}

		@Override
		public String getServiceName() {
			return EventScheduler.class.getName();
		}

		@Override
		public void run() {
			log.info("Start event scheduler.");
			while (!isStopped()) {
				EventHandler handler;
				try {
					handler = eventQueue.poll(5, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
					continue;
				}

				try {
					if (handler != null) {
						handler.run();
					}
				}
				catch (Throwable e) {
					handler.handleException(e);
				}
			}
		}

		public <T> CompletableFuture<RemotingCommand> appendEvent(String name, Supplier<ControllerResult<T>> supplier, boolean isWriteEvent) {
			if (isStopped() || !DLedgerController.this.roleHandler.isLeaderState()) {
				RemotingCommand command = RemotingCommand.createResponseCommand(ResponseCode.CONTROLLER_NOT_LEADER, "The controller is not in leader state");
				CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
				future.complete(command);
				return future;
			}

			final EventHandler<T> event = new ControllerEventHandler<>(name, supplier, isWriteEvent);
			int tryTimes = 0;
			while (true) {
				try {
					if (!eventQueue.offer(event, 5, TimeUnit.SECONDS)) {
						continue;
					}
					return event.future();
				}
				catch (InterruptedException e) {
					log.error("Error happen in EventScheduler when append event", e);
					tryTimes++;
					if (tryTimes > 3) {
						return null;
					}
				}
			}
		}
	}

	class ControllerEventHandler<T> implements EventHandler<T> {
		private final String name;
		private final Supplier<ControllerResult<T>> supplier;
		private final CompletableFuture<RemotingCommand> future;
		private final boolean isWriteEvent;

		ControllerEventHandler(String name, Supplier<ControllerResult<T>> supplier, boolean isWriteEvent) {
			this.name = name;
			this.supplier = supplier;
			this.future = new CompletableFuture<>();
			this.isWriteEvent = isWriteEvent;
		}

		@Override
		public void run() throws Throwable {
			ControllerResult<T> result = supplier.get();
			log.info("Event queue run event {}, get the result {}", name, result);
			boolean appendSuccess = true;

			if (!isWriteEvent || result.getEvents() == null || result.getEvents().isEmpty()) {
				if (DLedgerController.this.controllerConfig.isProcessReadEvent()) {
					AppendEntryRequest request = new AppendEntryRequest();
					request.setBody(new byte[0]);
					appendSuccess = appendToDLedgerAndWait(request);
				}
			}
			else {
				List<EventMessage> events = result.getEvents();
				List<byte[]> eventBytes = new ArrayList<>(events.size());
				for (EventMessage event : events) {
					if (event != null) {
						byte[] data = DLedgerController.this.eventSerializer.serialize(event);
						if (data != null && data.length > 0) {
							eventBytes.add(data);
						}
					}
				}

				if (!eventBytes.isEmpty()) {
					BatchAppendEntryRequest request = new BatchAppendEntryRequest();
					request.setBatchMsgs(eventBytes);
					appendSuccess = appendToDLedgerAndWait(request);
				}
			}

			if (appendSuccess) {
				RemotingCommand response = RemotingCommand.createResponseCommandWithHeader(result.getResponseCode(), (CommandCustomHeader) result.getResponse());
				if (result.getBody() != null) {
					response.setBody(result.getBody());
				}
				if (result.getRemark() != null) {
					response.setRemark(result.getRemark());
				}
				future.complete(response);
			}
			else {
				log.error("Failed to append event to DLedger, the response is {}, try cancel the future", result.getResponse());
				future.cancel(true);
			}
		}

		@Override
		public CompletableFuture<RemotingCommand> future() {
			return future;
		}

		@Override
		public void handleException(Throwable e) {
			log.error("Error happen when handle event {}", name, e);
			future.completeExceptionally(e);
		}
	}

	class RoleChangeHandler implements DLedgerLeaderElector.RoleChangeHandler {
		private final String selfId;
		private final ExecutorService executorService = ThreadUtils.newSingleThreadExecutor(new ThreadFactoryImpl("DLedgerControllerRoleChangeHandler_"));

	}
}
