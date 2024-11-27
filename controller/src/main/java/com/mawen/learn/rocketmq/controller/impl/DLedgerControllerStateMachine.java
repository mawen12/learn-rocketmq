package com.mawen.learn.rocketmq.controller.impl;

import java.util.concurrent.CompletableFuture;

import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.controller.impl.event.EventMessage;
import com.mawen.learn.rocketmq.controller.impl.event.EventSerializer;
import com.mawen.learn.rocketmq.controller.impl.manager.ReplicasInfoManager;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.statemachine.CommittedEntryIterator;
import io.openmessaging.storage.dledger.statemachine.StateMachine;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
public class DLedgerControllerStateMachine implements StateMachine {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

	private final ReplicasInfoManager replicasInfoManager;
	private final EventSerializer eventSerializer;
	private final String dLedgerId;

	public DLedgerControllerStateMachine(ReplicasInfoManager replicasInfoManager, EventSerializer eventSerializer, String dLedgerGroupId, String dLedgerSelfId) {
		this.replicasInfoManager = replicasInfoManager;
		this.eventSerializer = eventSerializer;
		this.dLedgerId = generateDLedgerId(dLedgerGroupId, dLedgerSelfId);
	}

	@Override
	public void onApply(CommittedEntryIterator iterator) {
		int applyingSize = 0;
		long firstApplyIndex = -1;
		long lastApplyIndex = -1;
		while (iterator.hasNext()) {
			DLedgerEntry entry = iterator.next();
			byte[] body = entry.getBody();
			if (body != null && body.length > 0) {
				EventMessage event = eventSerializer.deserialize(body);
				replicasInfoManager.applyEvent(event);
			}

			firstApplyIndex = firstApplyIndex == -1 ? entry.getIndex() : firstApplyIndex;
			lastApplyIndex = entry.getIndex();
			applyingSize++;
		}
		log.info("Apply {} events index from {} to {} on controller {}", applyingSize, firstApplyIndex, lastApplyIndex, dLedgerId);
	}

	@Override
	public void onSnapshotSave(SnapshotWriter snapshotWriter, CompletableFuture<Boolean> completableFuture) {
		// NOP
	}

	@Override
	public boolean onSnapshotLoad(SnapshotReader snapshotReader) {
		return false;
	}

	@Override
	public void onShutdown() {
		// NOP
	}

	@Override
	public String getBindDLedgerId() {
		return dLedgerId;
	}
}
