package com.mawen.learn.rocketmq.controller.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Utils;
import com.mawen.learn.rocketmq.common.ControllerConfig;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.controller.elect.impl.DefaultElectPolicy;
import com.mawen.learn.rocketmq.controller.impl.closure.ControllerClosure;
import com.mawen.learn.rocketmq.controller.impl.event.ControllerResult;
import com.mawen.learn.rocketmq.controller.impl.manager.RaftReplicasInfoManager;
import com.mawen.learn.rocketmq.controller.impl.task.BrokerCloseChannelRequest;
import com.mawen.learn.rocketmq.controller.impl.task.CheckNotActiveBrokerRequest;
import com.mawen.learn.rocketmq.controller.impl.task.GetBrokerLiveInfoRequest;
import com.mawen.learn.rocketmq.controller.impl.task.GetSyncStateDataRequest;
import com.mawen.learn.rocketmq.controller.impl.task.RaftBrokerHeartBeatEventRequest;
import com.mawen.learn.rocketmq.controller.metrics.ControllerMetricsManager;
import com.mawen.learn.rocketmq.remoting.exception.RemotingCommandException;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingCommand;
import com.mawen.learn.rocketmq.remoting.protocol.RemotingSerializable;
import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.body.SyncStateSet;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.ElectMasterRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.GetReplicaInfoRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.GetReplicaInfoResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.admin.CleanControllerBrokerDataRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.register.ApplyBrokerIdResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.register.GetNextBrokerIdResponseHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerRequestHeader;
import io.opentelemetry.api.common.AttributesBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import static com.mawen.learn.rocketmq.controller.metrics.ControllerMetricsConstant.*;

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

	public JRaftControllerStateMachine(ControllerConfig controllerConfig, NodeId nodeId) {
		this.replicasInfoManager = new RaftReplicasInfoManager(controllerConfig);
		this.nodeId = nodeId;
		this.onLeaderStartCallbacks = new ArrayList<>();
		this.onLeaderStopCallbacks = new ArrayList<>();
	}

	@Override
	public void onApply(Iterator iterator) {
		while (iterator.hasNext()) {
			byte[] data = iterator.getData().array();
			ControllerClosure controllerClosure = (ControllerClosure) iterator.done();
			processEvent(controllerClosure, data, iterator.getTerm(), iterator.getIndex());

			iterator.next();
		}
	}

	private void processEvent(ControllerClosure controllerClosure, byte[] data, long term, long index) {
		RemotingCommand request;
		ControllerResult<?> result;

		try {
			if (controllerClosure != null) {
				request = controllerClosure.getRequestEvent();
			}
			else {
				request = RemotingCommand.decode(Arrays.copyOfRange(data, 4, data.length));
			}
			log.info("process event: term {}, index {}, request code {}", term, index, request.getCode());

			switch (request.getCode()) {
				case RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET: {
					AlterSyncStateSetRequestHeader requestHeader = request.decodeCommandCustomHeader(AlterSyncStateSetRequestHeader.class);
					SyncStateSet syncStateSet = RemotingSerializable.decode(request.getBody(), SyncStateSet.class);
					result = alterSyncStateSet(requestHeader, syncStateSet);
					break;
				}
				case RequestCode.CONTROLLER_ELECT_MASTER: {
					ElectMasterRequestHeader requestHeader = request.decodeCommandCustomHeader(ElectMasterRequestHeader.class);
					result = electMaster(requestHeader);
					break;
				}
				case RequestCode.CONTROLLER_GET_NEXT_BROKER_ID: {
					GetNextBrokerIdRequestHeader requestHeader = request.decodeCommandCustomHeader(GetNextBrokerIdRequestHeader.class);
					result = getNextBrokerId(requestHeader);
					break;
				}
				case RequestCode.CONTROLLER_APPLY_BROKER_ID: {
					ApplyBrokerIdRequestHeader requestHeader = request.decodeCommandCustomHeader(ApplyBrokerIdRequestHeader.class);
					result = applyBrokerId(requestHeader);
					break;
				}
				case RequestCode.CONTROLLER_REGISTER_BROKER: {
					RegisterBrokerToControllerRequestHeader requestHeader = request.decodeCommandCustomHeader(RegisterBrokerToControllerRequestHeader.class);
					result = registerBroker(requestHeader);
					break;
				}
				case RequestCode.CONTROLLER_GET_REPLICA_INFO: {
					GetReplicaInfoRequestHeader requestHeader = request.decodeCommandCustomHeader(GetReplicaInfoRequestHeader.class);
					result = getReplicaInfo(requestHeader);
					break;
				}
				case RequestCode.CONTROLLER_GET_SYNC_STATE_DATA: {
					List<String> brokerNames = RemotingSerializable.decode(request.getBody(), List.class);
					GetSyncStateDataRequest dataRequest = request.decodeCommandCustomHeader(GetSyncStateDataRequest.class);
					result = getSyncStateData(brokerNames, dataRequest.getInvokeTime());
					break;
				}
				case RequestCode.CLEAN_BROKER_DATA: {
					CleanControllerBrokerDataRequestHeader requestHeader = request.decodeCommandCustomHeader(CleanControllerBrokerDataRequestHeader.class);
					result = cleanBrokerData(requestHeader);
					break;
				}
				case RequestCode.GET_BROKER_LIVE_INFO_REQUEST:{
					GetBrokerLiveInfoRequest dataRequest = request.decodeCommandCustomHeader(GetBrokerLiveInfoRequest.class);
					result = replicasInfoManager.getBrokerLiveInfo(dataRequest);
					break;
				}
				case RequestCode.RAFT_BROKER_HEART_BEAT_EVENT_REQUEST: {
					RaftBrokerHeartBeatEventRequest dataRequest = request.decodeCommandCustomHeader(RaftBrokerHeartBeatEventRequest.class);
					result = replicasInfoManager.onBrokerHeartBeat(dataRequest);
					break;
				}
				case RequestCode.BROKER_CLOSE_CHANNEL_REQUEST: {
					BrokerCloseChannelRequest dataRequest = request.decodeCommandCustomHeader(BrokerCloseChannelRequest.class);
					result = replicasInfoManager.onBrokerCloseChannel(dataRequest);
					break;
				}
				case RequestCode.CHECK_NOT_ACTIVE_BROKER_REQUEST: {
					CheckNotActiveBrokerRequest dataRequest = request.decodeCommandCustomHeader(CheckNotActiveBrokerRequest.class);
					result = replicasInfoManager.checkNotActiveBroker(dataRequest);
					break;
				}
				default:
					throw new RemotingCommandException("Unknown request code: " + request.getCode());
			}
			result.getEvents().forEach(replicasInfoManager::applyEvent);
		}
		catch (RemotingCommandException e) {
			log.error("Fail to process event", e);
			if (controllerClosure != null) {
				controllerClosure.run(new Status(RaftError.EINTERNAL, e.getMessage()));
			}
			return;
		}
		log.info("process event: term {}, index {}, request code {} success with result {}",
				term, index, request.getCode(), result.toString());
		if (controllerClosure != null) {
			controllerClosure.setControllerResult(result);
			controllerClosure.run(Status.OK());
		}
	}

	private ControllerResult<AlterSyncStateSetResponseHeader> alterSyncStateSet(AlterSyncStateSetRequestHeader requestHeader, SyncStateSet syncStateSet) {
		return replicasInfoManager.alterSyncStateSet(requestHeader, syncStateSet, new RaftReplicasInfoManager.BrokerValidPredicateWithInvokeTime(requestHeader.getInvokeTime(), replicasInfoManager));
	}

	private ControllerResult<ElectMasterResponseHeader> electMaster(ElectMasterRequestHeader requestHeader) {
		ControllerResult<ElectMasterResponseHeader> result = replicasInfoManager.electMaster(requestHeader, new DefaultElectPolicy((clusterName, brokerName, brokerId) -> replicasInfoManager.isBrokerActive(clusterName, brokerName, brokerId, requestHeader.getInvokeTime()), replicasInfoManager::getBrokerLiveInfo));
		log.info("elect master, request: {}, result: {}", requestHeader, result);

		AttributesBuilder attributesBuilder = ControllerMetricsManager.newAttributesBuilder()
				.put(LABEL_CLUSTER_NAME, requestHeader.getClusterName())
				.put(LABEL_BROKER_SET, requestHeader.getBrokerName());

		switch (result.getResponseCode()) {
			case ResponseCode.SUCCESS:
				ControllerMetricsManager.electionTotal.add(1,
						attributesBuilder.put(LABEL_ELECTION_RESULT, ElectionResult.NEW_MASTER_ELECTED.getLowerCaseName()).build());
				break;
			case ResponseCode.CONTROLLER_MASTER_STILL_EXIST:
				ControllerMetricsManager.electionTotal.add(1,
						attributesBuilder.put(LABEL_ELECTION_RESULT,ElectionResult.KEEP_CURRENT_MASTER.getLowerCaseName()).build());
				break;
			case ResponseCode.CONTROLLER_MASTER_NOT_AVAILABLE:
			case ResponseCode.CONTROLLER_ELECT_MASTER_FAILED:
				ControllerMetricsManager.electionTotal.add(1,
						attributesBuilder.put(LABEL_ELECTION_RESULT, ElectionResult.NO_MASTER_ELECTED.getLowerCaseName()).build());
				break;
			default:
				break;
		}
		return result;
	}

	private ControllerResult<GetNextBrokerIdResponseHeader> getNextBrokerId(GetNextBrokerIdRequestHeader requestHeader) {
		return replicasInfoManager.getNextBrokerId(requestHeader);
	}

	private ControllerResult<ApplyBrokerIdResponseHeader> applyBrokerId(ApplyBrokerIdRequestHeader requestHeader) {
		return replicasInfoManager.applyBrokerId(requestHeader);
	}

	private ControllerResult<?> registerBroker(RegisterBrokerToControllerRequestHeader requestHeader) {
		return replicasInfoManager.registerBroker(requestHeader, new RaftReplicasInfoManager.BrokerValidPredicateWithInvokeTime(requestHeader.getInvokeTime(), replicasInfoManager));
	}

	private ControllerResult<GetReplicaInfoResponseHeader> getReplicaInfo(GetReplicaInfoRequestHeader requestHeader) {
		return replicasInfoManager.getReplicaInfo(requestHeader);
	}

	private ControllerResult<Void> getSyncStateData(List<String> brokerNames, long invokeTile) {
		return replicasInfoManager.getSyncStateData(brokerNames, new RaftReplicasInfoManager.BrokerValidPredicateWithInvokeTime(invokeTile, replicasInfoManager));
	}

	private ControllerResult<Void> cleanBrokerData(CleanControllerBrokerDataRequestHeader requestHeader) {
		return replicasInfoManager.cleanBrokerData(requestHeader, new RaftReplicasInfoManager.BrokerValidPredicateWithInvokeTime(requestHeader.getInvokeTime(), replicasInfoManager));
	}

	@Override
	public void onShutdown() {
		log.info("StateMachine {} node {} onShutdown", getClass().getName(), nodeId.toString());
	}

	@Override
	public void onSnapshotSave(SnapshotWriter writer, Closure done) {
		byte[] data;
		try {
			data = replicasInfoManager.serialize();
		}
		catch (Throwable e) {
			done.run(new Status(RaftError.EIO, "Fail to serialize replicasInfoManager state machine data"));
			return;
		}

		Utils.runInThread(() -> {
			try {
				FileUtils.writeByteArrayToFile(new File(writer.getPath() + File.separator + "data"), data);
				if (writer.addFile("data")) {
					log.info("Save snapshot, path={}", writer.getPath());
					done.run(Status.OK());
				}
				else {
					throw new IOException("Fail to add file to writer");
				}
			}
			catch (IOException e) {
				log.error("Fail to save snapshot", e);
				done.run(new Status(RaftError.EIO, "Fail to save snapshot"));
			}
		});
	}

	@Override
	public boolean onSnapshotLoad(SnapshotReader reader) {
		if (reader.getFileMeta("data") == null) {
			log.error("Fail to find data file in {}", reader.getPath());
			return false;
		}

		try {
			byte[] data = FileUtils.readFileToByteArray(new File(reader.getPath() + File.separator + "data"));
			replicasInfoManager.deserializeFrom(data);
			log.info("Load snapshot from {}", reader.getPath());
			return true;
		}
		catch (Throwable e) {
			log.error("Fail to load snapshot from {}", reader.getPath(), e);
			return false;
		}
	}

	@Override
	public void onLeaderStart(long l) {
		for (Consumer<Long> callback : onLeaderStartCallbacks) {
			callback.accept(l);
		}
		log.info("node {} Start Leader, term={}", nodeId, l);
	}

	@Override
	public void onLeaderStop(Status status) {
		for (Consumer<Status> callback : onLeaderStopCallbacks) {
			callback.accept(status);
		}
		log.info("node {} Stop Leader, status={}", nodeId, status);
	}

	public void registerOnLeaderStart(Consumer<Long> callback) {
		onLeaderStartCallbacks.add(callback);
	}

	public void registerOnLeaderStop(Consumer<Status> callback) {
		onLeaderStopCallbacks.add(callback);
	}

	@Override
	public void onError(RaftException e) {
		log.error("Encountered an error={} on StateMachine {}, node {}, raft may stop working since some error occurs, you should figure out the cause and repair or remove this node.",
				e.getStatus(), getClass().getName(), nodeId, e);
	}

	@Override
	public void onConfigurationCommitted(Configuration configuration) {
		log.info("Configuration committed, conf={}", configuration);
	}

	@Override
	public void onStopFollowing(LeaderChangeContext leaderChangeContext) {
		log.info("Stop following, ctx={}", leaderChangeContext);
	}

	@Override
	public void onStartFollowing(LeaderChangeContext leaderChangeContext) {
		log.info("Start following, ctx={}", leaderChangeContext);
	}
}
