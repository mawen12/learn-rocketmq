package com.mawen.learn.rocketmq.controller.impl.manager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.caucho.hessian.io.SerializerFactory;
import com.mawen.learn.rocketmq.common.ControllerConfig;
import com.mawen.learn.rocketmq.common.MixAll;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.controller.elect.ElectPolicy;
import com.mawen.learn.rocketmq.controller.helper.BrokerValidPredicate;
import com.mawen.learn.rocketmq.controller.impl.event.AlterSyncStateSetEvent;
import com.mawen.learn.rocketmq.controller.impl.event.ApplyBrokerIdEvent;
import com.mawen.learn.rocketmq.controller.impl.event.CleanBrokerDataEvent;
import com.mawen.learn.rocketmq.controller.impl.event.ControllerResult;
import com.mawen.learn.rocketmq.controller.impl.event.ElectMasterEvent;
import com.mawen.learn.rocketmq.controller.impl.event.EventMessage;
import com.mawen.learn.rocketmq.controller.impl.event.EventType;
import com.mawen.learn.rocketmq.controller.impl.event.UpdateBrokerAddresssEvent;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import com.mawen.learn.rocketmq.remoting.protocol.body.BrokerMemberGroup;
import com.mawen.learn.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.ElectMasterResponseBody;
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
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.register.RegisterBrokerToControllerResponseHeader;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/26
 */
public class ReplicasInfoManager {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

	protected static final SerializerFactory SERIALIZER_FACTORY = new SerializerFactory();
	protected final ControllerConfig controllerConfig;
	private final Map<String, BrokerReplicaInfo> replicaInfoTable;
	private final Map<String, SyncStateInfo> syncStateSetInfoMap;

	protected static byte[] hessianSerialize(Object object) throws IOException {
		try (ByteArrayOutputStream bout = new ByteArrayOutputStream()) {
			Hessian2Output hessianOut = new Hessian2Output(bout);
			hessianOut.setSerializerFactory(SERIALIZER_FACTORY);
			hessianOut.writeObject(object);
			hessianOut.close();
			return bout.toByteArray();
		}
	}

	protected static Object hessianDeserialize(byte[] data) throws IOException {
		try (ByteArrayInputStream bin = new ByteArrayInputStream(data, 0, data.length)) {
			Hessian2Input hin = new Hessian2Input(bin);
			hin.setSerializerFactory(SERIALIZER_FACTORY);
			Object o = hin.readObject();
			hin.close();
			return o;
		}
	}

	public ReplicasInfoManager(final ControllerConfig controllerConfig) {
		this.controllerConfig = controllerConfig;
		this.replicaInfoTable = new ConcurrentHashMap<>();
		this.syncStateSetInfoMap = new ConcurrentHashMap<>();
	}

	public ControllerResult<AlterSyncStateSetResponseHeader> alterSyncStateSet(final AlterSyncStateSetRequestHeader request,
	                                                                           final SyncStateSet syncStateSet, final BrokerValidPredicate brokerValidPredicate) {
		String brokerName = request.getBrokerName();
		ControllerResult<AlterSyncStateSetResponseHeader> result = new ControllerResult<>(new AlterSyncStateSetResponseHeader());
		AlterSyncStateSetResponseHeader response = result.getResponse();

		if (!isContainsBroker(brokerName)) {
			result.setCodeAndRemark(ResponseCode.CONTROLLER_ALTER_SYNC_STATE_SET_FAILED, "Broker metadata is not existed");
			return result;
		}
		Set<Long> newSyncStateSet = syncStateSet.getSyncStateSet();
		SyncStateInfo syncStateInfo = syncStateSetInfoMap.get(brokerName);
		BrokerReplicaInfo brokerReplicaInfo = replicaInfoTable.get(brokerName);

		Set<Long> oldSyncStateSet = syncStateInfo.getSyncStateSet();
		if (oldSyncStateSet.size() == newSyncStateSet.size() && oldSyncStateSet.containsAll(newSyncStateSet)) {
			String err = "The newSyncStateSet is equal with oldSyncStateSet, no needed to update syncStateSet";
			log.warn("{}", err);
			result.setCodeAndRemark(ResponseCode.CONTROLLER_ALTER_SYNC_STATE_SET_FAILED, err);
			return result;
		}

		if (syncStateInfo.getMasterBrokerId() == null || !syncStateInfo.getMasterBrokerId().equals(request.getMasterBrokerId())) {
			String err = String.format("Rejecting alter syncStateSet request because the current leader is: {%s}, not {%s}", syncStateInfo.getMasterBrokerId(), request.getMasterBrokerId());
			log.error("{}", err);
			result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_MASTER, err);
			return result;
		}

		if (syncStateInfo.getMasterEpoch() != request.getMasterEpoch()) {
			String err = String.format("Rejecting alter syncStateSet request because the current master epoch is:{%d}, not {%d}", syncStateInfo.getMasterEpoch(), request.getMasterEpoch());
			log.error("{}", err);
			result.setCodeAndRemark(ResponseCode.CONTROLLER_FENCED_MASTER_EPOCH, err);
			return result;
		}

		if (syncStateSet.getSyncStateSetEpoch() != syncStateInfo.getSyncStateSetEpoch()) {
			String err = String.format("Rejecting alter syncStateSet request because the current syncStateSet epoch is:{%d}, not {%d}", syncStateInfo.getSyncStateSetEpoch(), syncStateSet.getSyncStateSetEpoch());
			log.error("{}", err);
			result.setCodeAndRemark(ResponseCode.CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH, err);
			return result;
		}

		for (Long replica : newSyncStateSet) {
			if (!brokerReplicaInfo.isBrokerExist(replica)) {
				String err = String.format("Rejecting alter syncStateSet request because the replicas {%s} don't exist", replica);
				log.error("{}", err);
				result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_MASTER, err);
				return result;
			}
			if (!brokerValidPredicate.check(brokerReplicaInfo.getClusterName(), brokerReplicaInfo.getBrokerName(), replica)) {
				String err = String.format("Rejecting alter syncStateSet request because the replicas {%s} don't exist", replica);
				log.error("{}", err);
				result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_REPLICAS, err);
				return result;
			}
		}

		if (!newSyncStateSet.contains(syncStateInfo.getMasterBrokerId())) {
			String err = String.format("Rejecting alter syncStateSet request because the newSyncStateSet don't contains origin leader {%s}", syncStateInfo.getMasterBrokerId());
			log.error(err);
			result.setCodeAndRemark(ResponseCode.CONTROLLER_ALTER_SYNC_STATE_SET_FAILED, err);
			return result;
		}

		int epoch = syncStateInfo.getSyncStateSetEpoch() + 1;
		response.setNewSyncStateSetEpoch(epoch);
		result.setBody(new SyncStateSet(newSyncStateSet, epoch).encode());
		AlterSyncStateSetEvent event = new AlterSyncStateSetEvent(brokerName, newSyncStateSet);
		result.addEvent(event);
		return result;
	}

	public ControllerResult<ElectMasterResponseHeader> electMaster(final ElectMasterRequestHeader request, final ElectPolicy electPolicy) {
		String brokerName = request.getBrokerName();
		Long brokerId = request.getBrokerId();
		ControllerResult<ElectMasterResponseHeader> result = new ControllerResult<>(new ElectMasterResponseHeader());
		ElectMasterResponseHeader response = result.getResponse();
		if (!isContainsBroker(brokerName)) {
			result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_NEED_TO_BE_REGISTERED, "Broker hasn't been registered");
			return result;
		}

		SyncStateInfo syncStateInfo = syncStateSetInfoMap.get(brokerName);
		BrokerReplicaInfo brokerReplicaInfo = replicaInfoTable.get(brokerName);
		Set<Long> syncStateSet = syncStateInfo.getSyncStateSet();
		Long oldMaster = syncStateInfo.getMasterBrokerId();
		Set<Long> allReplicaBrokers = controllerConfig.isEnableElectUncleanMaster() ? brokerReplicaInfo.getAllBroker() : null;
		Long newMaster = null;

		if (syncStateInfo.isFirstTimeForElect()) {
			newMaster = brokerId;
		}

		if (newMaster == null || newMaster == -1) {
			Long assignedBrokerId = request.getDesignateElect() ? brokerId : null;
			newMaster = electPolicy.elect(brokerReplicaInfo.getClusterName(), brokerReplicaInfo.getBrokerName(), syncStateSet, allReplicaBrokers, oldMaster, assignedBrokerId);
		}

		if (newMaster != null && newMaster.equals(oldMaster)) {
			String err = String.format("The old master %s is still alive, not need to elect new master for broker %s", oldMaster, brokerReplicaInfo.getBrokerName());
			log.warn(err);
			response.setMasterEpoch(syncStateInfo.getMasterEpoch());
			response.setSyncStateSetEpoch(syncStateInfo.getSyncStateSetEpoch());
			response.setMasterBrokerId(oldMaster);
			response.setMasterAddress(brokerReplicaInfo.getBrokerAddress(oldMaster));

			result.setBody(new ElectMasterResponseBody(syncStateSet).encode());
			result.setCodeAndRemark(ResponseCode.CONTROLLER_MASTER_STILL_EXIST, err);
			return result;
		}

		if (newMaster != null) {
			int masterEpoch = syncStateInfo.getMasterEpoch();
			int syncStateSetEpoch = syncStateInfo.getSyncStateSetEpoch();
			Set<Long> newSyncStateSet = new HashSet<>();
			newSyncStateSet.add(newMaster);

			response.setMasterBrokerId(newMaster);
			response.setMasterAddress(brokerReplicaInfo.getBrokerAddress(newMaster));
			response.setMasterEpoch(masterEpoch + 1);
			response.setSyncStateSetEpoch(syncStateSetEpoch + 1);
			ElectMasterResponseBody responseBody = new ElectMasterResponseBody(newSyncStateSet);

			BrokerMemberGroup brokerMemberGroup = buildBrokerMemberGroup(brokerReplicaInfo);
			if (brokerMemberGroup != null) {
				responseBody.setBrokerMemberGroup(brokerMemberGroup);
			}

			result.setBody(responseBody.encode());
			ElectMasterEvent event = new ElectMasterEvent(brokerName, newMaster);
			result.addEvent(event);
			log.info("Elect new master {] for broker {}", newMaster, brokerName);
			return result;
		}

		if (request.getBrokerId() == null || request.getBrokerId() == -1) {
			ElectMasterEvent event = new ElectMasterEvent(false, brokerName);
			result.addEvent(event);
			result.setCodeAndRemark(ResponseCode.CONTROLLER_MASTER_NOT_AVAILABLE, "Old master has down and failed to elect a new broker master");
		}
		else {
			result.setCodeAndRemark(ResponseCode.CONTROLLER_ELECT_MASTER_FAILED, "Failed to elect a new master");
		}
		log.warn("Failed to elect a new master for broker {}", brokerName);
		return result;
	}

	private BrokerMemberGroup buildBrokerMemberGroup(final BrokerReplicaInfo brokerReplicaInfo) {
		if (brokerReplicaInfo != null) {
			BrokerMemberGroup group = new BrokerMemberGroup(brokerReplicaInfo.getClusterName(), brokerReplicaInfo.getBrokerName());
			Map<Long, String> brokerIdTable = brokerReplicaInfo.getBrokerIdTable();
			Map<Long, String> memberGroup = new HashMap<>();
			brokerIdTable.forEach(memberGroup::put);
			group.setBrokerAddrs(memberGroup);
			return group;
		}
		return null;
	}

	public ControllerResult<GetNextBrokerIdResponseHeader> getNextBrokerId(final GetNextBrokerIdRequestHeader request) {
		String clusterName = request.getClusterName();
		String brokerName = request.getBrokerName();
		BrokerReplicaInfo brokerReplicaInfo = replicaInfoTable.get(brokerName);
		ControllerResult<GetNextBrokerIdResponseHeader> result = new ControllerResult<>(new GetNextBrokerIdResponseHeader(clusterName, brokerName));
		GetNextBrokerIdResponseHeader response = result.getResponse();

		if (brokerReplicaInfo == null) {
			response.setNextBrokerId(MixAll.FIRST_BROKER_CONTROLLER_ID);
		}
		else {
			response.setNextBrokerId(brokerReplicaInfo.getNextAssignBrokerId());
		}
		return result;
	}

	public ControllerResult<ApplyBrokerIdResponseHeader> applyBrokerId(final ApplyBrokerIdRequestHeader request) {
		String clusterName = request.getClusterName();
		String brokerName = request.getBrokerName();
		Long brokerId = request.getAppliedBrokerId();
		String registerCheckCode = request.getRegisterCheckCode();
		String brokerAddress = registerCheckCode.split(";")[0];
		BrokerReplicaInfo brokerReplicaInfo = replicaInfoTable.get(brokerName);

		ControllerResult<ApplyBrokerIdResponseHeader> result = new ControllerResult<>(new ApplyBrokerIdResponseHeader(clusterName, brokerName));
		ApplyBrokerIdEvent event = new ApplyBrokerIdEvent(clusterName, brokerName, brokerAddress, brokerId, registerCheckCode);
		if (brokerReplicaInfo == null) {
			if (brokerId == MixAll.FIRST_BROKER_CONTROLLER_ID) {
				result.addEvent(event);
			}
			else {
				result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_ID_INVALID, String.format("Broker-set: %s hasn't been registered in controller, but broker try to apply brokerId: %d", brokerName, brokerId));
			}
			return result;
		}

		if (!brokerReplicaInfo.isBrokerExist(brokerId) || registerCheckCode.equals(brokerReplicaInfo.getBrokerRegisterCheckCode(brokerId))) {
			result.addEvent(event);
			return result;
		}

		result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_ID_INVALID, String.format("Fail to apply brokerId: %d in broker-set: %s", brokerId, brokerName));
		return result;
	}

	public ControllerResult<RegisterBrokerToControllerResponseHeader> registerBroker(final RegisterBrokerToControllerRequestHeader request, final BrokerValidPredicate alivePredicate) {
		String brokerAddress = request.getBrokerAddress();
		String brokerName = request.getBrokerName();
		String clusterName = request.getClusterName();
		Long brokerId = request.getBrokerId();
		ControllerResult<RegisterBrokerToControllerResponseHeader> result = new ControllerResult<>(new RegisterBrokerToControllerResponseHeader(clusterName, brokerName));
		RegisterBrokerToControllerResponseHeader response = result.getResponse();

		if (!isContainsBroker(brokerName)) {
			result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_NEED_TO_BE_REGISTERED, String.format("Broker-set: %s hasn't need registered in controller", brokerName));
			return result;
		}

		BrokerReplicaInfo brokerReplicaInfo = replicaInfoTable.get(brokerName);
		SyncStateInfo syncStateInfo = syncStateSetInfoMap.get(brokerName);
		if (!brokerReplicaInfo.isBrokerExist(brokerId)) {
			result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_NEED_TO_BE_REGISTERED, String.format("BrokerId: %d hasn't been registered in broker-set: %s", brokerId, brokerName));
			return result;
		}
		if (syncStateInfo.isMasterExist() && alivePredicate.check(clusterName, brokerName, syncStateInfo.getMasterBrokerId())) {
			response.setMasterBrokerId(syncStateInfo.getMasterBrokerId());
			response.setMasterAddress(brokerReplicaInfo.getBrokerAddress(response.getMasterBrokerId()));
			response.setMasterEpoch(syncStateInfo.getMasterEpoch());
			response.setSyncStateSetEpoch(syncStateInfo.getSyncStateSetEpoch());
		}

		result.setBody(new SyncStateSet(syncStateInfo.getSyncStateSet(), syncStateInfo.getSyncStateSetEpoch()).encode());

		if (!brokerAddress.equals(brokerReplicaInfo.getBrokerAddress(brokerId))) {
			UpdateBrokerAddresssEvent event = new UpdateBrokerAddresssEvent(clusterName, brokerName, brokerAddress, brokerId);
			result.addEvent(event);
		}
		return result;
	}

	public ControllerResult<GetReplicaInfoResponseHeader> getReplicaInfo(final GetReplicaInfoRequestHeader request) {
		String brokerName = request.getBrokerName();
		ControllerResult<GetReplicaInfoResponseHeader> result = new ControllerResult<>(new GetReplicaInfoResponseHeader());
		GetReplicaInfoResponseHeader response = result.getResponse();

		if (isContainsBroker(brokerName)) {
			SyncStateInfo syncStateInfo = syncStateSetInfoMap.get(brokerName);
			BrokerReplicaInfo brokerReplicaInfo = replicaInfoTable.get(brokerName);
			Long masterBrokerId = syncStateInfo.getMasterBrokerId();

			response.setMasterBrokerId(masterBrokerId);
			response.setMasterAddress(brokerReplicaInfo.getBrokerAddress(masterBrokerId));
			response.setMasterEpoch(syncStateInfo.getMasterEpoch());

			result.setBody(new SyncStateSet(syncStateInfo.getSyncStateSet(), syncStateInfo.getSyncStateSetEpoch()).encode());
			return result;
		}

		result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_METADATA_NOT_EXIST,"Broker metadata is not existed");
		return result;
	}

	public ControllerResult<Void> getSyncStateData(final List<String> brokerNames, final BrokerValidPredicate validPredicate) {
		ControllerResult<Void> result = new ControllerResult<>();
		BrokerReplicasInfo brokerReplicasInfo = new BrokerReplicasInfo();
		for (String brokerName : brokerNames) {
			if (isContainsBroker(brokerName)) {
				SyncStateInfo syncStateInfo = syncStateSetInfoMap.get(brokerName);
				BrokerReplicaInfo brokerReplicaInfo = replicaInfoTable.get(brokerName);
				Set<Long> syncStateSet = syncStateInfo.getSyncStateSet();
				Long masterBrokerId = syncStateInfo.getMasterBrokerId();

				List<BrokerReplicasInfo.ReplicaIdentity> inSyncReplicas = new ArrayList<>();
				List<BrokerReplicasInfo.ReplicaIdentity> notInSyncReplicas = new ArrayList<>();

				if (brokerReplicaInfo == null) {
					continue;
				}

				brokerReplicaInfo.getBrokerIdTable().forEach((brokerId, brokerAddress) -> {
					boolean isAlive = validPredicate.check(brokerReplicaInfo.getClusterName(), brokerName, brokerId);
					BrokerReplicasInfo.ReplicaIdentity replica = new BrokerReplicasInfo.ReplicaIdentity(brokerName, brokerId, brokerAddress);
					replica.setAlive(isAlive);
					if (syncStateSet.contains(brokerId)) {
						inSyncReplicas.add(replica);
					}
					else {
						notInSyncReplicas.add(replica);
					}
				});

				BrokerReplicasInfo.ReplicasInfo inSyncState = new BrokerReplicasInfo.ReplicasInfo(masterBrokerId, brokerReplicaInfo.getBrokerAddress(masterBrokerId), syncStateInfo.getMasterEpoch(), syncStateInfo.getSyncStateSetEpoch(), inSyncReplicas, notInSyncReplicas);
				brokerReplicasInfo.addReplicaInfo(brokerName, inSyncState);
			}
		}
		result.setBody(brokerReplicasInfo.encode());
		return result;
	}

	public ControllerResult<Void> cleanBrokerData(CleanControllerBrokerDataRequestHeader request, BrokerValidPredicate validPredicate) {
		String clusterName = request.getClusterName();
		String brokerName = request.getBrokerName();
		String brokerControllerIdsToClean = request.getBrokerControllerIdsToClean();
		ControllerResult<Void> result = new ControllerResult<>();

		Set<Long> brokerIdSet = null;
		if (!request.isCleanLivingBroker()) {
			SyncStateInfo syncStateInfo = syncStateSetInfoMap.get(brokerName);
			if (StringUtils.isEmpty(brokerControllerIdsToClean) && syncStateInfo != null && syncStateInfo.getMasterBrokerId() != null) {
				String remark = String.format("Broker %s is still alive, clean up failure", brokerName);
				result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, remark);
				return result;
			}

			if (StringUtils.isNotEmpty(brokerControllerIdsToClean)) {
				try {
					brokerIdSet = Stream.of(brokerControllerIdsToClean.split(";")).map(Long::valueOf).collect(Collectors.toSet());
				}
				catch (NumberFormatException e) {
					String remark = String.format("Please set the option <brokerControllerIdsToClean> according to the format, exception: %s", e);
					result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA,remark);
					return result;
				}
				for (Long brokerId : brokerIdSet) {
					if (validPredicate.check(clusterName, brokerName, brokerId)) {
						String remark = String.format("Broker [%s %s] is still alive, clean up failure", brokerName, brokerId);
						result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA,remark);
						return result;
					}
				}
			}
		}

		if (isContainsBroker(brokerName)) {
			CleanBrokerDataEvent event = new CleanBrokerDataEvent(brokerName, brokerIdSet);
			result.addEvent(event);
			return result;
		}
		result.setCodeAndRemark(ResponseCode.CONTROLLER_INVALID_CLEAN_BROKER_METADATA, String.format("Broker %s is not existed, clean broker data failure.", brokerName));
		return result;
	}

	public List<String> scanNeedReelectBrokerSets(final BrokerValidPredicate validPredicate) {
		List<String> needReelectBrokerSets = new LinkedList<>();
		syncStateSetInfoMap.forEach((brokerName, syncStateInfo) -> {
			Long masterBrokerId = syncStateInfo.getMasterBrokerId();
			String clusterName = syncStateInfo.getClusterName();

			if (masterBrokerId != null && !validPredicate.check(clusterName, brokerName, masterBrokerId)) {
				Set<Long> brokerIds = replicaInfoTable.get(brokerName).getBrokerIdTable().keySet();
				boolean alive = brokerIds.stream().anyMatch(id -> validPredicate.check(clusterName, brokerName, id));
				if (alive) {
					needReelectBrokerSets.add(brokerName);

				}
			}
		});
		return needReelectBrokerSets;
	}

	public void applyEvent(final EventMessage event) {
		EventType type = event.getEventType();
		switch (type) {
			case ALTER_SYNC_STATE_SET_EVENT:
				handleAlterSyncStateSet((AlterSyncStateSetEvent) event);
				break;
			case APPLY_BROKER_ID_EVENT:
				handleApplyBrokerId((ApplyBrokerIdEvent) event);
				break;
			case ELECT_MASTER_EVENT:
				handleElectMaster((ElectMasterEvent) event);
				break;
			case CLEAN_BROKER_DATA_EVENT:
				handleCleanBrokerData((CleanBrokerDataEvent) event);
				break;
			case UPDATE_BROKER_ADDRESS:
				handleUpdateBrokerAddress((UpdateBrokerAddresssEvent) event);
				break;
			default:
				break;
		}
	}

	private void handleAlterSyncStateSet(final AlterSyncStateSetEvent event) {
		String brokerName = event.getBrokerName();
		if (isContainsBroker(brokerName)) {
			SyncStateInfo syncStateInfo = syncStateSetInfoMap.get(brokerName);
			syncStateInfo.updateSyncStateSetInfo(event.getNewSyncStateSet());
		}
	}

	private void handleApplyBrokerId(final ApplyBrokerIdEvent event) {
		String brokerName = event.getBrokerName();
		if (isContainsBroker(brokerName)) {
			BrokerReplicaInfo brokerReplicaInfo = replicaInfoTable.get(brokerName);
			if (!brokerReplicaInfo.isBrokerExist(event.getNewBrokerId())) {
				brokerReplicaInfo.addBroker(event.getNewBrokerId(), event.getBrokerAddress(), event.getRegisterCheckCode());
			}
		}
		else {
			String clusterName = event.getClusterName();
			BrokerReplicaInfo brokerReplicaInfo = new BrokerReplicaInfo(clusterName, brokerName);
			brokerReplicaInfo.addBroker(event.getNewBrokerId(), event.getBrokerAddress(), event.getRegisterCheckCode());
			replicaInfoTable.put(brokerName, brokerReplicaInfo);
			SyncStateInfo syncStateInfo = new SyncStateInfo(clusterName, brokerName);
			syncStateSetInfoMap.put(brokerName, syncStateInfo);
		}
	}

	private void handleUpdateBrokerAddress(final UpdateBrokerAddresssEvent event) {
		String brokerName = event.getBrokerName();
		String brokerAddress = event.getBrokerAddress();
		Long brokerId = event.getBrokerId();

		BrokerReplicaInfo brokerReplicaInfo = replicaInfoTable.get(brokerName);
		brokerReplicaInfo.updateBrokerAddress(brokerId, brokerAddress);
	}

	private void handleElectMaster(final ElectMasterEvent event) {
		String brokerName = event.getBrokerName();
		Long newMaster = event.getNewMasterBrokerId();
		if (isContainsBroker(brokerName)) {
			SyncStateInfo syncStateInfo = syncStateSetInfoMap.get(brokerName);

			if (event.isNewMasterElected()) {
				syncStateInfo.updateMasterInfo(newMaster);

				Set<Long> newSyncStateSet = new HashSet<>();
				newSyncStateSet.add(newMaster);
				syncStateInfo.updateSyncStateSetInfo(newSyncStateSet);
			}
			else {
				syncStateInfo.updateMasterInfo(null);
			}
			return;
		}
		log.error("Receive an ElectMasterEvent which contains the un-registered broker, event = {}", event);
	}

	private void handleCleanBrokerData(final CleanBrokerDataEvent event) {
		String brokerName = event.getBrokerName();
		Set<Long> brokerIdSetToClean = event.getBrokerIdSetToClean();

		if (brokerIdSetToClean == null || brokerIdSetToClean.isEmpty()) {
			replicaInfoTable.remove(brokerName);
			syncStateSetInfoMap.remove(brokerName);
		}

		if (!isContainsBroker(brokerName)) {
			return;
		}

		BrokerReplicaInfo brokerReplicaInfo = replicaInfoTable.get(brokerName);
		SyncStateInfo syncStateInfo = syncStateSetInfoMap.get(brokerName);
		for (Long brokerId : brokerIdSetToClean) {
			brokerReplicaInfo.removeBrokerId(brokerId);
			syncStateInfo.removeFromSyncState(brokerId);
		}
		if (brokerReplicaInfo.getBrokerIdTable().isEmpty()) {
			replicaInfoTable.remove(brokerName);
		}
		if (syncStateInfo.getSyncStateSet().isEmpty()) {
			syncStateSetInfoMap.remove(brokerName);
		}
	}

	private boolean isContainsBroker(String brokerName) {
		return replicaInfoTable.containsKey(brokerName) || syncStateSetInfoMap.containsKey(brokerName);
	}

	protected void putInt(ByteArrayOutputStream outputStream, int value) {
		outputStream.write((byte)(value >>> 24));
		outputStream.write((byte)(value >>> 16));
		outputStream.write((byte)(value >>> 8));
		outputStream.write((byte)value);
	}

	protected int getInt(byte[] memory, int index) {
		return memory[index] << 24 | memory[index + 1] & 0xFF << 16 | (memory[index + 2] & 0xFF) << 8 | memory[index + 3] & 0xFF;
	}

	public byte[] serialize() throws IOException {
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			putInt(outputStream, replicaInfoTable.size());
			for (Map.Entry<String, BrokerReplicaInfo> entry : replicaInfoTable.entrySet()) {
				byte[] brokerName = entry.getKey().getBytes(StandardCharsets.UTF_8);
				byte[] brokerReplicaInfo = hessianSerialize(entry.getValue());
				putInt(outputStream, brokerName.length);
				outputStream.write(brokerName);
				putInt(outputStream,brokerReplicaInfo.length);
				outputStream.write(brokerReplicaInfo);
			}
			putInt(outputStream,syncStateSetInfoMap.size());
			for (Map.Entry<String, SyncStateInfo> entry : syncStateSetInfoMap.entrySet()) {
				byte[] brokerName = entry.getKey().getBytes(StandardCharsets.UTF_8);
				byte[] syncStateInfo = hessianSerialize(entry.getValue());
				putInt(outputStream,brokerName.length);
				outputStream.write(brokerName);
				putInt(outputStream,syncStateInfo.length);
				outputStream.write(syncStateInfo);
			}
			return outputStream.toByteArray();
		}
		catch (Throwable e) {
			log.error("serialize replicaInfoTable or syncStateSetInfoTable error", e);
			throw e;
		}
	}

	public void deserializeFrom(byte[] data) throws IOException {
		int index = 0;
		replicaInfoTable.clear();
		syncStateSetInfoMap.clear();

		try {
			int replicaInfoTableSize = getInt(data, index);
			index += 4;
			for (int i = 0; i < replicaInfoTableSize; i++) {
				int brokerNameLength = getInt(data, index);
				index += 4;
				String brokerName = new String(data, index, brokerNameLength, StandardCharsets.UTF_8);
				index += brokerNameLength;
				int syncStateInfoLength = getInt(data, index);
				index += 4;
				byte[] syncStateInfoArray = new byte[syncStateInfoLength];
				System.arraycopy(data, index, syncStateInfoArray, 0, syncStateInfoLength);
				SyncStateInfo syncStateInfo = (SyncStateInfo) hessianDeserialize(syncStateInfoArray);
				index += syncStateInfoLength;
				syncStateSetInfoMap.put(brokerName, syncStateInfo);
			}
		}
		catch (Throwable e) {
			log.error("deserialize replicaInfoTable or syncStateSetInfoTable error", e);
			throw e;
		}
	}
}
