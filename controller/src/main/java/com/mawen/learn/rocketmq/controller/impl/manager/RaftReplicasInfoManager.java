package com.mawen.learn.rocketmq.controller.impl.manager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.alibaba.fastjson.JSON;
import com.mawen.learn.rocketmq.common.ControllerConfig;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.controller.helper.BrokerValidPredicate;
import com.mawen.learn.rocketmq.controller.impl.event.ControllerResult;
import com.mawen.learn.rocketmq.controller.impl.heartbeat.BrokerIdentityInfo;
import com.mawen.learn.rocketmq.controller.impl.heartbeat.BrokerLiveInfo;
import com.mawen.learn.rocketmq.controller.impl.task.BrokerCloseChannelRequest;
import com.mawen.learn.rocketmq.controller.impl.task.BrokerCloseChannelResponse;
import com.mawen.learn.rocketmq.controller.impl.task.CheckNotActiveBrokerRequest;
import com.mawen.learn.rocketmq.controller.impl.task.CheckNotActiveBrokerResponse;
import com.mawen.learn.rocketmq.controller.impl.task.GetBrokerLiveInfoRequest;
import com.mawen.learn.rocketmq.controller.impl.task.GetBrokerLiveInfoResponse;
import com.mawen.learn.rocketmq.controller.impl.task.RaftBrokerHeartBeatEventRequest;
import com.mawen.learn.rocketmq.controller.impl.task.RaftBrokerHeartBeatEventResponse;
import com.mawen.learn.rocketmq.remoting.protocol.ResponseCode;
import lombok.AllArgsConstructor;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
public class RaftReplicasInfoManager extends ReplicasInfoManager {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.CONTROLLER_LOGGER_NAME);

	private final Map<BrokerIdentityInfo, BrokerLiveInfo> brokerLiveTable = new ConcurrentHashMap<>(256);

	public RaftReplicasInfoManager(ControllerConfig controllerConfig) {
		super(controllerConfig);
	}

	public ControllerResult<GetBrokerLiveInfoResponse> getBrokerLiveInfo(final GetBrokerLiveInfoRequest request) {
		BrokerIdentityInfo brokerIdentify = request.getBrokerIdentify();
		ControllerResult<GetBrokerLiveInfoResponse> result = new ControllerResult<>(new GetBrokerLiveInfoResponse());

		Map<BrokerIdentityInfo, BrokerLiveInfo> resBrokerLiveTable = new Hashtable<>();
		if (brokerIdentify == null || brokerIdentify.isEmpty()) {
			resBrokerLiveTable.putAll(brokerLiveTable);
		}
		else {
			if (brokerLiveTable.containsKey(brokerIdentify)) {
				resBrokerLiveTable.put(brokerIdentify, brokerLiveTable.get(brokerIdentify));
			}
			else {
				log.warn("GetBrokerLiveInfo failed, brokerIdentifyInfo: {} not exist", brokerIdentify);
				result.setCodeAndRemark(ResponseCode.CONTROLLER_BROKER_LIVE_INFO_NOT_EXISTS, "brokerIdentifyInfo not exist");
			}
		}

		try {
			result.setBody(JSON.toJSONBytes(resBrokerLiveTable));
		}
		catch (Throwable e) {
			log.error("json serialize resBrokerLiveTable {} error", resBrokerLiveTable, e);
			result.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "serialize error");
		}

		return result;
	}

	public ControllerResult<RaftBrokerHeartBeatEventResponse> onBrokerHeartBeat(RaftBrokerHeartBeatEventRequest request) {
		BrokerIdentityInfo brokerIdentityInfo = request.getBrokerIdentifyInfo();
		BrokerLiveInfo brokerLiveInfo = request.getBrokerLiveInfo();
		ControllerResult<RaftBrokerHeartBeatEventResponse> result = new ControllerResult<>(new RaftBrokerHeartBeatEventResponse());

		BrokerLiveInfo prev = brokerLiveTable.computeIfAbsent(brokerIdentityInfo, identifyInfo -> {
			log.info("new broker registered, brokerIdentifyInfo: {}", identifyInfo);
			return brokerLiveInfo;
		});
		prev.setLastUpdateTimestamp(brokerLiveInfo.getLastUpdateTimestamp());
		prev.setHeartbeatTimeoutMillis(brokerLiveInfo.getHeartbeatTimeoutMillis());
		prev.setElectionPriority(brokerLiveInfo.getElectionPriority());
		if (brokerLiveInfo.getEpoch() > prev.getEpoch() || brokerLiveInfo.getEpoch() == prev.getEpoch() && brokerLiveInfo.getMaxOffset() > prev.getMaxOffset()) {
			prev.setEpoch(brokerLiveInfo.getEpoch());
			prev.setMaxOffset(brokerLiveInfo.getMaxOffset());
			prev.setConfirmOffset(brokerLiveInfo.getConfirmOffset());
		}
		return result;
	}

	public ControllerResult<BrokerCloseChannelResponse> onBrokerCloseChannel(BrokerCloseChannelRequest request) {
		BrokerIdentityInfo brokerIdentifyInfo = request.getBrokerIdentifyInfo();
		ControllerResult<BrokerCloseChannelResponse> result = new ControllerResult<>(new BrokerCloseChannelResponse());

		if (brokerIdentifyInfo == null || brokerIdentifyInfo.isEmpty()) {
			log.warn("onBrokerCloseChannel failed, brokerIdentifyInfo is null");
		}
		else {
			brokerLiveTable.remove(brokerIdentifyInfo);
			log.info("onBrokerCloseChannel success, brokerIdentityInfo: {}", brokerIdentifyInfo);
		}
		return result;
	}

	public ControllerResult<CheckNotActiveBrokerResponse> checkNotActiveBroker(CheckNotActiveBrokerRequest request) {
		List<BrokerIdentityInfo> notActiveBrokerIdentityInfoList = new ArrayList<>();
		long checkTime = request.getCheckTimeMillis();
		Iterator<Map.Entry<BrokerIdentityInfo, BrokerLiveInfo>> iterator = brokerLiveTable.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<BrokerIdentityInfo, BrokerLiveInfo> next = iterator.next();
			long last = next.getValue().getLastUpdateTimestamp();
			long timeoutMillis = next.getValue().getHeartbeatTimeoutMillis();
			if (checkTime - last > timeoutMillis) {
				notActiveBrokerIdentityInfoList.add(next.getKey());
				iterator.remove();
				log.warn("Broker expired, brokerInfo {}, expired {}ms", next.getKey(), timeoutMillis);
			}
		}

		List<String> needReelectBrokerNames = scanNeedReelectBrokerSets((clusterName, brokerName, brokerId) -> !isBrokerActive(clusterName, brokerName, brokerId, checkTime));
		Set<String> alreadyReportedBrokerName = notActiveBrokerIdentityInfoList.stream()
				.map(BrokerIdentityInfo::getBrokerName)
				.collect(Collectors.toSet());

		notActiveBrokerIdentityInfoList.addAll(needReelectBrokerNames.stream()
				.filter(brokerName -> !alreadyReportedBrokerName.contains(brokerName))
				.map(brokerName -> new BrokerIdentityInfo(null, brokerName, null))
				.collect(Collectors.toList()));

		ControllerResult<CheckNotActiveBrokerResponse> result = new ControllerResult<>(new CheckNotActiveBrokerResponse());
		try {
			result.setBody(JSON.toJSONBytes(notActiveBrokerIdentityInfoList));
		}
		catch (Throwable e) {
			log.error("json serialize notActiveBrokerIdentityInfoList {} error", notActiveBrokerIdentityInfoList, e);
			result.setCodeAndRemark(ResponseCode.SYSTEM_ERROR, "serialize error");
		}
		return result;
	}

	public boolean isBrokerActive(String clusterName, String brokerName, Long brokerId, long invokeTime) {
		BrokerLiveInfo info = brokerLiveTable.get(new BrokerIdentityInfo(clusterName, brokerName, brokerId));
		if (info != null) {
			long last = info.getLastUpdateTimestamp();
			long timeoutMillis = info.getHeartbeatTimeoutMillis();
			return (last + timeoutMillis) >= invokeTime;
		}
		return false;
	}

	public BrokerLiveInfo getBrokerLiveInfo(String clusterName, String brokerName, Long brokerId) {
		return brokerLiveTable.get(new BrokerIdentityInfo(clusterName, brokerName, brokerId));
	}

	@Override
	public byte[] serialize() throws IOException {
		try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
			byte[] superSerialize = super.serialize();
			putInt(outputStream, superSerialize.length);
			outputStream.write(superSerialize);
			putInt(outputStream, brokerLiveTable.size());
			for (Map.Entry<BrokerIdentityInfo, BrokerLiveInfo> entry : brokerLiveTable.entrySet()) {
				byte[] brokerIdentityInfo = hessianSerialize(entry.getKey());
				byte[] brokerLiveInfo = hessianSerialize(entry.getValue());
				putInt(outputStream, brokerIdentityInfo.length);
				outputStream.write(brokerIdentityInfo);
				putInt(outputStream, brokerLiveInfo.length);
				outputStream.write(brokerLiveInfo);
			}
			return outputStream.toByteArray();
		}
		catch (Throwable e) {
			log.error("serialize replicaInfoTable or syncStateSetInfoTable error", e);
			throw e;
		}
	}

	@Override
	public void deserializeFrom(byte[] data) throws IOException {
		int index = 0;
		brokerLiveTable.clear();

		try {
			int superTableSize = getInt(data, index);
			index += 4;

			byte[] superTableData = new byte[superTableSize];
			System.arraycopy(data, index, superTableData, 0, superTableSize);
			super.deserializeFrom(superTableData);

			index += superTableSize;
			int brokerLiveTableSize = getInt(data, index);

			index += 4;
			for (int i = 0; i < brokerLiveTableSize; i++) {
				int brokerIdentityInfoLength = getInt(data, index);
				index += 4;

				byte[] brokerIdentityInfoArray = new byte[brokerIdentityInfoLength];
				System.arraycopy(data, index, brokerIdentityInfoArray, 0, brokerIdentityInfoLength);
				BrokerIdentityInfo brokerIdentityInfo = (BrokerIdentityInfo) hessianDeserialize(brokerIdentityInfoArray);
				index += brokerIdentityInfoLength;

				int brokerLiveInfoLength = getInt(data, index);
				index += 4;

				byte[] brokerLiveInfoArray = new byte[brokerLiveInfoLength];
				System.arraycopy(data, index, brokerIdentityInfoArray, 0, brokerIdentityInfoLength);
				BrokerLiveInfo brokerLiveInfo = (BrokerLiveInfo) hessianDeserialize(brokerLiveInfoArray);
				index += brokerLiveInfoLength;

				brokerLiveTable.put(brokerIdentityInfo, brokerLiveInfo);
			}
		}
		catch (Throwable e) {
			log.error("deserialize replicaInfoTable or syncStateSetInfoTable error" ,e);
			throw e;
		}
	}

	@AllArgsConstructor
	public static class BrokerValidPredicateWithInvokeTime implements BrokerValidPredicate {
		private final long invokeTime;
		private final RaftReplicasInfoManager raftReplicasInfoManager;

		@Override
		public boolean check(String clusterName, String brokerName, Long brokerId) {
			return raftReplicasInfoManager.isBrokerActive(clusterName,brokerName,brokerId,invokeTime);
		}
	}
}
