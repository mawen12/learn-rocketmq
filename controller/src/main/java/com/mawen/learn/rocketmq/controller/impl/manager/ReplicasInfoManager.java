package com.mawen.learn.rocketmq.controller.impl.manager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.caucho.hessian.io.SerializerFactory;
import com.mawen.learn.rocketmq.common.ControllerConfig;
import com.mawen.learn.rocketmq.common.constant.LoggerName;
import com.mawen.learn.rocketmq.controller.helper.BrokerValidPredicate;
import com.mawen.learn.rocketmq.controller.impl.event.ControllerResult;
import com.mawen.learn.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import com.mawen.learn.rocketmq.remoting.protocol.body.SyncStateSet;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetRequestHeader;
import com.mawen.learn.rocketmq.remoting.protocol.header.controller.AlterSyncStateSetResponseHeader;
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
	private final Map<String, BrokerReplicasInfo> replicaInfoTable;
	private final Map<String, SyncStateInfo> syncStateSetInfoMap;

	protected static byte[] hessianSerialize(Object object) throws IOException {
		try (ByteArrayOutputStream bout = new ByteArrayOutputStream()){
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

		if (!isContainerBroker(brokerName)) {
			resu
		}

	}
}
