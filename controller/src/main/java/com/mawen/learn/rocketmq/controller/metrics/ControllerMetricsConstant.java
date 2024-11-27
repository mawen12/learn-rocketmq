package com.mawen.learn.rocketmq.controller.metrics;

import com.mawen.learn.rocketmq.remoting.protocol.RequestCode;

/**
 *
 * @author <a href="1181963012mw@gmail.com">mawen12</a>
 * @since 2024/11/27
 */
public class ControllerMetricsConstant {

	public static final String LABEL_ADDRESS = "address";
	public static final String LABEL_GROUP = "group";
	public static final String LABEL_PEER_ID = "peer_id";
	public static final String LABEL_AGGREGATION = "aggregation";
	public static final String AGGREGATION_DELTA = "delta";

	public static final String OPEN_TELEMETRY_METER_NAME = "controller";

	public static final String GAUGE_ROLE = "role";

	public static final String GAUGE_DLEDGER_DISK_USGAE = "dledger_disk_usage";

	public static final String GAUGE_ACTIVE_BROKER_NUM = "active_broker_num";

	public static final String COUNTER_REQUEST_TOTAL = "request_total";

	public static final String COUNTER_DLEDGER_OP_TOTAL = "dledger_op_total";

	public static final String COUNTER_ELECTION_TOTAL = "election_total";

	public static final String HISTOGRAM_REQUEST_LATENCY = "request_latency";

	public static final String HISTOGRAM_DLEDGER_OP_LATENCY = "dledger_op_latency";

	public static final String LABEL_CLUSTER_NAME = "cluster";

	public static final String LABEL_BROKER_SET = "broker_set";

	public static final String LABEL_REQUEST_TYPE = "request_type";

	public static final String LABEL_REQUEST_HANDLE_STATUS = "request_handle_status";

	public static final String LABEL_DLEDGER_OPERATION = "dledger_operation";

	public static final String LABEL_DLEDGER_OPERATION_STATUS = "dLedger_operation_status";

	public static final String LABEL_ELECTION_RESULT = "election_result";

	public enum RequestType {
		CONTROLLER_ALTER_SYNC_STATE_SET(RequestCode.CONTROLLER_ALTER_SYNC_STATE_SET),

		CONTROLLER_ELECT_MASTER(RequestCode.CONTROLLER_ELECT_MASTER),

		CONTROLLER_REGISTER_BROKER(RequestCode.CONTROLLER_REGISTER_BROKER),

		CONTROLLER_GET_REPLICA_INFO(RequestCode.CONTROLLER_GET_REPLICA_INFO),

		CONTROLLER_GET_METADATA_INFO(RequestCode.CONTROLLER_GET_METADATA_INFO),

		CONTROLLER_GET_SYNC_STATE_DATA(RequestCode.CONTROLLER_GET_SYNC_STATE_DATA),

		CONTROLLER_NOTIFY_BROKER_ROLE_CHANGED(RequestCode.NOTIFY_BROKER_ROLE_CHANGED),

		CONTROLLER_BROKER_HEARTBEAT(RequestCode.BROKER_HEARTBEAT),

		CONTROLLER_UPDATER_CONTROLLER_CONFIG(RequestCode.UPDATE_CONTROLLER_CONFIG),

		CONTROLLER_GET_CONTROLLER_CONFIG(RequestCode.GET_CONTROLLER_CONFIG),

		CONTROLLER_CLEAN_BROKER_DATA(RequestCode.CLEAN_BROKER_DATA),

		CONTROLLER_GET_NEXT_BROKER_ID(RequestCode.CONTROLLER_GET_NEXT_BROKER_ID),

		CONTROLLER_APPLY_BROKER_ID(RequestCode.CONTROLLER_APPLY_BROKER_ID)
		;

		private final int code;

		RequestType(int code) {
			this.code = code;
		}

		public static String getLowerCaseNameByCode(int code) {
			for (RequestType requestType : RequestType.values()) {
				if (requestType.code == code) {
					return requestType.name();
				}
			}
			return null;
		}
	}

	public enum RequestHandleStatus {
		SUCCESS,

		FAILED,

		TIMEOUT;

		public String getLowerCaseName() {
			return this.name().toLowerCase();
		}
	}

	public enum DLedgerOperation {
		APPEND;

		public String getLowerCaseName() {
			return this.name().toLowerCase();
		}
	}

	public enum DLedgerOperationStatus {
		SUCCESS,

		FAILED,

		TIMEOUT;

		public String getLowerCaseName() {
			return this.name().toLowerCase();
		}
	}

	public enum ElectionResult {
		NEW_MASTER_ELECTED,

		KEEP_CURRENT_MASTER,

		NO_MASTER_ELECTED;

		public String getLowerCaseName() {
			return this.name().toLowerCase();
		}
	}
}
