package com.jnks.iot.server.common.data.audit;

import lombok.Getter;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;

import java.util.Optional;

public enum ActionType {

    ADDED(JnksIotMsgType.ENTITY_CREATED), // log entity
    DELETED(JnksIotMsgType.ENTITY_DELETED), // log string id
    UPDATED(JnksIotMsgType.ENTITY_UPDATED), // log entity
    ATTRIBUTES_UPDATED(JnksIotMsgType.ATTRIBUTES_UPDATED), // log attributes/values
    ATTRIBUTES_DELETED(JnksIotMsgType.ATTRIBUTES_DELETED), // log attributes
    TIMESERIES_UPDATED(JnksIotMsgType.TIMESERIES_UPDATED), // log timeseries update
    TIMESERIES_DELETED(JnksIotMsgType.TIMESERIES_DELETED), // log timeseries
    RPC_CALL, // log method and params
    CREDENTIALS_UPDATED, // log new credentials
    ASSIGNED_TO_CUSTOMER(JnksIotMsgType.ENTITY_ASSIGNED), // log customer name
    UNASSIGNED_FROM_CUSTOMER(JnksIotMsgType.ENTITY_UNASSIGNED), // log customer name
    ACTIVATED, // log string id
    SUSPENDED, // log string id
    CREDENTIALS_READ(true), // log device id
    ATTRIBUTES_READ(true), // log attributes
    RELATION_ADD_OR_UPDATE(JnksIotMsgType.RELATION_ADD_OR_UPDATE),
    RELATION_DELETED(JnksIotMsgType.RELATION_DELETED),
    RELATIONS_DELETED(JnksIotMsgType.RELATIONS_DELETED),
    REST_API_RULE_ENGINE_CALL, // log call to rule engine from REST API
    ALARM_ACK(JnksIotMsgType.ALARM_ACK, true),
    ALARM_CLEAR(JnksIotMsgType.ALARM_CLEAR, true),
    ALARM_DELETE(JnksIotMsgType.ALARM_DELETE, true),
    ALARM_ASSIGNED(JnksIotMsgType.ALARM_ASSIGNED, true),
    ALARM_UNASSIGNED(JnksIotMsgType.ALARM_UNASSIGNED, true),
    LOGIN,
    LOGOUT,
    LOCKOUT,
    ASSIGNED_FROM_TENANT(JnksIotMsgType.ENTITY_ASSIGNED_FROM_TENANT),
    ASSIGNED_TO_TENANT(JnksIotMsgType.ENTITY_ASSIGNED_TO_TENANT),
    PROVISION_SUCCESS(JnksIotMsgType.PROVISION_SUCCESS),
    PROVISION_FAILURE(JnksIotMsgType.PROVISION_FAILURE),
    ADDED_COMMENT(JnksIotMsgType.COMMENT_CREATED),
    UPDATED_COMMENT(JnksIotMsgType.COMMENT_UPDATED),
    DELETED_COMMENT,
    SMS_SENT;

    @Getter
    private final boolean read;

    private final JnksIotMsgType ruleEngineMsgType;

    @Getter
    private final boolean alarmAction;

    ActionType() {
        this(false, null, false);
    }

    ActionType(boolean read) {
        this(read, null, false);
    }

    ActionType(JnksIotMsgType ruleEngineMsgType) {
        this(false, ruleEngineMsgType, false);
    }

    ActionType(JnksIotMsgType ruleEngineMsgType, boolean isAlarmAction) {
        this(false, ruleEngineMsgType, isAlarmAction);
    }

    ActionType(boolean read, JnksIotMsgType ruleEngineMsgType, boolean alarmAction) {
        this.read = read;
        this.ruleEngineMsgType = ruleEngineMsgType;
        this.alarmAction = alarmAction;
    }

    public Optional<JnksIotMsgType> getRuleEngineMsgType() {
        return Optional.ofNullable(ruleEngineMsgType);
    }

}
