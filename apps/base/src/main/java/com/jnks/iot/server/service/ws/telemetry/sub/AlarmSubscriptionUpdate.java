package com.jnks.iot.server.service.ws.telemetry.sub;

import lombok.Getter;
import lombok.ToString;
import com.jnks.iot.server.common.data.alarm.AlarmInfo;
import com.jnks.iot.server.service.subscription.SubscriptionErrorCode;

@ToString
public class AlarmSubscriptionUpdate {

    @Getter
    private int errorCode;
    @Getter
    private String errorMsg;
    @Getter
    private AlarmInfo alarm;
    @Getter
    private boolean alarmDeleted;

    public AlarmSubscriptionUpdate(AlarmInfo alarm) {
        this(alarm, false);
    }

    public AlarmSubscriptionUpdate(AlarmInfo alarm, boolean alarmDeleted) {
        super();
        this.alarm = alarm;
        this.alarmDeleted = alarmDeleted;
    }

    public AlarmSubscriptionUpdate(SubscriptionErrorCode errorCode) {
        this(errorCode, null);
    }

    public AlarmSubscriptionUpdate(SubscriptionErrorCode errorCode, String errorMsg) {
        super();
        this.errorCode = errorCode.getCode();
        this.errorMsg = errorMsg != null ? errorMsg : errorCode.getDefaultMsg();
    }

}