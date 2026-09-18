package org.thingsboard.server.common.data.query;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.thingsboard.server.common.data.alarm.Alarm;
import org.thingsboard.server.common.data.alarm.AlarmInfo;
import org.thingsboard.server.common.data.id.EntityId;

import java.util.HashMap;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
public class AlarmData extends AlarmInfo {

    private static final long serialVersionUID = -7042457913823369638L;

    @Getter
    private final EntityId entityId;
    @Getter
    private final Map<EntityKeyType, Map<String, TsValue>> latest;

    public AlarmData(AlarmInfo main, AlarmData prototype) {
        super(main);
        this.entityId = prototype.entityId;
        this.latest = new HashMap<>();
        this.latest.putAll(prototype.getLatest());
    }

    public AlarmData(Alarm alarm, EntityId entityId) {
        super(alarm);
        this.entityId = entityId;
        this.latest = new HashMap<>();
    }
}
