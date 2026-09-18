package org.thingsboard.server.common.data;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.EnumSet;
import java.util.List;

/**
 * @author Andrew Shvayka
 */
public enum EntityType {
    TENANT(1),
    CUSTOMER(2),
    USER(3, "tb_user"),
    DASHBOARD(4),
    ASSET(5),
    DEVICE(6),
    ALARM(7),
    RULE_CHAIN(11),
    RULE_NODE(12),
    ENTITY_VIEW(15) {
        // backward compatibility for TbOriginatorTypeSwitchNode to return correct rule node connection.
        @Override
        public String getNormalName() {
            return "Entity View";
        }
    },
    WIDGETS_BUNDLE(16),
    WIDGET_TYPE(17),
    TENANT_PROFILE(20),
    DEVICE_PROFILE(21),
    ASSET_PROFILE(22),
    API_USAGE_STATE(23),
    TB_RESOURCE(24, "resource"),
    OTA_PACKAGE(25),
    RPC(27),
    QUEUE(28),
    NOTIFICATION_TARGET(29),
    NOTIFICATION_TEMPLATE(30),
    NOTIFICATION_REQUEST(31),
    NOTIFICATION(32),
    NOTIFICATION_RULE(33),
    QUEUE_STATS(34),
    OAUTH2_CLIENT(35),
    DOMAIN(36),
    MOBILE_APP(37),
    MOBILE_APP_BUNDLE(38),
    CALCULATED_FIELD(39),
    CALCULATED_FIELD_LINK(40);

    @Getter
    private final int protoNumber; // Corresponds to EntityTypeProto
    @Getter
    private final String tableName;
    @Getter
    private final String normalName = StringUtils.capitalize(StringUtils.removeStart(name(), "TB_")
            .toLowerCase().replaceAll("_", " "));

    public static final List<String> NORMAL_NAMES = EnumSet.allOf(EntityType.class).stream()
            .map(EntityType::getNormalName).toList();

    EntityType(int protoNumber) {
        this.protoNumber = protoNumber;
        this.tableName = name().toLowerCase();
    }

    EntityType(int protoNumber, String tableName) {
        this.protoNumber = protoNumber;
        this.tableName = tableName;
    }

    public boolean isOneOf(EntityType... types) {
        if (types == null) {
            return false;
        }
        for (EntityType type : types) {
            if (this == type) {
                return true;
            }
        }
        return false;
    }

}
