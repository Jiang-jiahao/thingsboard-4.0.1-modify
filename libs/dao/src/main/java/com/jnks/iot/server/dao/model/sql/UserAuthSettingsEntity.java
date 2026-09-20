package com.jnks.iot.server.dao.model.sql;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.id.UserAuthSettingsId;
import com.jnks.iot.server.common.data.id.UserId;
import com.jnks.iot.server.common.data.security.UserAuthSettings;
import com.jnks.iot.server.common.data.security.model.mfa.account.AccountTwoFaSettings;
import com.jnks.iot.server.dao.model.BaseEntity;
import com.jnks.iot.server.dao.model.BaseSqlEntity;
import com.jnks.iot.server.dao.model.ModelConstants;
import com.jnks.iot.server.dao.util.mapping.JsonConverter;

import java.util.UUID;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@Entity
@Table(name = ModelConstants.USER_AUTH_SETTINGS_TABLE_NAME)
public class UserAuthSettingsEntity extends BaseSqlEntity<UserAuthSettings> implements BaseEntity<UserAuthSettings> {

    @Column(name = ModelConstants.USER_AUTH_SETTINGS_USER_ID_PROPERTY, nullable = false, unique = true)
    private UUID userId;
    @Convert(converter = JsonConverter.class)
    @Column(name = ModelConstants.USER_AUTH_SETTINGS_TWO_FA_SETTINGS)
    private JsonNode twoFaSettings;

    public UserAuthSettingsEntity(UserAuthSettings userAuthSettings) {
        if (userAuthSettings.getId() != null) {
            this.setId(userAuthSettings.getId().getId());
        }
        this.setCreatedTime(userAuthSettings.getCreatedTime());
        if (userAuthSettings.getUserId() != null) {
            this.userId = userAuthSettings.getUserId().getId();
        }
        if (userAuthSettings.getTwoFaSettings() != null) {
            this.twoFaSettings = JacksonUtil.valueToTree(userAuthSettings.getTwoFaSettings());
        }
    }

    @Override
    public UserAuthSettings toData() {
        UserAuthSettings userAuthSettings = new UserAuthSettings();
        userAuthSettings.setId(new UserAuthSettingsId(id));
        userAuthSettings.setCreatedTime(createdTime);
        if (userId != null) {
            userAuthSettings.setUserId(new UserId(userId));
        }
        if (twoFaSettings != null) {
            userAuthSettings.setTwoFaSettings(JacksonUtil.treeToValue(twoFaSettings, AccountTwoFaSettings.class));
        }
        return userAuthSettings;
    }

}
