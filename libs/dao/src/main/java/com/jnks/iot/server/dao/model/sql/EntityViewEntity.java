package com.jnks.iot.server.dao.model.sql;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.EntityView;
import com.jnks.iot.server.dao.model.ModelConstants;

@Data
@EqualsAndHashCode(callSuper = true)
@Entity
@Table(name = ModelConstants.ENTITY_VIEW_TABLE_NAME)
public class EntityViewEntity extends AbstractEntityViewEntity<EntityView> {

    public EntityViewEntity() {
        super();
    }

    public EntityViewEntity(EntityView entityView) {
        super(entityView);
    }

    @Override
    public EntityView toData() {
        return super.toEntityView();
    }
}
