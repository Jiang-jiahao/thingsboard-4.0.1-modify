package com.jnks.iot.server.common.data.sync.ie;

import lombok.Data;
import com.jnks.iot.server.common.data.EntityType;
import com.jnks.iot.server.common.data.ExportableEntity;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.util.ThrowingRunnable;

@Data
public class EntityImportResult<E extends ExportableEntity<? extends EntityId>> {

    private E savedEntity;
    private E oldEntity;
    private EntityType entityType;

    private ThrowingRunnable saveReferencesCallback = () -> {};
    private ThrowingRunnable sendEventsCallback = () -> {};

    private boolean updatedAllExternalIds = true;

    private boolean created;
    private boolean updated;
    private boolean updatedRelatedEntities;

    public void addSaveReferencesCallback(ThrowingRunnable callback) {
        this.saveReferencesCallback = this.saveReferencesCallback.andThen(callback);
    }

    public void addSendEventsCallback(ThrowingRunnable callback) {
        this.sendEventsCallback = this.sendEventsCallback.andThen(callback);
    }

}
