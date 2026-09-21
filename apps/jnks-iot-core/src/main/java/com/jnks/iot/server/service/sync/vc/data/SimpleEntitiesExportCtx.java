package com.jnks.iot.server.service.sync.vc.data;

import lombok.Getter;
import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.sync.ie.EntityExportSettings;
import com.jnks.iot.server.common.data.sync.vc.request.create.SingleEntityVersionCreateRequest;

public class SimpleEntitiesExportCtx extends EntitiesExportCtx<SingleEntityVersionCreateRequest> {

    @Getter
    private final EntityExportSettings settings;

    public SimpleEntitiesExportCtx(User user, CommitGitRequest commit, SingleEntityVersionCreateRequest request) {
        this(user, commit, request, request != null ? buildExportSettings(request.getConfig()) : null);
    }

    public SimpleEntitiesExportCtx(User user, CommitGitRequest commit, SingleEntityVersionCreateRequest request, EntityExportSettings settings) {
        super(user, commit, request);
        this.settings = settings;
    }
}
