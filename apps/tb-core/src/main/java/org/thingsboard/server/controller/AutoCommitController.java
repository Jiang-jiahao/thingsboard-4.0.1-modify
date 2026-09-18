package org.thingsboard.server.controller;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.id.EntityId;
import org.thingsboard.server.service.sync.vc.EntitiesVersionControlService;

import java.util.UUID;

/**
 * 版本控制「自动提交」辅助基类，本身不暴露 REST 路径。
 * <p>
 * 资产 / 设备 / 仪表盘等实体保存后，子类可调用 {@link #autoCommit}，把变更提交到租户
 * 配置的 Git 仓库。真正实现在 {@link EntitiesVersionControlService}；规则引擎节点
 * 上没有该 Bean 时会返回失败 Future。
 *
 * @see EntitiesVersionControlService
 */
public class AutoCommitController extends BaseController {

    @Autowired
    private EntitiesVersionControlService vcService;

    /**
     * 按租户自动提交设置，把单个实体的当前版本提交到 Git。
     * 未配置 VC 服务（例如 Rule Engine 进程）时返回失败 Future。
     */
    protected ListenableFuture<UUID> autoCommit(User user, EntityId entityId) throws Exception {
        if (vcService != null) {
            return vcService.autoCommit(user, entityId);
        } else {
            // We do not support auto-commit for rule engine
            return Futures.immediateFailedFuture(new RuntimeException("Operation not supported!"));
        }
    }


}
