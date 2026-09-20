package com.jnks.iot.server.service.entitiy.widgets.type;

import com.jnks.iot.server.common.data.widget.WidgetTypeDetails;
import com.jnks.iot.server.service.entitiy.SimpleTbEntityService;
import com.jnks.iot.server.service.security.model.SecurityUser;

/**
 * 部件类型业务层契约，在通用保存/删除之外支持按 FQN 更新已有部件。
 */
public interface TbWidgetTypeService extends SimpleTbEntityService<WidgetTypeDetails> {

    /** 保存部件类型；{@code updateExistingByFqn} 为 true 时按 FQN 覆盖已有记录。 */
    WidgetTypeDetails save(WidgetTypeDetails widgetTypeDetails, boolean updateExistingByFqn, SecurityUser user) throws Exception;

}
