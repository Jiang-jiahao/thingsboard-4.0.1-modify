package org.thingsboard.server.service.entitiy.widgets.type;

import org.thingsboard.server.common.data.widget.WidgetTypeDetails;
import org.thingsboard.server.service.entitiy.SimpleTbEntityService;
import org.thingsboard.server.service.security.model.SecurityUser;

/**
 * 部件类型业务层契约，在通用保存/删除之外支持按 FQN 更新已有部件。
 */
public interface TbWidgetTypeService extends SimpleTbEntityService<WidgetTypeDetails> {

    /** 保存部件类型；{@code updateExistingByFqn} 为 true 时按 FQN 覆盖已有记录。 */
    WidgetTypeDetails save(WidgetTypeDetails widgetTypeDetails, boolean updateExistingByFqn, SecurityUser user) throws Exception;

}
