package org.thingsboard.server.service.entitiy.widgets.bundle;

import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.id.WidgetTypeId;
import org.thingsboard.server.common.data.id.WidgetsBundleId;
import org.thingsboard.server.common.data.widget.WidgetsBundle;
import org.thingsboard.server.service.entitiy.SimpleTbEntityService;

import java.util.List;

/**
 * 部件包业务层契约：保存/删除，以及更新包内部件列表。
 */
public interface TbWidgetsBundleService extends SimpleTbEntityService<WidgetsBundle> {

    /** 按部件类型 ID 列表更新部件包内容。 */
    void updateWidgetsBundleWidgetTypes(WidgetsBundleId widgetsBundleId, List<WidgetTypeId> widgetTypeIds, User user) throws Exception;

    /** 按部件 FQN 列表更新部件包内容。 */
    void updateWidgetsBundleWidgetFqns(WidgetsBundleId widgetsBundleId, List<String> widgetFqns, User user) throws Exception;


}
