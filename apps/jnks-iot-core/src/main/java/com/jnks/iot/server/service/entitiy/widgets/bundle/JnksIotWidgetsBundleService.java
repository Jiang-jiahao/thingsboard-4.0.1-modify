package com.jnks.iot.server.service.entitiy.widgets.bundle;

import com.jnks.iot.server.common.data.User;
import com.jnks.iot.server.common.data.id.WidgetTypeId;
import com.jnks.iot.server.common.data.id.WidgetsBundleId;
import com.jnks.iot.server.common.data.widget.WidgetsBundle;
import com.jnks.iot.server.service.entitiy.SimpleJnksIotEntityService;

import java.util.List;

/**
 * 部件包业务层契约：保存/删除，以及更新包内部件列表。
 */
public interface JnksIotWidgetsBundleService extends SimpleJnksIotEntityService<WidgetsBundle> {

    /** 按部件类型 ID 列表更新部件包内容。 */
    void updateWidgetsBundleWidgetTypes(WidgetsBundleId widgetsBundleId, List<WidgetTypeId> widgetTypeIds, User user) throws Exception;

    /** 按部件 FQN 列表更新部件包内容。 */
    void updateWidgetsBundleWidgetFqns(WidgetsBundleId widgetsBundleId, List<String> widgetFqns, User user) throws Exception;


}
