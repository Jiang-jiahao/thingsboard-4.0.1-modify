package com.jnks.iot.server.common.data.widget;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.id.WidgetTypeId;
import com.jnks.iot.server.common.data.id.WidgetsBundleId;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WidgetsBundleWidget {

    private WidgetsBundleId widgetsBundleId;
    private WidgetTypeId widgetTypeId;
    private int widgetTypeOrder;

}
