package com.jnks.iot.server.dao.sql.widget;

import org.springframework.data.jpa.repository.JpaRepository;
import com.jnks.iot.server.dao.model.sql.WidgetsBundleWidgetCompositeKey;
import com.jnks.iot.server.dao.model.sql.WidgetsBundleWidgetEntity;

import java.util.List;
import java.util.UUID;

public interface WidgetsBundleWidgetRepository extends JpaRepository<WidgetsBundleWidgetEntity, WidgetsBundleWidgetCompositeKey> {

    List<WidgetsBundleWidgetEntity> findAllByWidgetsBundleId(UUID widgetsBundleId);

}
