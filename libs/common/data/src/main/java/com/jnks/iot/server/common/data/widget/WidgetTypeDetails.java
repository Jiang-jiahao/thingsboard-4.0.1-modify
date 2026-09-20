package com.jnks.iot.server.common.data.widget;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.jnks.iot.server.common.data.ExportableEntity;
import com.jnks.iot.server.common.data.HasImage;
import com.jnks.iot.server.common.data.HasName;
import com.jnks.iot.server.common.data.HasTenantId;
import com.jnks.iot.server.common.data.ResourceExportData;
import com.jnks.iot.server.common.data.id.WidgetTypeId;
import com.jnks.iot.server.common.data.validation.Length;
import com.jnks.iot.server.common.data.validation.NoXss;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@JsonPropertyOrder({"fqn", "name", "deprecated", "image", "description", "descriptor", "externalId", "resources"})
public class WidgetTypeDetails extends WidgetType implements HasName, HasTenantId, HasImage, ExportableEntity<WidgetTypeId> {

    @Schema(description = "Relative or external image URL. Replaced with image data URL (Base64) in case of relative URL and 'inlineImages' option enabled.")
    private String image;
    @NoXss
    @Length(fieldName = "description", max = 1024)
    @Schema(description = "Description of the widget")
    private String description;
    @NoXss
    @Schema(description = "Tags of the widget type")
    private String[] tags;

    private WidgetTypeId externalId;

    private List<ResourceExportData> resources;

    public WidgetTypeDetails() {
        super();
    }

    public WidgetTypeDetails(WidgetTypeId id) {
        super(id);
    }

    public WidgetTypeDetails(BaseWidgetType baseWidgetType) {
        super(baseWidgetType);
    }

    public WidgetTypeDetails(WidgetTypeDetails widgetTypeDetails) {
        super(widgetTypeDetails);
        this.image = widgetTypeDetails.getImage();
        this.description = widgetTypeDetails.getDescription();
        this.tags = widgetTypeDetails.getTags();
        this.externalId = widgetTypeDetails.getExternalId();
        this.resources = widgetTypeDetails.getResources() != null ? new ArrayList<>(widgetTypeDetails.getResources()) : null;
    }

}
