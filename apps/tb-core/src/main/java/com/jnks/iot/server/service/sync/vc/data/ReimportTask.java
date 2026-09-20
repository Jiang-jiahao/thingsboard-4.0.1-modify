package com.jnks.iot.server.service.sync.vc.data;

import lombok.Data;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;
import com.jnks.iot.server.common.data.sync.ie.EntityImportSettings;

@Data
public class ReimportTask {

    private final EntityExportData data;
    private final EntityImportSettings settings;

}
