package com.jnks.iot.server.common.data.sync.vc;

import lombok.AllArgsConstructor;
import lombok.Data;
import com.jnks.iot.server.common.data.sync.ie.EntityExportData;

@Data
@AllArgsConstructor
public class EntityDataDiff {
    private EntityExportData<?> currentVersion;
    private EntityExportData<?> otherVersion;
}
