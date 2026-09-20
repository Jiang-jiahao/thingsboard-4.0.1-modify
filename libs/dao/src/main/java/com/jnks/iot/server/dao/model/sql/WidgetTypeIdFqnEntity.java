package com.jnks.iot.server.dao.model.sql;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

@Data
@AllArgsConstructor
public class WidgetTypeIdFqnEntity {
    private UUID id;
    private String fqn;
}
