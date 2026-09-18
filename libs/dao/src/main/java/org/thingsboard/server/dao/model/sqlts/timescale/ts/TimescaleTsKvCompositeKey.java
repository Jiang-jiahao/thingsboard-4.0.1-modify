package org.thingsboard.server.dao.model.sqlts.timescale.ts;

import jakarta.persistence.Transient;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimescaleTsKvCompositeKey implements Serializable {

    @Transient
    private static final long serialVersionUID = -4089175869616037523L;

    private UUID entityId;
    private int key;
    private long ts;
} 
