package com.jnks.iot.rule.engine.data;

import lombok.Data;
import com.jnks.iot.server.common.data.relation.EntitySearchDirection;
import com.jnks.iot.server.common.data.relation.RelationEntityTypeFilter;

import java.util.List;

@Data
public class RelationsQuery {

    private EntitySearchDirection direction;
    private int maxLevel = 1;
    private List<RelationEntityTypeFilter> filters;
    private boolean fetchLastLevelOnly = false;
}
