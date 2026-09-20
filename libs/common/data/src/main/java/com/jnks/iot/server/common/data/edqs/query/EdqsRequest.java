package com.jnks.iot.server.common.data.edqs.query;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.query.EntityCountQuery;
import com.jnks.iot.server.common.data.query.EntityDataQuery;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class EdqsRequest {

    private EntityDataQuery entityDataQuery;
    private EntityCountQuery entityCountQuery;

}
