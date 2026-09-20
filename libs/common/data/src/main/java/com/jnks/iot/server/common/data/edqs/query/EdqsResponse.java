package com.jnks.iot.server.common.data.edqs.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.jnks.iot.server.common.data.page.PageData;
import com.jnks.iot.server.common.data.query.EntityData;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class EdqsResponse {

    private PageData<EntityData> entityDataQueryResult;
    private Long entityCountQueryResult;
    private String error;

}
