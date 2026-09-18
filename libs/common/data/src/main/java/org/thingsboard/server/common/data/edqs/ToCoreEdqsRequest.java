package org.thingsboard.server.common.data.edqs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ToCoreEdqsRequest {

    private EdqsSyncRequest syncRequest;
    private Boolean apiEnabled;

    @JsonIgnore
    public ToCoreEdqsMsg toInternalMsg() {
        return new ToCoreEdqsMsg(syncRequest, apiEnabled);
    }

}
