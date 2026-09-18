package org.thingsboard.server.common.data.edqs;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ToCoreEdqsMsg {

    private EdqsSyncRequest syncRequest;
    private Boolean apiEnabled;

}
