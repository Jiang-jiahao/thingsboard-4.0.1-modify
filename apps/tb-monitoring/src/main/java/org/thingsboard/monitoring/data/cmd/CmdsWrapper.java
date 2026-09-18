package org.thingsboard.monitoring.data.cmd;

import lombok.Data;

import java.util.List;

@Data
public class CmdsWrapper {

    private List<EntityDataCmd> entityDataCmds;

}
