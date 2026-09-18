package org.thingsboard.server.common.data.id;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public class UserCredentialsId extends UUIDBased {

    @JsonCreator
    public UserCredentialsId(@JsonProperty("id")UUID id){
        super(id);
    }
}
