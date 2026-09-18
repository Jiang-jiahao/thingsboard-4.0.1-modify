package org.thingsboard.server.cache.mobile.secret;

import lombok.Data;

@Data
public class MobileSecretEvictEvent {

    private final String secret;

}
