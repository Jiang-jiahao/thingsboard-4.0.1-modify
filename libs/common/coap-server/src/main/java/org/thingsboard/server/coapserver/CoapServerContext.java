package org.thingsboard.server.coapserver;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CoapServerContext {

    @Getter
    @Value("${coap.bind_address}")
    private String host;

    @Getter
    @Value("${coap.bind_port}")
    private Integer port;

    @Getter
    @Autowired(required = false)
    private TbCoapDtlsSettings dtlsSettings;

}
