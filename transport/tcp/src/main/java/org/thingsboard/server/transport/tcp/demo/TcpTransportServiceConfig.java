//package org.thingsboard.server.transport.tcp;
//
//import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.thingsboard.server.transport.tcp.TcpTransportContext;
//
//@Configuration
//@ConditionalOnExpression("'${service.type:null}'=='tb-transport' || ('${service.type:null}'=='monolith' && '${transport.api_enabled:true}'=='true' && '${transport.tcp.enabled}'=='true')")
//public class TcpTransportServiceConfig {
//
//    @Bean
//    public TcpTransportContext tcpTransportContext() {
//        return new TcpTransportContext();
//    }
//
//    @Bean
//    public TcpTransportConfiguration tcpTransportConfiguration() {
//        return new TcpTransportConfiguration();
//    }
//}