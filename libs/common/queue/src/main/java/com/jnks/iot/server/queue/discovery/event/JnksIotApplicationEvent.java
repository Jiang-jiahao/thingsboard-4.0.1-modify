package com.jnks.iot.server.queue.discovery.event;

import lombok.Getter;
import lombok.ToString;
import org.springframework.context.ApplicationEvent;

import java.util.concurrent.atomic.AtomicInteger;

@ToString
public class JnksIotApplicationEvent extends ApplicationEvent {

    private static final long serialVersionUID = 3884264064887765146L;

    private static final AtomicInteger sequence = new AtomicInteger();

    @Getter
    private final int sequenceNumber;

    public JnksIotApplicationEvent(Object source) {
        super(source);
        sequenceNumber = sequence.incrementAndGet();
    }

}
