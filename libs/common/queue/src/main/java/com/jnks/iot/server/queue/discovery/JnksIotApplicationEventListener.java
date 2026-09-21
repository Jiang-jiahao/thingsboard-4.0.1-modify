package com.jnks.iot.server.queue.discovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import com.jnks.iot.server.queue.discovery.event.JnksIotApplicationEvent;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * tb事件监听器抽象，继承了spring的ApplicationListener
 * 里面定义了一个事件处理的基本框架，由子类决定对哪个事件感兴趣
 * @param <T>
 */
public abstract class JnksIotApplicationEventListener<T extends JnksIotApplicationEvent> implements ApplicationListener<T> {

    private int lastProcessedSequenceNumber = Integer.MIN_VALUE;
    private final Lock seqNumberLock = new ReentrantLock();

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void onApplicationEvent(T event) {
        if (!filterJnksIotApplicationEvent(event)) {
            log.trace("Skipping event due to filter: {}", event);
            return;
        }
        boolean validUpdate = false;
        seqNumberLock.lock();
        try {
            if (event.getSequenceNumber() > lastProcessedSequenceNumber) {
                validUpdate = true;
                lastProcessedSequenceNumber = event.getSequenceNumber();
            }
        } finally {
            seqNumberLock.unlock();
        }
        if (validUpdate) {
            try {
                onJnksIotApplicationEvent(event);
            } catch (Exception e) {
                log.error("Failed to handle partition change event: {}", event, e);
            }
        } else {
            log.info("Application event ignored due to invalid sequence number ({} > {}). Event: {}", lastProcessedSequenceNumber, event.getSequenceNumber(), event);
        }
    }

    protected abstract void onJnksIotApplicationEvent(T event);

    protected boolean filterJnksIotApplicationEvent(T event) {
        return true;
    }

}
