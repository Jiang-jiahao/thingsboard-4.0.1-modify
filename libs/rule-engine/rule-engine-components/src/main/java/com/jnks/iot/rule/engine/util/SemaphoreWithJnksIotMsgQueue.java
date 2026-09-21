package com.jnks.iot.rule.engine.util;

import com.google.common.util.concurrent.ListenableFuture;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.common.util.DonAsynchron;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.msg.JnksIotMsg;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;

/**
 * A utility class designed to manage a queue of messages for a specific entity, ensuring that
 * message processing is synchronized on a per-entity basis. This is achieved through the use of a semaphore,
 * allowing only one message at a time to be processed for each entity ID, thus preventing race conditions
 * and ensuring thread-safe operations.
 * <p>
 * This class is especially useful in scenarios where the order of message processing and
 * resource access synchronization are crucial, such as updating caches or databases in a concurrent environment.
 */
@Data
@Slf4j
public class SemaphoreWithJnksIotMsgQueue {

    private final EntityId entityId;
    private final Semaphore semaphore = new Semaphore(1);
    private final Queue<JnksIotMsgJnksIotContextBiFunction> queue = new ConcurrentLinkedQueue<>();

    /**
     * Adds a message to the queue for asynchronous processing and attempts to process the queue if possible.
     * This method is thread-safe and ensures that messages are processed in the order they were added,
     * with each message for a specific entity being processed one at a time due to the semaphore control.
     *
     * @param msg                   The message to be processed.
     * @param ctx                   The context in which the message should be processed.
     * @param msgProcessingFunction The function that defines how the message will be processed.
     */
    public void addToQueueAndTryProcess(JnksIotMsg msg, JnksIotContext ctx, BiFunction<JnksIotContext, JnksIotMsg, ListenableFuture<JnksIotMsg>> msgProcessingFunction) {
        queue.add(new JnksIotMsgJnksIotContextBiFunction(msg, ctx, msgProcessingFunction));
        tryProcessQueue();
    }

    /**
     * Attempts to process the next message in the queue. If the semaphore is available (indicating
     * that no other message for the same entity is currently being processed), this method will
     * acquire the semaphore and start processing the message. If the semaphore is not available,
     * this method will return immediately, ensuring that messages are processed sequentially
     * for each entity.
     * <p>
     * This method is automatically called after adding a message to the queue to ensure
     * that the queue is processed promptly.
     */
    private void tryProcessQueue() {
        while (!queue.isEmpty()) {
            // The semaphore have to be acquired before EACH poll and released before NEXT poll.
            // Otherwise, some message will remain unprocessed in queue
            if (!semaphore.tryAcquire()) {
                return;
            }
            JnksIotMsgJnksIotContextBiFunction jnksIotMsgJnksIotContext = null;
            try {
                jnksIotMsgJnksIotContext = queue.poll();
                if (jnksIotMsgJnksIotContext == null) {
                    semaphore.release();
                    continue;
                }
                final JnksIotMsg msg = jnksIotMsgJnksIotContext.msg();
                if (!msg.getCallback().isMsgValid()) {
                    log.trace("[{}] Skipping non-valid message [{}]", entityId, msg);
                    semaphore.release();
                    continue;
                }
                //DO PROCESSING
                final JnksIotContext ctx = jnksIotMsgJnksIotContext.ctx();
                final ListenableFuture<JnksIotMsg> resultMsgFuture = jnksIotMsgJnksIotContext.biFunction().apply(ctx, msg);
                DonAsynchron.withCallback(resultMsgFuture, resultMsg -> {
                    try {
                        ctx.tellSuccess(resultMsg);
                    } finally {
                        semaphore.release();
                        tryProcessQueue();
                    }
                }, t -> {
                    try {
                        ctx.tellFailure(msg, t);
                    } finally {
                        semaphore.release();
                        tryProcessQueue();
                    }
                }, ctx.getDbCallbackExecutor());
            } catch (Throwable t) {
                semaphore.release();
                if (jnksIotMsgJnksIotContext == null) { // if no message polled, the loop become infinite, will throw exception
                    log.error("[{}] Failed to process JnksIotMsgJnksIotContext queue", entityId, t);
                    throw t;
                }
                JnksIotMsg msg = jnksIotMsgJnksIotContext.msg();
                JnksIotContext ctx = jnksIotMsgJnksIotContext.ctx();
                log.debug("[{}] Failed to process message: {}", entityId, msg, t);
                ctx.tellFailure(msg, t); // you are not allowed to throw here, because queue will remain unprocessed
                continue; // We are probably the last who process the queue. We have to continue poll until get successful callback or queue is empty
            }
            break; //submitted async exact one task. next poll will try on callback
        }
    }

    /**
     * A utility record to hold the tuple of a {@link JnksIotMsg}, {@link JnksIotContext}, and the message processing function.
     * This facilitates passing these three elements as a single object within the queue.
     */
    private record JnksIotMsgJnksIotContextBiFunction(JnksIotMsg msg, JnksIotContext ctx,
                                            BiFunction<JnksIotContext, JnksIotMsg, ListenableFuture<JnksIotMsg>> biFunction) {
    }

}
