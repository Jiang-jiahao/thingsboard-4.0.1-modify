package com.jnks.iot.server.actors;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.server.common.msg.JnksIotActorMsg;
import com.jnks.iot.server.common.msg.JnksIotActorStopReason;

@Slf4j
public class TestRootActor extends AbstractJnksIotActor {

    @Getter
    private final JnksIotActorId actorId;
    @Getter
    private final ActorTestCtx testCtx;

    private boolean initialized;
    private long sum;
    private int count;

    public TestRootActor(JnksIotActorId actorId, ActorTestCtx testCtx) {
        this.actorId = actorId;
        this.testCtx = testCtx;
    }

    @Override
    public void init(JnksIotActorCtx ctx) throws JnksIotActorException {
        super.init(ctx);
        initialized = true;
    }

    @Override
    public boolean process(JnksIotActorMsg msg) {
        if (initialized) {
            int value = ((IntJnksIotActorMsg) msg).getValue();
            sum += value;
            count += 1;
            // 当执行次数达到期望的执行次数的时候，进入设置结果，并countDown
            if (count == testCtx.getExpectedInvocationCount()) {
                testCtx.getActual().set(sum);
                testCtx.getInvocationCount().addAndGet(count);
                sum = 0;
                count = 0;
                testCtx.getLatch().countDown();
            }
        }
        return true;
    }

    @Override
    public void destroy(JnksIotActorStopReason stopReason, Throwable cause) {

    }

    public static class TestRootActorCreator implements JnksIotActorCreator {

        private final JnksIotActorId actorId;
        private final ActorTestCtx testCtx;

        public TestRootActorCreator(JnksIotActorId actorId, ActorTestCtx testCtx) {
            this.actorId = actorId;
            this.testCtx = testCtx;
        }

        @Override
        public JnksIotActorId createActorId() {
            return actorId;
        }

        @Override
        public JnksIotActor createActor() {
            return new TestRootActor(actorId, testCtx);
        }
    }
}
