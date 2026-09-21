package com.jnks.iot.server.actors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FailedToInitActor extends TestRootActor {

    /**
     * 允许重试的次数
     */
    int retryAttempts;
    int retryDelay;
    int attempts = 0;

    public FailedToInitActor(JnksIotActorId actorId, ActorTestCtx testCtx, int retryAttempts, int retryDelay) {
        super(actorId, testCtx);
        this.retryAttempts = retryAttempts;
        this.retryDelay = retryDelay;
    }

    @Override
    public void init(JnksIotActorCtx ctx) throws JnksIotActorException {
        if (attempts < retryAttempts) {
            attempts++;
            throw new JnksIotActorException("Test attempt", new RuntimeException());
        } else {
            super.init(ctx);
        }
    }

    @Override
    public InitFailureStrategy onInitFailure(int attempt, Throwable t) {
        return InitFailureStrategy.retryWithDelay(retryDelay);
    }

    public static class FailedToInitActorCreator implements JnksIotActorCreator {

        private final JnksIotActorId actorId;
        private final ActorTestCtx testCtx;
        private final int retryAttempts;
        /**
         * 初始化失败后延迟多久重新执行初始化。单位：毫秒
         */
        private final int retryDelay;

        public FailedToInitActorCreator(JnksIotActorId actorId, ActorTestCtx testCtx, int retryAttempts, int retryDelay) {
            this.actorId = actorId;
            this.testCtx = testCtx;
            this.retryAttempts = retryAttempts;
            this.retryDelay = retryDelay;
        }

        @Override
        public JnksIotActorId createActorId() {
            return actorId;
        }

        @Override
        public JnksIotActor createActor() {
            return new FailedToInitActor(actorId, testCtx, retryAttempts, retryDelay);
        }
    }
}
