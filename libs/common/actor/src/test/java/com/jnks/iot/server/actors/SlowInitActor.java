package com.jnks.iot.server.actors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SlowInitActor extends TestRootActor {

    public SlowInitActor(JnksIotActorId actorId, ActorTestCtx testCtx) {
        super(actorId, testCtx);
    }

    @Override
    public void init(JnksIotActorCtx ctx) throws JnksIotActorException {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        super.init(ctx);
    }

    public static class SlowInitActorCreator implements JnksIotActorCreator {

        private final JnksIotActorId actorId;
        private final ActorTestCtx testCtx;

        public SlowInitActorCreator(JnksIotActorId actorId, ActorTestCtx testCtx) {
            this.actorId = actorId;
            this.testCtx = testCtx;
        }

        @Override
        public JnksIotActorId createActorId() {
            return actorId;
        }

        @Override
        public JnksIotActor createActor() {
            return new SlowInitActor(actorId, testCtx);
        }
    }
}
