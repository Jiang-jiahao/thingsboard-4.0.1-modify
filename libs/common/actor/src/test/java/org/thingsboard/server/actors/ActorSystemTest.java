/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.actors;

import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.thingsboard.common.util.ThingsBoardExecutors;
import org.thingsboard.common.util.ThingsBoardThreadFactory;
import org.thingsboard.server.common.data.id.DeviceId;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class ActorSystemTest {

    public static final String ROOT_DISPATCHER = "root-dispatcher";
    private static final int _100K = 100 * 1024;
    public static final int TIMEOUT_AWAIT_MAX_SEC = 30;

    private volatile TbActorSystem actorSystem;
    private volatile ExecutorService submitPool;
    private ExecutorService executor;
    private int parallelism;

    @BeforeEach
    public void initActorSystem() {
        int cores = Runtime.getRuntime().availableProcessors();
        parallelism = Math.max(2, cores / 2);
        log.debug("parallelism {}", parallelism);
        TbActorSystemSettings settings = new TbActorSystemSettings(5, parallelism, 42);
        actorSystem = new DefaultTbActorSystem(settings);
        submitPool = Executors.newFixedThreadPool(parallelism, ThingsBoardThreadFactory.forName(getClass().getSimpleName() + "-submit-test-scope")); //order guaranteed
    }

    @AfterEach
    public void shutdownActorSystem() {
        actorSystem.stop();
        submitPool.shutdownNow();
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    /**
     * 测试1个actor 发送100K个消息
     * @throws InterruptedException 中断异常
     */
    @Test
    public void test1actorsAnd100KMessages() throws InterruptedException {
        executor = ThingsBoardExecutors.newWorkStealingPool(parallelism, getClass());
        actorSystem.createDispatcher(ROOT_DISPATCHER, executor);
        testActorsAndMessages(1, _100K, 1);
    }

    /**
     * 测试10个actor每个actor都发送100K个消息
     * @throws InterruptedException 中断异常
     */
    @Test
    public void test10actorsAnd100KMessages() throws InterruptedException {
        executor = ThingsBoardExecutors.newWorkStealingPool(parallelism, getClass());
        actorSystem.createDispatcher(ROOT_DISPATCHER, executor);
        testActorsAndMessages(10, _100K, 1);
    }

    /**
     * 单线程测试100K个actor每个actor都发送1个消息，并测试5次
     * @throws InterruptedException 中断异常
     */
    @Test
    public void test100KActorsAnd1Messages5timesSingleThread() throws InterruptedException {
        executor = Executors.newSingleThreadExecutor(ThingsBoardThreadFactory.forName(getClass().getSimpleName()));
        actorSystem.createDispatcher(ROOT_DISPATCHER, executor);
        testActorsAndMessages(_100K, 1, 5);
    }

    /**
     * 多线程测试100K个actor每个actor都发送1个消息，并测试5次
     * @throws InterruptedException 中断异常
     */
    @Test
    public void test100KActorsAnd1Messages5times() throws InterruptedException {
        executor = ThingsBoardExecutors.newWorkStealingPool(parallelism, getClass());
        actorSystem.createDispatcher(ROOT_DISPATCHER, executor);
        testActorsAndMessages(_100K, 1, 5);
    }

    /**
     * 多线程测试100K个actor每个actor都发送10个消息
     * @throws InterruptedException 中断异常
     */
    @Test
    public void test100KActorsAnd10Messages() throws InterruptedException {
        executor = ThingsBoardExecutors.newWorkStealingPool(parallelism, getClass());
        actorSystem.createDispatcher(ROOT_DISPATCHER, executor);
        testActorsAndMessages(_100K, 10, 1);
    }

    /**
     * 多线程测试1K个actor每个actor都发送1K个消息，并测试10次
     * @throws InterruptedException 中断异常
     */
    @Test
    public void test1KActorsAnd1KMessages() throws InterruptedException {
        executor = ThingsBoardExecutors.newWorkStealingPool(parallelism, getClass());
        actorSystem.createDispatcher(ROOT_DISPATCHER, executor);
        testActorsAndMessages(1000, 1000, 10);
    }

    /**
     * 多线程测试100K个actor每个actor都发送1K个消息，并测试10次
     * @throws InterruptedException 中断异常
     */
    @Test
    public void test100KActorsAnd100KMessages() throws InterruptedException {
        executor = ThingsBoardExecutors.newWorkStealingPool(parallelism, getClass());
        actorSystem.createDispatcher(ROOT_DISPATCHER, executor);
        testActorsAndMessages(_100K, 1000, 10);
    }

    /**
     * 测试当一个 Actor 被销毁后，它不会再收到任何新的消息。
     * @throws InterruptedException 中断异常
     */
    @Test
    public void testNoMessagesAfterDestroy() throws InterruptedException {
        executor = ThingsBoardExecutors.newWorkStealingPool(parallelism, getClass());
        actorSystem.createDispatcher(ROOT_DISPATCHER, executor);
        ActorTestCtx testCtx1 = getActorTestCtx(1);
        ActorTestCtx testCtx2 = getActorTestCtx(1);
        // 创建两个actor
        TbActorRef actorId1 = actorSystem.createRootActor(ROOT_DISPATCHER, new SlowInitActor.SlowInitActorCreator(
                new TbEntityActorId(new DeviceId(UUID.randomUUID())), testCtx1));
        TbActorRef actorId2 = actorSystem.createRootActor(ROOT_DISPATCHER, new SlowInitActor.SlowInitActorCreator(
                new TbEntityActorId(new DeviceId(UUID.randomUUID())), testCtx2));
        // 因为上面的actor初始化的很慢，延迟有500毫秒，所以actorSystem.stop(actorId1);会先执行。模拟向actor放数据的时候actor被关了的情况
        actorId1.tell(new IntTbActorMsg(42));
        actorId2.tell(new IntTbActorMsg(42));
        actorSystem.stop(actorId1);

        Assertions.assertTrue(testCtx2.getLatch().await(1, TimeUnit.SECONDS));
        Assertions.assertFalse(testCtx1.getLatch().await(1, TimeUnit.SECONDS));
    }

    /**
     * 验证 Actor 创建的单例性，只有一个actor会被创建
     * 它不关心创建这些 Actor 的工厂函数被调用了多少次，只关心最终新 Actor 实例的数量。
     * @throws InterruptedException 中断异常
     */
    @Test
    public void testOneActorCreated() throws InterruptedException {
        // 创建Dispatcher
        executor = ThingsBoardExecutors.newWorkStealingPool(parallelism, getClass());
        actorSystem.createDispatcher(ROOT_DISPATCHER, executor);
        ActorTestCtx testCtx1 = getActorTestCtx(1);
        ActorTestCtx testCtx2 = getActorTestCtx(1);
        assertThat(testCtx1.getLatch().getCount()).as("testCtx1 latch initial state").isEqualTo(1);
        assertThat(testCtx2.getLatch().getCount()).as("testCtx2 latch initial state").isEqualTo(1);
        // 创建actorId
        TbActorId actorId = new TbEntityActorId(new DeviceId(UUID.randomUUID()));
        // initLatch用来延迟执行SlowCreateActorCreator的创建，模拟在没有创建完成actor的时候，再次进行创建相同id的actor的场景
        final CountDownLatch initLatch = new CountDownLatch(1);
        final CountDownLatch actorsReadyLatch = new CountDownLatch(2);
        submitPool.submit(() -> {
            log.info("submit 1");
            actorSystem.createRootActor(ROOT_DISPATCHER, new SlowCreateActor.SlowCreateActorCreator(actorId, testCtx1, initLatch));
            actorsReadyLatch.countDown();
            log.info("done 1");
        });
        submitPool.submit(() -> {
            log.info("submit 2");
            actorSystem.createRootActor(ROOT_DISPATCHER, new SlowCreateActor.SlowCreateActorCreator(actorId, testCtx2, initLatch));
            actorsReadyLatch.countDown();
            log.info("done 2");
        });

        initLatch.countDown(); //replacement for Thread.wait(500) in the SlowCreateActorCreator
        Assertions.assertTrue(actorsReadyLatch.await(TIMEOUT_AWAIT_MAX_SEC, TimeUnit.SECONDS));
        log.info("actorsReadyLatch ok");
        actorSystem.tell(actorId, new IntTbActorMsg(42));

        // 等待创建，两个上下文中只有一个被初始化。不管是Ctx1还是Ctx2
        Awaitility.await("one of two actors latch zeroed").atMost(TIMEOUT_AWAIT_MAX_SEC, TimeUnit.SECONDS)
                        .until(() -> testCtx1.getLatch().getCount() + testCtx2.getLatch().getCount() == 1);
        // 防止虚假通过测试，这里让出cup（因为上面等待只能说明有一个actor被创建了，可能存在还有一个actor正在创建，所以等待一会）
        Thread.yield();
        if (testCtx1.getLatch().getCount() == 0) {
            // 如果是Ctx1被创建了，那么我们就等待Ctx2被创建，如果在等待期间没有创建成功，则表明验证成功，只有Ctx1被创建了
            assertThat(testCtx2.getLatch().await(100, TimeUnit.MILLISECONDS)).as("testCtx2 never latched").isFalse();
        } else {
            // 如果是Ctx2被创建了，那么我们就等待Ctx1被创建，如果在等待期间没有创建成功，则表明验证成功，只有Ctx2被创建了
            assertThat(testCtx1.getLatch().await(100, TimeUnit.MILLISECONDS)).as("testCtx1 never latched").isFalse();
        }

    }

    /**
     * 验证 Actor 创建的单例性，创建器只会调用一次
     * @throws InterruptedException 中断异常
     */
    @Test
    public void testActorCreatorCalledOnce() throws InterruptedException {
        // 创建Dispatcher
        executor = ThingsBoardExecutors.newWorkStealingPool(parallelism, getClass());
        actorSystem.createDispatcher(ROOT_DISPATCHER, executor);
        ActorTestCtx testCtx = getActorTestCtx(1);
        // 创建actorId
        TbActorId actorId = new TbEntityActorId(new DeviceId(UUID.randomUUID()));
        final int actorsCount = 1000;
        // initLatch用来延迟执行SlowCreateActorCreator的创建，模拟在没有创建完成actor的时候，再次进行创建相同id的actor的场景
        final CountDownLatch initLatch = new CountDownLatch(1);
        final CountDownLatch actorsReadyLatch = new CountDownLatch(actorsCount);
        for (int i = 0; i < actorsCount; i++) {
            submitPool.submit(() -> {
                actorSystem.createRootActor(ROOT_DISPATCHER, new SlowCreateActor.SlowCreateActorCreator(actorId, testCtx, initLatch));
                actorsReadyLatch.countDown();
            });
        }
        initLatch.countDown();
        Assertions.assertTrue(actorsReadyLatch.await(TIMEOUT_AWAIT_MAX_SEC, TimeUnit.SECONDS));

        actorSystem.tell(actorId, new IntTbActorMsg(42));

        Assertions.assertTrue(testCtx.getLatch().await(TIMEOUT_AWAIT_MAX_SEC, TimeUnit.SECONDS));
        // 创建actor会执行一次InvocationCount + 1，一个用于消息被处理的时候InvocationCount + 1
        Assertions.assertEquals(2, testCtx.getInvocationCount().get());
    }

    /**
     * 验证 Actor 初始化失败时的处理逻辑
     * @throws InterruptedException 中断异常
     */
    @Test
    public void testFailedInit() throws InterruptedException {
        executor = ThingsBoardExecutors.newWorkStealingPool(parallelism, getClass());
        actorSystem.createDispatcher(ROOT_DISPATCHER, executor);
        ActorTestCtx testCtx1 = getActorTestCtx(1);
        ActorTestCtx testCtx2 = getActorTestCtx(1);

        TbActorRef actorId1 = actorSystem.createRootActor(ROOT_DISPATCHER, new FailedToInitActor.FailedToInitActorCreator(
                new TbEntityActorId(new DeviceId(UUID.randomUUID())), testCtx1, 1, 3000)); // 1表示重试一次就初始化成功
        TbActorRef actorId2 = actorSystem.createRootActor(ROOT_DISPATCHER, new FailedToInitActor.FailedToInitActorCreator(
                new TbEntityActorId(new DeviceId(UUID.randomUUID())), testCtx2, 2, 1)); // 2表示重试两次就初始化成功

        actorId1.tell(new IntTbActorMsg(42));
        actorId2.tell(new IntTbActorMsg(42));
        // Actor1在2秒内未完成初始化
        Assertions.assertFalse(testCtx1.getLatch().await(2, TimeUnit.SECONDS));
        // Actor2在1秒内完成初始化，因为Actor2是1毫秒重试一次，所以Actor2在1秒内完成初始化
        Assertions.assertTrue(testCtx2.getLatch().await(1, TimeUnit.SECONDS));
        // Actor1在3秒内完成初始化，因为Actor1是2秒重试一次，所以Actor1在3秒内完成初始化
        Assertions.assertTrue(testCtx1.getLatch().await(3, TimeUnit.SECONDS));
    }


    /**
     * 测试多个actor并发处理消息
     * @param actorsCount 需要创建的actor数量
     * @param msgNumber 每个 Actor 要处理的消息总数
     * @param times 整个测试要重复执行的次数
     * @throws InterruptedException 中断异常
     */
    public void testActorsAndMessages(int actorsCount, int msgNumber, int times) throws InterruptedException {
        // 生成msgNumber条随机的消息
        Random random = new Random();
        int[] randomIntegers = new int[msgNumber];
        long sumTmp = 0;
        for (int i = 0; i < msgNumber; i++) {
            int tmp = random.nextInt();
            randomIntegers[i] = tmp;
            sumTmp += tmp;
        }
        // 计算消息和
        long expected = sumTmp;

        List<ActorTestCtx> testCtxes = new ArrayList<>();
        // 创建actorsCount个actor
        List<TbActorRef> actorRefs = new ArrayList<>();
        for (int actorIdx = 0; actorIdx < actorsCount; actorIdx++) {
            ActorTestCtx testCtx = getActorTestCtx(msgNumber);
            actorRefs.add(actorSystem.createRootActor(ROOT_DISPATCHER, new TestRootActor.TestRootActorCreator(
                    new TbEntityActorId(new DeviceId(UUID.randomUUID())), testCtx)));
            testCtxes.add(testCtx);
        }

        for (int t = 0; t < times; t++) {
            long start = System.nanoTime();
            // 并发的给所有的actor发送msgNumber条消息
            for (int i = 0; i < msgNumber; i++) {
                int tmp = randomIntegers[i];
                submitPool.execute(() -> actorRefs.forEach(actorId -> actorId.tell(new IntTbActorMsg(tmp))));
            }
            log.info("Submitted all messages");
            testCtxes.forEach(ctx -> {
                try {
                    // 等待actor执行完成
                    boolean success = ctx.getLatch().await(TIMEOUT_AWAIT_MAX_SEC, TimeUnit.SECONDS);
                    if (!success) {
                        log.warn("Failed: {}, {}", ctx.getActual().get(), ctx.getInvocationCount().get());
                    }
                    Assertions.assertTrue(success);
                    Assertions.assertEquals(expected, ctx.getActual().get());
                    Assertions.assertEquals(msgNumber, ctx.getInvocationCount().get());
                    ctx.clear();
                } catch (InterruptedException e) {
                    log.error("interrupted", e);
                }
            });
            long duration = System.nanoTime() - start;
            log.info("Time spend: {}ns ({} ms)", duration, TimeUnit.NANOSECONDS.toMillis(duration));
        }
    }

    private ActorTestCtx getActorTestCtx(int i) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicLong actual = new AtomicLong();
        AtomicInteger invocations = new AtomicInteger();
        return new ActorTestCtx(countDownLatch, invocations, i, actual);
    }
}
