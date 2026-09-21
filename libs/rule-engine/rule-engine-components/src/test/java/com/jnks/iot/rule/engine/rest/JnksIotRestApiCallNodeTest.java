package com.jnks.iot.rule.engine.rest;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.Arguments;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.rule.engine.AbstractRuleNodeUpgradeTest;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNode;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.server.common.data.id.DeviceId;
import com.jnks.iot.server.common.data.id.EntityId;
import com.jnks.iot.server.common.data.id.RuleChainId;
import com.jnks.iot.server.common.data.id.RuleNodeId;
import com.jnks.iot.server.common.data.msg.JnksIotMsgType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgDataType;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class JnksIotRestApiCallNodeTest extends AbstractRuleNodeUpgradeTest {

    @Spy
    private JnksIotRestApiCallNode restNode;

    @Mock
    private JnksIotContext ctx;

    private EntityId originator = new DeviceId(Uuids.timeBased());
    private JnksIotMsgMetaData metaData = new JnksIotMsgMetaData();

    private RuleChainId ruleChainId = new RuleChainId(Uuids.timeBased());
    private RuleNodeId ruleNodeId = new RuleNodeId(Uuids.timeBased());

    private HttpServer server;

    public void setupServer(String pattern, HttpRequestHandler handler) throws IOException {
        SocketConfig config = SocketConfig.custom().setSoReuseAddress(true).setTcpNoDelay(true).build();
        server = ServerBootstrap.bootstrap()
                .setSocketConfig(config)
                .registerHandler(pattern, handler)
                .create();
        server.start();
    }

    private void initWithConfig(JnksIotRestApiCallNodeConfiguration config) {
        try {
            JnksIotNodeConfiguration nodeConfiguration = new JnksIotNodeConfiguration(JacksonUtil.valueToTree(config));
            restNode = new JnksIotRestApiCallNode();
            restNode.init(ctx, nodeConfiguration);
        } catch (JnksIotNodeException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @AfterEach
    public void teardown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void deleteRequestWithoutBody() throws IOException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final String path = "/path/to/delete";
        setupServer("*", new HttpRequestHandler() {

            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context)
                    throws HttpException, IOException {
                try {
                    assertEquals(request.getRequestLine().getUri(), path, "Request path matches");
                    assertTrue(request.containsHeader("Foo"), "Custom header included");
                    assertEquals("Bar", request.getFirstHeader("Foo").getValue(), "Custom header value");
                    response.setStatusCode(200);
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Thread.sleep(1000L);
                            } catch (InterruptedException e) {
                                // ignore
                            } finally {
                                latch.countDown();
                            }
                        }
                    }).start();
                } catch (Exception e) {
                    System.out.println("Exception handling request: " + e.toString());
                    e.printStackTrace();
                    latch.countDown();
                }
            }
        });

        JnksIotRestApiCallNodeConfiguration config = new JnksIotRestApiCallNodeConfiguration().defaultConfiguration();
        config.setRequestMethod("DELETE");
        config.setHeaders(Collections.singletonMap("Foo", "Bar"));
        config.setIgnoreRequestBody(true);
        config.setRestEndpointUrlPattern(String.format("http://localhost:%d%s", server.getLocalPort(), path));
        initWithConfig(config);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(metaData)
                .dataType(JnksIotMsgDataType.JSON)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .ruleChainId(ruleChainId)
                .ruleNodeId(ruleNodeId)
                .build();
        restNode.onMsg(ctx, msg);

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Server handled request");

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<JnksIotMsgMetaData> metadataCaptor = ArgumentCaptor.forClass(JnksIotMsgMetaData.class);
        ArgumentCaptor<String> dataCaptor = ArgumentCaptor.forClass(String.class);
        verify(ctx).transformMsg(msgCaptor.capture(), metadataCaptor.capture(), dataCaptor.capture());

        assertNotSame(metaData, metadataCaptor.getValue());
        assertEquals(JnksIotMsg.EMPTY_JSON_OBJECT, dataCaptor.getValue());
    }

    @Test
    public void deleteRequestWithBody() throws IOException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final String path = "/path/to/delete";
        setupServer("*", new HttpRequestHandler() {

            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context)
                    throws HttpException, IOException {
                try {
                    assertEquals(path, request.getRequestLine().getUri(), "Request path matches");
                    assertTrue(request.containsHeader("Content-Type"), "Content-Type included");
                    assertEquals("application/json",
                            request.getFirstHeader("Content-Type").getValue(), "Content-Type value");
                    assertTrue(request.containsHeader("Content-Length"), "Content-Length included");
                    assertEquals("2",
                            request.getFirstHeader("Content-Length").getValue(), "Content-Length value");
                    assertTrue(request.containsHeader("Foo"), "Custom header included");
                    assertEquals("Bar", request.getFirstHeader("Foo").getValue(), "Custom header value");
                    response.setStatusCode(200);
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Thread.sleep(1000L);
                            } catch (InterruptedException e) {
                                // ignore
                            } finally {
                                latch.countDown();
                            }
                        }
                    }).start();
                } catch (Exception e) {
                    System.out.println("Exception handling request: " + e.toString());
                    e.printStackTrace();
                    latch.countDown();
                }
            }
        });

        JnksIotRestApiCallNodeConfiguration config = new JnksIotRestApiCallNodeConfiguration().defaultConfiguration();
        config.setRequestMethod("DELETE");
        config.setHeaders(Collections.singletonMap("Foo", "Bar"));
        config.setIgnoreRequestBody(false);
        config.setRestEndpointUrlPattern(String.format("http://localhost:%d%s", server.getLocalPort(), path));
        initWithConfig(config);

        JnksIotMsg msg = JnksIotMsg.newMsg()
                .type(JnksIotMsgType.POST_TELEMETRY_REQUEST)
                .originator(originator)
                .copyMetaData(metaData)
                .dataType(JnksIotMsgDataType.JSON)
                .data(JnksIotMsg.EMPTY_JSON_OBJECT)
                .ruleChainId(ruleChainId)
                .ruleNodeId(ruleNodeId)
                .build();
        restNode.onMsg(ctx, msg);

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Server handled request");

        ArgumentCaptor<JnksIotMsg> msgCaptor = ArgumentCaptor.forClass(JnksIotMsg.class);
        ArgumentCaptor<JnksIotMsgMetaData> metadataCaptor = ArgumentCaptor.forClass(JnksIotMsgMetaData.class);
        ArgumentCaptor<String> dataCaptor = ArgumentCaptor.forClass(String.class);
        verify(ctx).transformMsg(msgCaptor.capture(), metadataCaptor.capture(), dataCaptor.capture());

        assertNotSame(metaData, metadataCaptor.getValue());
        assertEquals(JnksIotMsg.EMPTY_JSON_OBJECT, dataCaptor.getValue());
    }

    private static Stream<Arguments> givenFromVersionAndConfig_whenUpgrade_thenVerifyHasChangesAndConfig() {
        return Stream.of(
                // config for version 2 with upgrade from version 0
                Arguments.of(0,
                        "{\"restEndpointUrlPattern\":\"http://localhost/api\",\"requestMethod\":\"POST\"," +
                                "\"useSimpleClientHttpFactory\":false,\"ignoreRequestBody\":false,\"enableProxy\":false," +
                                "\"useSystemProxyProperties\":false,\"proxyScheme\":null,\"proxyHost\":null,\"proxyPort\":0," +
                                "\"proxyUser\":null,\"proxyPassword\":null,\"readTimeoutMs\":0,\"maxParallelRequestsCount\":0," +
                                "\"headers\":{\"Content-Type\":\"application/json\"},\"useRedisQueueForMsgPersistence\":false," +
                                "\"trimQueue\":null,\"maxQueueSize\":null,\"credentials\":{\"type\":\"anonymous\"},\"trimDoubleQuotes\":false}",
                        true,
                        "{\"restEndpointUrlPattern\":\"http://localhost/api\",\"requestMethod\": \"POST\"," +
                                "\"useSimpleClientHttpFactory\": false,\"parseToPlainText\": false,\"ignoreRequestBody\": false," +
                                "\"enableProxy\": false,\"useSystemProxyProperties\": false,\"proxyScheme\": null,\"proxyHost\": null," +
                                "\"proxyPort\": 0,\"proxyUser\": null,\"proxyPassword\": null,\"readTimeoutMs\": 0," +
                                "\"maxParallelRequestsCount\": 0,\"headers\": {\"Content-Type\": \"application/json\"}," +
                                "\"credentials\": {\"type\": \"anonymous\"}," +
                                "\"maxInMemoryBufferSizeInKb\": 256}"),
                // config for version 2 with upgrade from version 1
                Arguments.of(1,
                        "{\"restEndpointUrlPattern\":\"http://localhost/api\",\"requestMethod\": \"POST\"," +
                                "\"useSimpleClientHttpFactory\": false,\"parseToPlainText\": false,\"ignoreRequestBody\": false," +
                                "\"enableProxy\": false,\"useSystemProxyProperties\": false,\"proxyScheme\": null,\"proxyHost\": null," +
                                "\"proxyPort\": 0,\"proxyUser\": null,\"proxyPassword\": null,\"readTimeoutMs\": 0," +
                                "\"maxParallelRequestsCount\": 0,\"headers\": {\"Content-Type\": \"application/json\"}," +
                                "\"useRedisQueueForMsgPersistence\": false,\"trimQueue\": null,\"maxQueueSize\": null," +
                                "\"credentials\": {\"type\": \"anonymous\"}}",
                        true,
                        "{\"restEndpointUrlPattern\":\"http://localhost/api\",\"requestMethod\": \"POST\"," +
                                "\"useSimpleClientHttpFactory\": false,\"parseToPlainText\": false,\"ignoreRequestBody\": false," +
                                "\"enableProxy\": false,\"useSystemProxyProperties\": false,\"proxyScheme\": null,\"proxyHost\": null," +
                                "\"proxyPort\": 0,\"proxyUser\": null,\"proxyPassword\": null,\"readTimeoutMs\": 0," +
                                "\"maxParallelRequestsCount\": 0,\"headers\": {\"Content-Type\": \"application/json\"}," +
                                "\"credentials\": {\"type\": \"anonymous\"}," +
                                "\"maxInMemoryBufferSizeInKb\": 256}"),
                // config for version 3 with upgrade from version 2
                Arguments.of(2,
                        "{\"restEndpointUrlPattern\":\"http://localhost/api\",\"requestMethod\": \"POST\"," +
                                "\"useSimpleClientHttpFactory\": false,\"parseToPlainText\": false,\"ignoreRequestBody\": false," +
                                "\"enableProxy\": false,\"useSystemProxyProperties\": false,\"proxyScheme\": null,\"proxyHost\": null," +
                                "\"proxyPort\": 0,\"proxyUser\": null,\"proxyPassword\": null,\"readTimeoutMs\": 0," +
                                "\"maxParallelRequestsCount\": 0,\"headers\": {\"Content-Type\": \"application/json\"}," +
                                "\"credentials\": {\"type\": \"anonymous\"}}",
                        true,
                        "{\"restEndpointUrlPattern\":\"http://localhost/api\",\"requestMethod\": \"POST\"," +
                                "\"useSimpleClientHttpFactory\": false,\"parseToPlainText\": false,\"ignoreRequestBody\": false," +
                                "\"enableProxy\": false,\"useSystemProxyProperties\": false,\"proxyScheme\": null,\"proxyHost\": null," +
                                "\"proxyPort\": 0,\"proxyUser\": null,\"proxyPassword\": null,\"readTimeoutMs\": 0," +
                                "\"maxParallelRequestsCount\": 0,\"headers\": {\"Content-Type\": \"application/json\"}," +
                                "\"credentials\": {\"type\": \"anonymous\"}," +
                                "\"maxInMemoryBufferSizeInKb\": 256}")
        );
    }

    @Override
    protected JnksIotNode getTestNode() {
        return restNode;
    }

}
