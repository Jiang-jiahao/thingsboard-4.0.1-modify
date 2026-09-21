package com.jnks.iot.rule.engine.aws.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import lombok.extern.slf4j.Slf4j;
import com.jnks.iot.rule.engine.api.RuleNode;
import com.jnks.iot.rule.engine.api.JnksIotContext;
import com.jnks.iot.rule.engine.api.JnksIotNodeConfiguration;
import com.jnks.iot.rule.engine.api.JnksIotNodeException;
import com.jnks.iot.rule.engine.api.util.JnksIotNodeUtils;
import com.jnks.iot.rule.engine.external.JnksIotAbstractExternalNode;
import com.jnks.iot.server.common.data.StringUtils;
import com.jnks.iot.server.common.data.plugin.ComponentType;
import com.jnks.iot.server.common.msg.JnksIotMsg;
import com.jnks.iot.server.common.msg.JnksIotMsgMetaData;
import com.jnks.iot.server.dao.exception.DataValidationException;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static com.jnks.iot.server.dao.service.ConstraintValidator.validateFields;

@Slf4j
@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "aws lambda",
        configClazz = JnksIotAwsLambdaNodeConfiguration.class,
        nodeDescription = "Publish message to the AWS Lambda",
        nodeDetails = "Publishes messages to AWS Lambda, a service that lets you run code " +
                "without provisioning or managing servers. " +
                "It sends messages using a RequestResponse invocation type. " +
                "The node uses a pre-configured client and specified function to run.<br><br>" +
                "Output connections: <code>Success</code>, <code>Failure</code>.",
        configDirective = "jnksIotExternalNodeLambdaConfig",
        iconUrl = "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgd2lkdGg9IjQ4IiBoZWlnaHQ9IjQ4Ij48cGF0aCBkPSJNMTMuMjMgMTAuNTZWMTBjLTEuOTQgMC0zLjk5LjM5LTMuOTkgMi42NyAwIDEuMTYuNjEgMS45NSAxLjYzIDEuOTUuNzYgMCAxLjQzLS40NyAxLjg2LTEuMjIuNTItLjkzLjUtMS44LjUtMi44NG0yLjcgNi41M2MtLjE4LjE2LS40My4xNy0uNjMuMDYtLjg5LS43NC0xLjA1LTEuMDgtMS41NC0xLjc5LTEuNDcgMS41LTIuNTEgMS45NS00LjQyIDEuOTUtMi4yNSAwLTQuMDEtMS4zOS00LjAxLTQuMTcgMC0yLjE4IDEuMTctMy42NCAyLjg2LTQuMzggMS40Ni0uNjQgMy40OS0uNzYgNS4wNC0uOTNWNy41YzAtLjY2LjA1LTEuNDEtLjMzLTEuOTYtLjMyLS40OS0uOTUtLjctMS41LS43LTEuMDIgMC0xLjkzLjUzLTIuMTUgMS42MS0uMDUuMjQtLjI1LjQ4LS40Ny40OWwtMi42LS4yOGMtLjIyLS4wNS0uNDYtLjIyLS40LS41Ni42LTMuMTUgMy40NS00LjEgNi00LjEgMS4zIDAgMyAuMzUgNC4wMyAxLjMzQzE3LjExIDQuNTUgMTcgNi4xOCAxNyA3Ljk1djQuMTdjMCAxLjI1LjUgMS44MSAxIDIuNDguMTcuMjUuMjEuNTQgMCAuNzFsLTIuMDYgMS43OGgtLjAxIj48L3BhdGg+PHBhdGggZD0iTTIwLjE2IDE5LjU0QzE4IDIxLjE0IDE0LjgyIDIyIDEyLjEgMjJjLTMuODEgMC03LjI1LTEuNDEtOS44NS0zLjc2LS4yLS4xOC0uMDItLjQzLjI1LS4yOSAyLjc4IDEuNjMgNi4yNSAyLjYxIDkuODMgMi42MSAyLjQxIDAgNS4wNy0uNSA3LjUxLTEuNTMuMzctLjE2LjY2LjI0LjMyLjUxIj48L3BhdGg+PHBhdGggZD0iTTIxLjA3IDE4LjVjLS4yOC0uMzYtMS44NS0uMTctMi41Ny0uMDgtLjE5LjAyLS4yMi0uMTYtLjAzLS4zIDEuMjQtLjg4IDMuMjktLjYyIDMuNTMtLjMzLjI0LjMtLjA3IDIuMzUtMS4yNCAzLjMyLS4xOC4xNi0uMzUuMDctLjI2LS4xMS4yNi0uNjcuODUtMi4xNC41Ny0yLjV6Ij48L3BhdGg+PC9zdmc+"
)
public class JnksIotAwsLambdaNode extends JnksIotAbstractExternalNode {

    private JnksIotAwsLambdaNodeConfiguration config;
    private AWSLambdaAsync client;

    @Override
    public void init(JnksIotContext ctx, JnksIotNodeConfiguration configuration) throws JnksIotNodeException {
        config = JnksIotNodeUtils.convert(configuration, JnksIotAwsLambdaNodeConfiguration.class);
        String errorPrefix = "'" + ctx.getSelf().getName() + "' node configuration is invalid: ";
        try {
            validateFields(config, errorPrefix);
            AWSCredentials awsCredentials = new BasicAWSCredentials(config.getAccessKey(), config.getSecretKey());
            client = AWSLambdaAsyncClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .withRegion(config.getRegion())
                    .withClientConfiguration(new ClientConfiguration()
                            .withConnectionTimeout((int) TimeUnit.SECONDS.toMillis(config.getConnectionTimeout()))
                            .withRequestTimeout((int) TimeUnit.SECONDS.toMillis(config.getRequestTimeout())))
                    .build();
        } catch (DataValidationException e) {
            throw new JnksIotNodeException(e, true);
        } catch (Exception e) {
            throw new JnksIotNodeException(e);
        }
    }

    @Override
    public void onMsg(JnksIotContext ctx, JnksIotMsg msg) {
        var jnksIotMsg = ackIfNeeded(ctx, msg);
        String functionName = JnksIotNodeUtils.processPattern(config.getFunctionName(), jnksIotMsg);
        String qualifier = StringUtils.isBlank(config.getQualifier()) ?
                JnksIotAwsLambdaNodeConfiguration.DEFAULT_QUALIFIER :
                JnksIotNodeUtils.processPattern(config.getQualifier(), jnksIotMsg);
        InvokeRequest request = toRequest(jnksIotMsg.getData(), functionName, qualifier);
        client.invokeAsync(request, new AsyncHandler<>() {
            @Override
            public void onError(Exception e) {
                tellFailure(ctx, jnksIotMsg, e);
            }

            @Override
            public void onSuccess(InvokeRequest request, InvokeResult invokeResult) {
                try {
                    if (config.isTellFailureIfFuncThrowsExc() && invokeResult.getFunctionError() != null) {
                        throw new RuntimeException(getPayload(invokeResult));
                    }
                    tellSuccess(ctx, getResponseMsg(jnksIotMsg, invokeResult));
                } catch (Exception e) {
                    tellFailure(ctx, processException(jnksIotMsg, invokeResult, e), e);
                }
            }
        });
    }

    private InvokeRequest toRequest(String requestBody, String functionName, String qualifier) {
        return new InvokeRequest()
                .withFunctionName(functionName)
                .withPayload(requestBody)
                .withQualifier(qualifier);
    }

    private String getPayload(InvokeResult invokeResult) {
        ByteBuffer buf = invokeResult.getPayload();
        if (buf == null) {
            throw new RuntimeException("Payload from result of AWS Lambda function execution is null.");
        }
        byte[] responseBytes = new byte[buf.remaining()];
        buf.get(responseBytes);
        return new String(responseBytes);
    }

    private JnksIotMsg getResponseMsg(JnksIotMsg originalMsg, InvokeResult invokeResult) {
        JnksIotMsgMetaData metaData = originalMsg.getMetaData().copy();
        metaData.putValue("requestId", invokeResult.getSdkResponseMetadata().getRequestId());
        String data = getPayload(invokeResult);
        return originalMsg.transform()
                .metaData(metaData)
                .data(data)
                .build();
    }

    private JnksIotMsg processException(JnksIotMsg origMsg, InvokeResult invokeResult, Throwable t) {
        JnksIotMsgMetaData metaData = origMsg.getMetaData().copy();
        metaData.putValue("error", t.getClass() + ": " + t.getMessage());
        metaData.putValue("requestId", invokeResult.getSdkResponseMetadata().getRequestId());
        return origMsg.transform()
                .metaData(metaData)
                .build();
    }

    @Override
    public void destroy() {
        if (client != null) {
            try {
                client.shutdown();
            } catch (Exception e) {
                log.error("Failed to shutdown Lambda client during destroy", e);
            }
        }
    }
}
