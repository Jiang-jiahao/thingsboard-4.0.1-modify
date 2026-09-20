package com.jnks.iot.server.exception;

import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.authentication.LockedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.util.WebUtils;
import com.jnks.iot.common.util.JacksonUtil;
import com.jnks.iot.server.common.data.exception.JnksIotErrorCode;
import com.jnks.iot.server.common.data.exception.JnksIotException;
import com.jnks.iot.server.common.msg.tools.MaxPayloadSizeExceededException;
import com.jnks.iot.server.common.msg.tools.TbRateLimitsException;
import com.jnks.iot.server.service.security.exception.AuthMethodNotSupportedException;
import com.jnks.iot.server.service.security.exception.JwtExpiredTokenException;
import com.jnks.iot.server.service.security.exception.UserPasswordExpiredException;
import com.jnks.iot.server.service.security.exception.UserPasswordNotValidException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Controller
@RestControllerAdvice
public class JnksIotErrorResponseHandler extends ResponseEntityExceptionHandler implements AccessDeniedHandler, ErrorController {

    private static final Map<HttpStatus, JnksIotErrorCode> statusToErrorCodeMap = new HashMap<>();

    static {
        statusToErrorCodeMap.put(HttpStatus.BAD_REQUEST, JnksIotErrorCode.BAD_REQUEST_PARAMS);
        statusToErrorCodeMap.put(HttpStatus.UNAUTHORIZED, JnksIotErrorCode.AUTHENTICATION);
        statusToErrorCodeMap.put(HttpStatus.FORBIDDEN, JnksIotErrorCode.PERMISSION_DENIED);
        statusToErrorCodeMap.put(HttpStatus.NOT_FOUND, JnksIotErrorCode.ITEM_NOT_FOUND);
        statusToErrorCodeMap.put(HttpStatus.METHOD_NOT_ALLOWED, JnksIotErrorCode.BAD_REQUEST_PARAMS);
        statusToErrorCodeMap.put(HttpStatus.NOT_ACCEPTABLE, JnksIotErrorCode.BAD_REQUEST_PARAMS);
        statusToErrorCodeMap.put(HttpStatus.UNSUPPORTED_MEDIA_TYPE, JnksIotErrorCode.BAD_REQUEST_PARAMS);
        statusToErrorCodeMap.put(HttpStatus.TOO_MANY_REQUESTS, JnksIotErrorCode.TOO_MANY_REQUESTS);
        statusToErrorCodeMap.put(HttpStatus.INTERNAL_SERVER_ERROR, JnksIotErrorCode.GENERAL);
        statusToErrorCodeMap.put(HttpStatus.SERVICE_UNAVAILABLE, JnksIotErrorCode.GENERAL);
    }

    private static final Map<JnksIotErrorCode, HttpStatus> errorCodeToStatusMap = new HashMap<>();

    static {
        errorCodeToStatusMap.put(JnksIotErrorCode.GENERAL, HttpStatus.INTERNAL_SERVER_ERROR);
        errorCodeToStatusMap.put(JnksIotErrorCode.AUTHENTICATION, HttpStatus.UNAUTHORIZED);
        errorCodeToStatusMap.put(JnksIotErrorCode.JWT_TOKEN_EXPIRED, HttpStatus.UNAUTHORIZED);
        errorCodeToStatusMap.put(JnksIotErrorCode.CREDENTIALS_EXPIRED, HttpStatus.UNAUTHORIZED);
        errorCodeToStatusMap.put(JnksIotErrorCode.PERMISSION_DENIED, HttpStatus.FORBIDDEN);
        errorCodeToStatusMap.put(JnksIotErrorCode.INVALID_ARGUMENTS, HttpStatus.BAD_REQUEST);
        errorCodeToStatusMap.put(JnksIotErrorCode.BAD_REQUEST_PARAMS, HttpStatus.BAD_REQUEST);
        errorCodeToStatusMap.put(JnksIotErrorCode.ITEM_NOT_FOUND, HttpStatus.NOT_FOUND);
        errorCodeToStatusMap.put(JnksIotErrorCode.TOO_MANY_REQUESTS, HttpStatus.TOO_MANY_REQUESTS);
        errorCodeToStatusMap.put(JnksIotErrorCode.TOO_MANY_UPDATES, HttpStatus.TOO_MANY_REQUESTS);
        errorCodeToStatusMap.put(JnksIotErrorCode.SUBSCRIPTION_VIOLATION, HttpStatus.FORBIDDEN);
        errorCodeToStatusMap.put(JnksIotErrorCode.VERSION_CONFLICT, HttpStatus.CONFLICT);
    }

    private static JnksIotErrorCode statusToErrorCode(HttpStatus status) {
        return statusToErrorCodeMap.getOrDefault(status, JnksIotErrorCode.GENERAL);
    }

    private static HttpStatus errorCodeToStatus(JnksIotErrorCode errorCode) {
        return errorCodeToStatusMap.getOrDefault(errorCode, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @RequestMapping("/error")
    public ResponseEntity<Object> handleError(HttpServletRequest request) {
        HttpStatus httpStatus = Optional.ofNullable(request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE))
                .map(status -> HttpStatus.resolve(Integer.parseInt(status.toString())))
                .orElse(HttpStatus.INTERNAL_SERVER_ERROR);
        String errorMessage = Optional.ofNullable(request.getAttribute(RequestDispatcher.ERROR_EXCEPTION))
                .map(e -> (ExceptionUtils.getMessage((Throwable) e)))
                .orElse(httpStatus.getReasonPhrase());
        return new ResponseEntity<>(JnksIotErrorResponse.of(errorMessage, statusToErrorCode(httpStatus), httpStatus), httpStatus);
    }

    @Override
    @ExceptionHandler(AccessDeniedException.class)
    public void handle(HttpServletRequest request, HttpServletResponse response,
                       AccessDeniedException accessDeniedException) throws IOException,
            ServletException {
        if (!response.isCommitted()) {
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.setStatus(HttpStatus.FORBIDDEN.value());
            JacksonUtil.writeValue(response.getWriter(),
                    JnksIotErrorResponse.of("You don't have permission to perform this operation!",
                            JnksIotErrorCode.PERMISSION_DENIED, HttpStatus.FORBIDDEN));
        }
    }

    @ExceptionHandler(Exception.class)
    public void handle(Exception exception, HttpServletResponse response) {
        log.debug("Processing exception {}", exception.getMessage(), exception);
        if (!response.isCommitted()) {
            try {
                response.setContentType(MediaType.APPLICATION_JSON_VALUE);

                if (exception instanceof JnksIotException) {
                    JnksIotException jnksIotException = (JnksIotException) exception;
                    if (jnksIotException.getErrorCode() == JnksIotErrorCode.SUBSCRIPTION_VIOLATION) {
                        handleSubscriptionException((JnksIotException) exception, response);
                    } else {
                        handleJnksIotException((JnksIotException) exception, response);
                    }
                } else if (exception instanceof TbRateLimitsException) {
                    handleRateLimitException(response, (TbRateLimitsException) exception);
                } else if (exception instanceof AccessDeniedException) {
                    handleAccessDeniedException(response);
                } else if (exception instanceof AuthenticationException) {
                    handleAuthenticationException((AuthenticationException) exception, response);
                }  else if (exception instanceof MaxPayloadSizeExceededException) {
                    handleMaxPayloadSizeExceededException(response, (MaxPayloadSizeExceededException) exception);
                } else {
                    response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
                    JacksonUtil.writeValue(response.getWriter(), JnksIotErrorResponse.of(exception.getMessage(),
                            JnksIotErrorCode.GENERAL, HttpStatus.INTERNAL_SERVER_ERROR));
                }
            } catch (IOException e) {
                log.error("Can't handle exception", e);
            }
        }
    }

    @Override
    protected ResponseEntity<Object> handleExceptionInternal(
            Exception ex, @Nullable Object body,
            HttpHeaders headers, HttpStatusCode statusCode,
            WebRequest request) {
        if (HttpStatus.INTERNAL_SERVER_ERROR.equals(statusCode)) {
            request.setAttribute(WebUtils.ERROR_EXCEPTION_ATTRIBUTE, ex, WebRequest.SCOPE_REQUEST);
        }
        JnksIotErrorCode errorCode = statusToErrorCode((HttpStatus) statusCode);
        return new ResponseEntity<>(JnksIotErrorResponse.of(ex.getMessage(), errorCode, (HttpStatus) statusCode), headers, statusCode);
    }

    private void handleJnksIotException(JnksIotException jnksIotException, HttpServletResponse response) throws IOException {
        JnksIotErrorCode errorCode = jnksIotException.getErrorCode();
        HttpStatus status = errorCodeToStatus(errorCode);
        response.setStatus(status.value());
        JacksonUtil.writeValue(response.getWriter(), JnksIotErrorResponse.of(jnksIotException.getMessage(), errorCode, status));
    }

    private void handleRateLimitException(HttpServletResponse response, TbRateLimitsException exception) throws IOException {
        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        String message = "Too many requests for current " + exception.getEntityType().name().toLowerCase() + "!";
        JacksonUtil.writeValue(response.getWriter(),
                JnksIotErrorResponse.of(message,
                        JnksIotErrorCode.TOO_MANY_REQUESTS, HttpStatus.TOO_MANY_REQUESTS));
    }

    private void handleMaxPayloadSizeExceededException(HttpServletResponse response, MaxPayloadSizeExceededException exception) throws IOException {
        response.setStatus(HttpStatus.PAYLOAD_TOO_LARGE.value());
        JacksonUtil.writeValue(response.getWriter(),
                JnksIotErrorResponse.of(exception.getMessage(),
                        JnksIotErrorCode.BAD_REQUEST_PARAMS, HttpStatus.PAYLOAD_TOO_LARGE));
    }

    private void handleSubscriptionException(JnksIotException subscriptionException, HttpServletResponse response) throws IOException {
        response.setStatus(HttpStatus.FORBIDDEN.value());
        JacksonUtil.writeValue(response.getWriter(),
                JacksonUtil.fromBytes(((HttpClientErrorException) subscriptionException.getCause()).getResponseBodyAsByteArray(), Object.class));
    }

    private void handleAccessDeniedException(HttpServletResponse response) throws IOException {
        response.setStatus(HttpStatus.FORBIDDEN.value());
        JacksonUtil.writeValue(response.getWriter(),
                JnksIotErrorResponse.of("You don't have permission to perform this operation!",
                        JnksIotErrorCode.PERMISSION_DENIED, HttpStatus.FORBIDDEN));

    }

    private void handleAuthenticationException(AuthenticationException authenticationException, HttpServletResponse response) throws IOException {
        response.setStatus(HttpStatus.UNAUTHORIZED.value());
        if (authenticationException instanceof BadCredentialsException || authenticationException instanceof UsernameNotFoundException) {
            JacksonUtil.writeValue(response.getWriter(), JnksIotErrorResponse.of("Invalid username or password", JnksIotErrorCode.AUTHENTICATION, HttpStatus.UNAUTHORIZED));
        } else if (authenticationException instanceof DisabledException) {
            JacksonUtil.writeValue(response.getWriter(), JnksIotErrorResponse.of("User account is not active", JnksIotErrorCode.AUTHENTICATION, HttpStatus.UNAUTHORIZED));
        } else if (authenticationException instanceof LockedException) {
            JacksonUtil.writeValue(response.getWriter(), JnksIotErrorResponse.of("User account is locked due to security policy", JnksIotErrorCode.AUTHENTICATION, HttpStatus.UNAUTHORIZED));
        } else if (authenticationException instanceof JwtExpiredTokenException) {
            JacksonUtil.writeValue(response.getWriter(), JnksIotErrorResponse.of("Token has expired", JnksIotErrorCode.JWT_TOKEN_EXPIRED, HttpStatus.UNAUTHORIZED));
        } else if (authenticationException instanceof AuthMethodNotSupportedException) {
            JacksonUtil.writeValue(response.getWriter(), JnksIotErrorResponse.of(authenticationException.getMessage(), JnksIotErrorCode.AUTHENTICATION, HttpStatus.UNAUTHORIZED));
        } else if (authenticationException instanceof UserPasswordExpiredException) {
            UserPasswordExpiredException expiredException = (UserPasswordExpiredException) authenticationException;
            String resetToken = expiredException.getResetToken();
            JacksonUtil.writeValue(response.getWriter(), JnksIotCredentialsExpiredResponse.of(expiredException.getMessage(), resetToken));
        } else if (authenticationException instanceof UserPasswordNotValidException) {
            UserPasswordNotValidException expiredException = (UserPasswordNotValidException) authenticationException;
            JacksonUtil.writeValue(response.getWriter(), JnksIotCredentialsViolationResponse.of(expiredException.getMessage()));
        } else {
            JacksonUtil.writeValue(response.getWriter(), JnksIotErrorResponse.of("Authentication failed", JnksIotErrorCode.AUTHENTICATION, HttpStatus.UNAUTHORIZED));
        }
    }

}
