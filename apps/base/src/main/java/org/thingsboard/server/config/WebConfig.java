package org.thingsboard.server.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springdoc.webmvc.ui.SwaggerWelcomeWebMvc;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.thingsboard.server.utils.MiscUtils;

import java.io.IOException;

@Controller
public class WebConfig {

    @RequestMapping(value = {"/assets", "/assets/", "/{path:^(?!api$)(?!assets$)(?!static$)(?!webjars$)(?!swagger-ui$)[^\\.]*}/**"})
    public String redirect() {
        return "forward:/index.html";
    }

    /**
     * 该方法仅用于处理swagger-ui.html的请求，将请求重定向到swagger-ui/index.html（但是在swagger里面已经实现了该重定向
     * 参考{@link SwaggerWelcomeWebMvc#redirectToUi}
     * @throws IOException io异常
     */
//    @RequestMapping("/swagger-ui.html")
//    public void redirectSwagger(HttpServletRequest request, HttpServletResponse response) throws IOException {
//        String baseUrl = MiscUtils.constructBaseUrl(request);
//        response.sendRedirect(baseUrl + "/swagger-ui/");
//    }

    @RequestMapping("/swagger-ui/")
    public String redirectSwaggerIndex() throws IOException {
        return "forward:/swagger-ui/index.html";
    }

}
