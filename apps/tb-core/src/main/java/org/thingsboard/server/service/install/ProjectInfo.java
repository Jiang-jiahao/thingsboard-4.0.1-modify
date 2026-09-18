package org.thingsboard.server.service.install;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;
import org.springframework.stereotype.Component;

/**
 * {@link BuildProperties} 由 {@code spring-boot-maven-plugin} 的 {@code build-info} 生成
 * {@code META-INF/build-info.properties} 后注册；IDE 直接运行若未执行该目标则无此 Bean。
 */
@Component
public class ProjectInfo {

    private final BuildProperties buildProperties;

    public ProjectInfo(@Autowired(required = false) BuildProperties buildProperties) {
        this.buildProperties = buildProperties;
    }

    public String getProjectVersion() {
        if (buildProperties != null && buildProperties.getVersion() != null) {
            return buildProperties.getVersion().replaceAll("[^\\d.]", "");
        }
        String fromManifest = ProjectInfo.class.getPackage().getImplementationVersion();
        if (fromManifest != null && !fromManifest.isBlank()) {
            return fromManifest.replaceAll("[^\\d.]", "");
        }
        return "0.0.0";
    }

    public String getProductType() {
        return "CE";
    }

}
