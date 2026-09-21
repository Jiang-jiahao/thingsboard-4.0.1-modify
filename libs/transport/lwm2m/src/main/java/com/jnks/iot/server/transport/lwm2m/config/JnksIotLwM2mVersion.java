package com.jnks.iot.server.transport.lwm2m.config;

import lombok.Getter;
import org.eclipse.leshan.core.LwM2m.LwM2mVersion;
import org.eclipse.leshan.core.request.ContentFormat;

public enum JnksIotLwM2mVersion {
    VERSION_1_0(0, LwM2mVersion.V1_0, ContentFormat.TLV, false),
    VERSION_1_1(1, LwM2mVersion.V1_1, ContentFormat.TEXT, true);

    @Getter
    private final int code;
    @Getter
    private final LwM2mVersion version;
    @Getter
    private final ContentFormat contentFormat;
    @Getter
    private final boolean composite;

    JnksIotLwM2mVersion(int code, LwM2mVersion version, ContentFormat contentFormat, boolean composite) {
        this.code = code;
        this.version = version;
        this.contentFormat = contentFormat;
        this.composite = composite;
    }

    public static JnksIotLwM2mVersion fromVersion(LwM2mVersion version) {
        for (JnksIotLwM2mVersion to : JnksIotLwM2mVersion.values()) {
            if (to.version.equals(version)) {
                return to;
            }
        }
        throw new IllegalArgumentException(String.format("Unsupported typeLwM2mVersion type : %s", version));
    }

    public static JnksIotLwM2mVersion fromVersionStr(String versionStr) {
        for (JnksIotLwM2mVersion to : JnksIotLwM2mVersion.values()) {
            if (to.version.toString().equals(versionStr)) {
                return to;
            }
        }
        throw new IllegalArgumentException(String.format("Unsupported contentFormatLwM2mVersion version : %s", versionStr));
    }

    public static JnksIotLwM2mVersion fromCode(int code) {
        for (JnksIotLwM2mVersion to : JnksIotLwM2mVersion.values()) {
            if (to.code == code) {
                return to;
            }
        }
        throw new IllegalArgumentException(String.format("Unsupported codeLwM2mVersion code : %d", code));
    }
}

