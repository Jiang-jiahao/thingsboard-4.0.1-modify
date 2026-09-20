package com.jnks.iot.server.common.data;

public interface HasImage extends HasTenantId, HasName {

    String getImage();

    void setImage(String image);

}
