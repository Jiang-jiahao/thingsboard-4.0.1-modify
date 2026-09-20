package com.jnks.iot.server.service.mail;

import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import com.jnks.iot.server.dao.settings.AdminSettingsService;

@Component
@Data
@Lazy
public class TbMailContextComponent {

    @Autowired
    private AdminSettingsService adminSettingsService;
}