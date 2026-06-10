///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { NgModule } from '@angular/core';
import { SharedModule } from '@shared/shared.module';
import { CommonModule } from '@angular/common';
import { HttpPullDeviceProfileTransportConfigurationComponent } from './http-pull-device-profile-transport-configuration.component';
import { HttpPullDeviceTransportConfigurationComponent } from './http-pull-device-transport-configuration.component';
import { HttpDeviceProfileTransportConfigurationComponent } from './http-device-profile-transport-configuration.component';
import { HttpDeviceTransportConfigurationComponent } from './http-device-transport-configuration.component';
import { HttpPullRoutingHelpDialogComponent } from './http-pull-routing-help-dialog.component';
import { HttpDeviceRoutingConfigurationComponent } from './http-device-routing-configuration.component';
import { HttpPassiveDeviceProfileTransportConfigurationComponent } from './http-passive-device-profile-transport-configuration.component';
import { HttpPassiveDeviceTransportConfigurationComponent } from './http-passive-device-transport-configuration.component';
import { HttpPullPollRequestsConfigComponent } from './http-pull-poll-requests-config.component';

@NgModule({
  declarations: [
    HttpPullDeviceProfileTransportConfigurationComponent,
    HttpPullDeviceTransportConfigurationComponent,
    HttpDeviceProfileTransportConfigurationComponent,
    HttpDeviceTransportConfigurationComponent,
    HttpPullRoutingHelpDialogComponent,
    HttpDeviceRoutingConfigurationComponent,
    HttpPassiveDeviceProfileTransportConfigurationComponent,
    HttpPassiveDeviceTransportConfigurationComponent,
    HttpPullPollRequestsConfigComponent
  ],
  imports: [
    CommonModule,
    SharedModule
  ],
  exports: [
    HttpPullDeviceProfileTransportConfigurationComponent,
    HttpPullDeviceTransportConfigurationComponent,
    HttpDeviceProfileTransportConfigurationComponent,
    HttpDeviceTransportConfigurationComponent,
    HttpPassiveDeviceProfileTransportConfigurationComponent,
    HttpPassiveDeviceTransportConfigurationComponent
  ]
})
export class HttpPullDeviceProfileTransportModule { }
