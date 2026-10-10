import { NgModule } from '@angular/core';
import { SharedModule } from '@shared/shared.module';
import { CommonModule } from '@angular/common';
import { HttpPullDeviceProfileTransportConfigurationComponent } from './http-pull-device-profile-transport-configuration.component';
import { HttpPullDeviceTransportConfigurationComponent } from './http-pull-device-transport-configuration.component';
import { HttpDeviceProfileTransportConfigurationComponent } from './http-device-profile-transport-configuration.component';
import { HttpPassiveDeviceProfileTransportConfigurationComponent } from './http-passive-device-profile-transport-configuration.component';
import { HttpPassiveDeviceTransportConfigurationComponent } from './http-passive-device-transport-configuration.component';
import { HttpPullPollRequestsConfigComponent } from './http-pull-poll-requests-config.component';

@NgModule({
  declarations: [
    HttpPullDeviceProfileTransportConfigurationComponent,
    HttpPullDeviceTransportConfigurationComponent,
    HttpDeviceProfileTransportConfigurationComponent,
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
    HttpPassiveDeviceProfileTransportConfigurationComponent,
    HttpPassiveDeviceTransportConfigurationComponent
  ]
})
export class HttpPullDeviceProfileTransportModule { }
