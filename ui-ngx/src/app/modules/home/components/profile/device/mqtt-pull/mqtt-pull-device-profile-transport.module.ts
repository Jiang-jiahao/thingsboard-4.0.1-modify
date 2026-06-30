///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { NgModule } from '@angular/core';
import { SharedModule } from '@shared/shared.module';
import { CommonModule } from '@angular/common';
import { MqttPullDeviceProfileTransportConfigurationComponent } from './mqtt-pull-device-profile-transport-configuration.component';
import { MqttPullDeviceTransportConfigurationComponent } from './mqtt-pull-device-transport-configuration.component';
import { MqttDeviceProfileTransportConfigurationComponent } from './mqtt-device-profile-transport-configuration.component';
import { MqttPassiveDeviceProfileTransportConfigurationComponent } from './mqtt-passive-device-profile-transport-configuration.component';
import { MqttPullSubscribeRequestsConfigComponent } from './mqtt-pull-subscribe-requests-config.component';
import { HttpPullDeviceProfileTransportModule } from '../http-pull/http-pull-device-profile-transport.module';

@NgModule({
  declarations: [
    MqttPullDeviceProfileTransportConfigurationComponent,
    MqttPullDeviceTransportConfigurationComponent,
    MqttDeviceProfileTransportConfigurationComponent,
    MqttPassiveDeviceProfileTransportConfigurationComponent,
    MqttPullSubscribeRequestsConfigComponent
  ],
  imports: [
    CommonModule,
    SharedModule,
    HttpPullDeviceProfileTransportModule
  ],
  exports: [
    MqttPullDeviceProfileTransportConfigurationComponent,
    MqttPullDeviceTransportConfigurationComponent,
    MqttDeviceProfileTransportConfigurationComponent,
    MqttPassiveDeviceProfileTransportConfigurationComponent
  ]
})
export class MqttPullDeviceProfileTransportModule { }
