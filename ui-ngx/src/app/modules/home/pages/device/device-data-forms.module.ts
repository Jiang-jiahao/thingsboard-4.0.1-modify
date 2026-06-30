///
/// Copyright © 2016-2025 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SharedModule } from '@shared/shared.module';
import { DeviceProfileCommonModule } from '@home/components/profile/device/common/device-profile-common.module';
import { DefaultDeviceConfigurationComponent } from './data/default-device-configuration.component';
import { DeviceConfigurationComponent } from './data/device-configuration.component';
import { DeviceDataComponent } from './data/device-data.component';
import { DefaultDeviceTransportConfigurationComponent } from './data/default-device-transport-configuration.component';
import { DeviceTransportConfigurationComponent } from './data/device-transport-configuration.component';
import { MqttDeviceTransportConfigurationComponent } from './data/mqtt-device-transport-configuration.component';
import { CoapDeviceTransportConfigurationComponent } from './data/coap-device-transport-configuration.component';
import { Lwm2mDeviceTransportConfigurationComponent } from './data/lwm2m-device-transport-configuration.component';
import { SnmpDeviceTransportConfigurationComponent } from './data/snmp-device-transport-configuration.component';
import { TcpDeviceTransportConfigurationComponent } from './data/tcp-device-transport-configuration.component';
import { UdpDeviceTransportConfigurationComponent } from './data/udp-device-transport-configuration.component';
import { HttpPullDeviceProfileTransportModule } from '@home/components/profile/device/http-pull/http-pull-device-profile-transport.module';
import { MqttPullDeviceProfileTransportModule } from '@home/components/profile/device/mqtt-pull/mqtt-pull-device-profile-transport.module';

@NgModule({
  declarations: [
    DefaultDeviceConfigurationComponent,
    DeviceConfigurationComponent,
    DefaultDeviceTransportConfigurationComponent,
    MqttDeviceTransportConfigurationComponent,
    CoapDeviceTransportConfigurationComponent,
    Lwm2mDeviceTransportConfigurationComponent,
    SnmpDeviceTransportConfigurationComponent,
    TcpDeviceTransportConfigurationComponent,
    UdpDeviceTransportConfigurationComponent,
    DeviceTransportConfigurationComponent,
    DeviceDataComponent
  ],
  imports: [
    CommonModule,
    SharedModule,
    DeviceProfileCommonModule,
    HttpPullDeviceProfileTransportModule,
    MqttPullDeviceProfileTransportModule
  ],
  exports: [
    DeviceDataComponent
  ]
})
export class DeviceDataFormsModule { }
