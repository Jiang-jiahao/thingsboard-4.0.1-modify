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
import { Component, DestroyRef, forwardRef, Input, OnInit } from '@angular/core';
import {
  ControlValueAccessor,
  NG_VALIDATORS,
  NG_VALUE_ACCESSOR,
  UntypedFormBuilder,
  UntypedFormGroup,
  ValidationErrors,
  Validator,
  Validators
} from '@angular/forms';
import { Store } from '@ngrx/store';
import { AppState } from '@app/core/core.state';
import { coerceBooleanProperty } from '@angular/cdk/coercion';
import {
  DeviceTransportType,
  TcpDeviceTransportConfiguration,
  TcpTransportConnectMode,
  TcpWireAuthenticationMode
} from '@shared/models/device.models';
import { isDefinedAndNotNull } from '@core/utils';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
@Component({
  selector: 'tb-tcp-device-transport-configuration',
  templateUrl: './tcp-device-transport-configuration.component.html',
  styleUrls: [],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => TcpDeviceTransportConfigurationComponent),
      multi: true
    }, {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => TcpDeviceTransportConfigurationComponent),
      multi: true
    }]
})
export class TcpDeviceTransportConfigurationComponent implements ControlValueAccessor, OnInit, Validator {
  tcpDeviceTransportConfigurationFormGroup: UntypedFormGroup;
  private requiredValue: boolean;
  get required(): boolean {
    return this.requiredValue;
  }
  @Input()
  set required(value: boolean) {
    this.requiredValue = coerceBooleanProperty(value);
  }
  @Input()
  disabled: boolean;

  private tcpWireAuthMode: TcpWireAuthenticationMode | null = null;

  private tcpProfileTransportConnectModeValue: TcpTransportConnectMode | null = null;

  /**
   * 来自设备页拉取的设备档案 TCP 链路上鉴权模式；
   * {@link TcpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 时显示协议设备 ID；
   * 与 {@link TcpWireAuthenticationMode#DEFERRED_PAYLOAD_TOKEN} 同属入站延迟鉴权时的表单项显隐见各 getter。
   */
  @Input()
  set tcpWireAuthenticationMode(mode: TcpWireAuthenticationMode | null) {
    const prev = this.tcpWireAuthMode;
    this.tcpWireAuthMode = mode;
    if (this.tcpDeviceTransportConfigurationFormGroup
        && prev === TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID
        && mode !== TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
      this.tcpDeviceTransportConfigurationFormGroup.patchValue(
        { tcpWireAuthPayloadDeviceId: '' },
        { emitEvent: true }
      );
    }
  }

  get showTcpWireAuthPayloadDeviceId(): boolean {
    return this.tcpWireAuthMode === TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID;
  }

  /** 设备档案 TCP CLIENT/SERVER；与延迟链路上鉴权组合用于隐藏无意义的 CLIENT 表单项 */
  @Input()
  set tcpProfileTransportConnectMode(mode: TcpTransportConnectMode | null) {
    this.tcpProfileTransportConnectModeValue = mode;
  }

  /** 延迟 TOKEN / 延迟协议设备 ID +（SERVER 或未带 connectMode）：按入站场景，不展示 CLIENT 对端主机/端口 */
  get showTcpClientEndpointFields(): boolean {
    if (this.isDeferredPayloadWireAuthMode()) {
      return this.tcpProfileTransportConnectModeValue === TcpTransportConnectMode.CLIENT;
    }
    return this.tcpProfileTransportConnectModeValue !== TcpTransportConnectMode.SERVER;
  }

  get showTcpDeviceHint(): boolean {
    return !this.isDeferredPayloadWireAuthInboundMode();
  }

  get showTcpSourceHostField(): boolean {
    return !this.isDeferredPayloadWireAuthInboundMode();
  }

  /** 延迟链路上鉴权且设备为出站 CLIENT 时通常无专用监听端口；其余情况保留（含入站监听端口） */
  get showTcpServerBindPortField(): boolean {
    if (this.isDeferredPayloadWireAuthMode()) {
      return this.tcpProfileTransportConnectModeValue !== TcpTransportConnectMode.CLIENT;
    }
    return true;
  }

  /** DEFERRED_PAYLOAD_TOKEN / DEFERRED_PAYLOAD_DEVICE_ID（链路上延迟解析身份） */
  private isDeferredPayloadWireAuthMode(): boolean {
    return this.tcpWireAuthMode === TcpWireAuthenticationMode.DEFERRED_PAYLOAD_TOKEN
      || this.tcpWireAuthMode === TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID;
  }

  /** 延迟 TOKEN 或延迟协议设备 ID，且非显式 CLIENT：入站专用端口场景，隐藏 CLIENT/源 IP/通用提示 */
  private isDeferredPayloadWireAuthInboundMode(): boolean {
    return this.isDeferredPayloadWireAuthMode()
      && this.tcpProfileTransportConnectModeValue !== TcpTransportConnectMode.CLIENT;
  }

  private propagateChange = (v: any) => { };
  constructor(private store: Store<AppState>,
              private fb: UntypedFormBuilder,
              private destroyRef: DestroyRef) {
  }
  registerOnChange(fn: any): void {
    this.propagateChange = fn;
  }
  registerOnTouched(fn: any): void {
  }
  ngOnInit() {
    this.tcpDeviceTransportConfigurationFormGroup = this.fb.group({
      host: ['127.0.0.1'],
      port: [5025, [Validators.min(1), Validators.max(65535)]],
      sourceHost: [''],
      serverBindPort: [null, [Validators.min(1), Validators.max(65535)]],
      tcpWireAuthPayloadDeviceId: ['']
    });
    this.tcpDeviceTransportConfigurationFormGroup.valueChanges.pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(() => {
      this.updateModel();
    });
  }
  setDisabledState(isDisabled: boolean): void {
    this.disabled = isDisabled;
    if (this.disabled) {
      this.tcpDeviceTransportConfigurationFormGroup.disable({emitEvent: false});
    } else {
      this.tcpDeviceTransportConfigurationFormGroup.enable({emitEvent: false});
    }
  }
  writeValue(value: TcpDeviceTransportConfiguration | null): void {
    if (isDefinedAndNotNull(value)) {
      this.tcpDeviceTransportConfigurationFormGroup.patchValue({
        host: value.host,
        port: value.port,
        sourceHost: value.sourceHost || '',
        serverBindPort: value.serverBindPort,
        tcpWireAuthPayloadDeviceId: value.tcpWireAuthPayloadDeviceId || ''
      }, {emitEvent: false});
    }
  }
  validate(): ValidationErrors | null {
    return this.tcpDeviceTransportConfigurationFormGroup.valid ? null : {tcpDeviceTransport: false};
  }
  private updateModel() {
    if (!this.tcpDeviceTransportConfigurationFormGroup.valid) {
        this.propagateChange(null);
        return;
      }
      const v = this.tcpDeviceTransportConfigurationFormGroup.getRawValue();
      const configuration: TcpDeviceTransportConfiguration = {
        type: DeviceTransportType.TCP,
        host: v.host,
        port: v.port
      };
      const sh = v.sourceHost?.trim();
      if (sh) {
        configuration.sourceHost = sh;
      }
      if (v.serverBindPort != null && v.serverBindPort !== '') {
        configuration.serverBindPort = Number(v.serverBindPort);
      }
      const pid = v.tcpWireAuthPayloadDeviceId?.trim();
      if (pid) {
        configuration.tcpWireAuthPayloadDeviceId = pid;
      }
      this.propagateChange(configuration);
  }
}
