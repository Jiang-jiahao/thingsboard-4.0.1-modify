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
    this.applyDeferredPayloadDeviceIdValidators();
  }

  get showTcpWireAuthPayloadDeviceId(): boolean {
    return this.tcpWireAuthMode === TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID;
  }

  /** 设备档案 TCP CLIENT/SERVER；与延迟链路上鉴权组合用于隐藏无意义的 CLIENT 表单项 */
  @Input()
  set tcpProfileTransportConnectMode(mode: TcpTransportConnectMode | null) {
    this.tcpProfileTransportConnectModeValue = mode;
    if (this.tcpDeviceTransportConfigurationFormGroup
        && mode === TcpTransportConnectMode.CLIENT) {
      this.tcpDeviceTransportConfigurationFormGroup.patchValue(
        { sourceHost: '', serverBindPort: null },
        { emitEvent: true }
      );
    }
    this.applyServerBindPortControlState();
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

  /**
   * 期望源 IP 仅用于档案为 SERVER 且链路上鉴权为 NONE（多设备共端口按源 IP 区分）等入站场景；
   * 档案为 CLIENT（平台主动连设备）时不展示。
   */
  get showTcpSourceHostField(): boolean {
    if (this.tcpProfileTransportConnectModeValue !== TcpTransportConnectMode.SERVER) {
      return false;
    }
    return !this.isDeferredPayloadWireAuthInboundMode();
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
      /* 勿对 null 加 min(1)：档案 connectMode 异步到达前 tcpProfileTransportConnectMode 为 null，会误使整个表单 invalid，deviceData 变成 null 提交 */
      serverBindPort: [null],
      tcpWireAuthPayloadDeviceId: ['']
    });
    this.tcpDeviceTransportConfigurationFormGroup.valueChanges.pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(() => {
      this.updateModel();
    });
    this.applyServerBindPortControlState();
    this.applyDeferredPayloadDeviceIdValidators();
  }

  /** 协议设备 ID 延迟档案：新增设备时须能填写且须非空，否则无法保存且易在共端口下产生脏数据。 */
  private applyDeferredPayloadDeviceIdValidators(): void {
    const grp = this.tcpDeviceTransportConfigurationFormGroup;
    if (!grp) {
      return;
    }
    const pidCtrl = grp.get('tcpWireAuthPayloadDeviceId');
    if (this.tcpWireAuthMode === TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
      pidCtrl.setValidators([(c) => {
        const v = c.value;
        return v != null && String(v).trim().length ? null : { required: true };
      }]);
    } else {
      pidCtrl.clearValidators();
    }
    pidCtrl.updateValueAndValidity({ emitEvent: true });
  }

  /** 设备侧不再提交 serverBindPort（监听端口在档案）；清空控件与校验避免残留旧数据 */
  private applyServerBindPortControlState(): void {
    const grp = this.tcpDeviceTransportConfigurationFormGroup;
    if (!grp) {
      return;
    }
    const ctrl = grp.get('serverBindPort');
    if (!ctrl) {
      return;
    }
    ctrl.reset(null, { emitEvent: false });
    ctrl.clearValidators();
    ctrl.updateValueAndValidity({ emitEvent: false });
    this.updateModel();
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
    this.applyServerBindPortControlState();
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
      if (this.tcpProfileTransportConnectModeValue === TcpTransportConnectMode.SERVER) {
        const sh = v.sourceHost?.trim();
        if (sh) {
          configuration.sourceHost = sh;
        }
      }
      const pid = v.tcpWireAuthPayloadDeviceId?.trim();
      if (pid) {
        configuration.tcpWireAuthPayloadDeviceId = pid;
      }
      this.propagateChange(configuration);
  }
}
