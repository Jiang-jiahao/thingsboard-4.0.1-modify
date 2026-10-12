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
  UdpDeviceTransportConfiguration,
  UdpWireAuthenticationMode
} from '@shared/models/device.models';
import { isDefinedAndNotNull } from '@core/utils';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
  selector: 'jnks-iot-udp-device-transport-configuration',
  templateUrl: './udp-device-transport-configuration.component.html',
  styleUrls: [],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => UdpDeviceTransportConfigurationComponent),
      multi: true
    }, {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => UdpDeviceTransportConfigurationComponent),
      multi: true
    }]
})
export class UdpDeviceTransportConfigurationComponent implements ControlValueAccessor, OnInit, Validator {

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

  private udpWireAuthMode: UdpWireAuthenticationMode | null = null;

  @Input()
  set udpWireAuthenticationMode(mode: UdpWireAuthenticationMode | null) {
    const prev = this.udpWireAuthMode;
    this.udpWireAuthMode = mode;
    const grp = this.tcpDeviceTransportConfigurationFormGroup;
    // 只在**确实填过值**时才清空回写：加载时 prev 还是 undefined，这段不该凭空产生一次"改动"
    if (grp
        && prev === UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID
        && mode !== UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID
        && String(grp.get('udpWireAuthPayloadDeviceId').value ?? '').length > 0) {
      grp.patchValue({ udpWireAuthPayloadDeviceId: '' }, { emitEvent: true });
    }
    this.runWithoutModelUpdate(() => this.applyDeferredPayloadDeviceIdValidators());
  }

  get showUdpWireAuthPayloadDeviceId(): boolean {
    return this.udpWireAuthMode === UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID;
  }

  /** UDP 设备向平台监听端口发报文；NONE 鉴权时可填期望源 IP 以区分同端口多设备 */
  get showUdpSourceHostField(): boolean {
    return this.udpWireAuthMode === UdpWireAuthenticationMode.NONE;
  }

  get showUdpDeviceHint(): boolean {
    return this.udpWireAuthMode !== UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID;
  }

  private propagateChange = (v: any) => { };

  /** 见 {@link #runWithoutModelUpdate}：初始化/写入期间抑制回写，否则父表单会被标脏。 */
  private suppressModelUpdate = false;

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
      sourceHost: [''],
      udpWireAuthPayloadDeviceId: [''],
      // 下行地址（必填）：平台只发到这里；没配就收不到 RPC/属性下发（不再回发上报的源地址）
      udpDownlinkHost: ['', Validators.required],
      udpDownlinkPort: [null, [Validators.required, Validators.min(1), Validators.max(65535)]]
    });
    this.tcpDeviceTransportConfigurationFormGroup.valueChanges.pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(() => {
      if (!this.suppressModelUpdate) {
        this.updateModel();
      }
    });
    this.runWithoutModelUpdate(() => {
      this.applyDeferredPayloadDeviceIdValidators();
    });
  }

  /**
   * 初始化与 {@link #writeValue} 期间**不能**回写模型。
   * <p>
   * `applyDeferredPayloadDeviceIdValidators()` 内部用 `updateValueAndValidity({emitEvent: true})` 刷新校验，
   * 这会触发本组件的 valueChanges；若此时调 {@link #updateModel} → `propagateChange(...)`，就等于在加载阶段
   * 往父表单的 `configuration` 控件写了一次值。Angular 把这次子→父写入记成"用户改动"，
   * 于是刚打开设备详情、什么都没编辑，顶层表单已经是 `ng-dirty` —— 离开时会弹「有未保存的更改」。
   */
  private runWithoutModelUpdate(action: () => void): void {
    this.suppressModelUpdate = true;
    try {
      action();
    } finally {
      this.suppressModelUpdate = false;
    }
  }

  private applyDeferredPayloadDeviceIdValidators(): void {
    const grp = this.tcpDeviceTransportConfigurationFormGroup;
    if (!grp) {
      return;
    }
    const pidCtrl = grp.get('udpWireAuthPayloadDeviceId');
    if (this.udpWireAuthMode === UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
      pidCtrl.setValidators([(c) => {
        const v = c.value;
        return v != null && String(v).trim().length ? null : { required: true };
      }]);
    } else {
      pidCtrl.clearValidators();
    }
    pidCtrl.updateValueAndValidity({ emitEvent: true });
  }

  setDisabledState(isDisabled: boolean): void {
    this.disabled = isDisabled;
    if (this.disabled) {
      this.tcpDeviceTransportConfigurationFormGroup.disable({emitEvent: false});
    } else {
      this.tcpDeviceTransportConfigurationFormGroup.enable({emitEvent: false});
    }
  }

  writeValue(value: UdpDeviceTransportConfiguration | null): void {
    if (isDefinedAndNotNull(value)) {
      this.tcpDeviceTransportConfigurationFormGroup.patchValue({
        sourceHost: value.sourceHost || '',
        udpWireAuthPayloadDeviceId: value.udpWireAuthPayloadDeviceId || '',
        udpDownlinkHost: value.udpDownlinkHost || '',
        udpDownlinkPort: value.udpDownlinkPort ?? null
      }, {emitEvent: false});
    }
    this.runWithoutModelUpdate(() => {
      this.applyDeferredPayloadDeviceIdValidators();
    });
  }

  validate(): ValidationErrors | null {
    return this.tcpDeviceTransportConfigurationFormGroup.valid ? null : {udpDeviceTransport: false};
  }

  private updateModel() {
    if (!this.tcpDeviceTransportConfigurationFormGroup.valid) {
      this.propagateChange(null);
      return;
    }
    const v = this.tcpDeviceTransportConfigurationFormGroup.getRawValue();
    const configuration: UdpDeviceTransportConfiguration = {
      type: DeviceTransportType.UDP
    };
    const sh = v.sourceHost?.trim();
    if (sh) {
      configuration.sourceHost = sh;
    }
    const pid = v.udpWireAuthPayloadDeviceId?.trim();
    if (pid) {
      configuration.udpWireAuthPayloadDeviceId = pid;
    }
    const dlHost = String(v.udpDownlinkHost ?? '').trim();
    const dlPortRaw = v.udpDownlinkPort;
    if (dlHost && dlPortRaw !== null && dlPortRaw !== undefined && dlPortRaw !== '') {
      const dlPort = Number(dlPortRaw);
      if (Number.isFinite(dlPort)) {
        configuration.udpDownlinkHost = dlHost;
        configuration.udpDownlinkPort = dlPort;
      }
    }
    this.propagateChange(configuration);
  }
}
