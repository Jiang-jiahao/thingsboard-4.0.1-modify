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
  selector: 'jnks-iot-tcp-device-transport-configuration',
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
   * {@link TcpWireAuthenticationMode#DEFERRED_PAYLOAD_DEVICE_ID} 时显示协议设备 ID；表单项显隐见各 getter。
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
    this.runWithoutModelUpdate(() => this.applyDeferredPayloadDeviceIdValidators());
  }

  get showTcpWireAuthPayloadDeviceId(): boolean {
    return this.tcpWireAuthMode === TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID;
  }

  /** 设备档案 TCP CLIENT/SERVER；与延迟链路上鉴权组合用于隐藏无意义的 CLIENT 表单项 */
  @Input()
  set tcpProfileTransportConnectMode(mode: TcpTransportConnectMode | null) {
    this.tcpProfileTransportConnectModeValue = mode;
    const grp = this.tcpDeviceTransportConfigurationFormGroup;
    // CLIENT 档案下 sourceHost 无意义；只在**确实填过值**时才清空回写 ——
    // 无条件 patchValue(emitEvent:true) 会在加载阶段凭空产生一次"改动"，把父表单标脏。
    if (grp && mode === TcpTransportConnectMode.CLIENT
        && String(grp.get('sourceHost').value ?? '').length > 0) {
      grp.patchValue({ sourceHost: '' }, { emitEvent: true });
    }
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

  /** DEFERRED_PAYLOAD_DEVICE_ID（链路上延迟解析协议设备号） */
  private isDeferredPayloadWireAuthMode(): boolean {
    return this.tcpWireAuthMode === TcpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID;
  }

  /** 延迟协议设备 ID，且非显式 CLIENT：入站场景，隐藏 CLIENT/源 IP/通用提示 */
  private isDeferredPayloadWireAuthInboundMode(): boolean {
    return this.isDeferredPayloadWireAuthMode()
      && this.tcpProfileTransportConnectModeValue !== TcpTransportConnectMode.CLIENT;
  }

  private propagateChange = (v: any) => { };

  /** 见 {@link #runWithoutModelUpdate}：初始化/写入期间抑制回写，否则父表单会被标脏。 */
  private suppressModelUpdate = false;

  /**
   * 初始化、{@link #writeValue} 以及档案输入变化期间**不能**回写模型。
   * <p>
   * `applyDeferredPayloadDeviceIdValidators()` 用 `updateValueAndValidity({emitEvent: true})` 刷新校验，
   * 这会触发本组件的 valueChanges；若此时调 {@link #updateModel} → `propagateChange(...)`，就等于在加载阶段
   * 往父表单的 `configuration` 控件写了一次值。Angular 把这次子→父写入记成"用户改动"，
   * 于是刚打开设备详情、什么都没编辑，顶层表单已经是 `ng-dirty` —— 离开时会弹「有未保存的更改」。
   * <p>
   * `emitEvent: true` 本身要保留：父级靠它重跑校验，只是不该顺带回写值。
   */
  private runWithoutModelUpdate(action: () => void): void {
    this.suppressModelUpdate = true;
    try {
      action();
    } finally {
      this.suppressModelUpdate = false;
    }
  }
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
      tcpWireAuthPayloadDeviceId: ['']
    });
    this.tcpDeviceTransportConfigurationFormGroup.valueChanges.pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(() => {
      if (!this.suppressModelUpdate) {
        this.updateModel();
      }
    });
    this.runWithoutModelUpdate(() => this.applyDeferredPayloadDeviceIdValidators());
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
