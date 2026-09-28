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
    if (this.tcpDeviceTransportConfigurationFormGroup
        && prev === UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID
        && mode !== UdpWireAuthenticationMode.DEFERRED_PAYLOAD_DEVICE_ID) {
      this.tcpDeviceTransportConfigurationFormGroup.patchValue(
        { udpWireAuthPayloadDeviceId: '' },
        { emitEvent: true }
      );
    }
    this.applyDeferredPayloadDeviceIdValidators();
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
      // 固定下行地址（可选）：设备从临时/NAT 端口上报、但固定端口收指令时填写
      udpDownlinkHost: [''],
      udpDownlinkPort: [null, [Validators.min(1), Validators.max(65535)]]
    });
    this.tcpDeviceTransportConfigurationFormGroup.valueChanges.pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(() => {
      this.applyDownlinkPairValidators();
      this.updateModel();
    });
    this.applyDeferredPayloadDeviceIdValidators();
    this.applyDownlinkPairValidators();
  }

  /**
   * 固定下行地址与端口必须成对填写：只填一个时把另一个标成必填，
   * 校验不通过时 {@link #updateModel} 会放弃这次变更（不会把半个地址存进去）。
   */
  private applyDownlinkPairValidators(): void {
    const grp = this.tcpDeviceTransportConfigurationFormGroup;
    const hostCtrl = grp.get('udpDownlinkHost');
    const portCtrl = grp.get('udpDownlinkPort');
    const hostSet = String(hostCtrl.value ?? '').trim().length > 0;
    const portVal = portCtrl.value;
    const portSet = portVal !== null && portVal !== undefined && portVal !== '';
    hostCtrl.setValidators(portSet && !hostSet ? [Validators.required] : []);
    portCtrl.setValidators(hostSet && !portSet
      ? [Validators.required, Validators.min(1), Validators.max(65535)]
      : [Validators.min(1), Validators.max(65535)]);
    hostCtrl.updateValueAndValidity({emitEvent: false});
    portCtrl.updateValueAndValidity({emitEvent: false});
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
    this.applyDeferredPayloadDeviceIdValidators();
    this.applyDownlinkPairValidators();
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
