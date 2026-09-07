///
/// Copyright © 2016-2025 The Thingsboard Authors
///

import { Component, DestroyRef, forwardRef, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import {
  ControlValueAccessor,
  NG_VALIDATORS,
  NG_VALUE_ACCESSOR,
  UntypedFormArray,
  UntypedFormBuilder,
  UntypedFormControl,
  UntypedFormGroup,
  ValidationErrors,
  Validator,
  Validators
} from '@angular/forms';
import { Store } from '@ngrx/store';
import { AppState } from '@app/core/core.state';
import { ProtocolTemplateBundleService } from '@core/services/protocol-template-bundle.service';
import {
  DeviceProfileRpcBindingType,
  DeviceProfileRpcMethod,
  DeviceProfileTransportConfiguration,
  DeviceTransportType,
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateCommandDirection,
  TcpDeviceProfileTransportConfiguration,
  UdpDeviceProfileTransportConfiguration,
  TransportTcpDataType,
  TransportUdpDataType,
  isHttpPullProfileTransport,
  isProtocolTemplateWireTransport,
  resolveHttpProfileTransportTypeForDisplay,
  wireProfileProtocolTemplateBundleId,
  wireProfileTransportPayloadType
} from '@shared/models/device.models';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { guid } from '@core/utils';

@Component({
  selector: 'tb-device-profile-rpc-methods',
  templateUrl: './device-profile-rpc-methods.component.html',
  styleUrls: ['./device-profile-rpc-methods.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => DeviceProfileRpcMethodsComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => DeviceProfileRpcMethodsComponent),
      multi: true
    }
  ]
})
export class DeviceProfileRpcMethodsComponent implements ControlValueAccessor, OnInit, OnChanges, Validator {

  @Input() disabled: boolean;
  /** 详情页非编辑模式为 true，与表单 disable 双保险 */
  @Input() readOnly = false;
  @Input() transportType: DeviceTransportType;
  @Input() transportConfiguration: DeviceProfileTransportConfiguration | null;

  deviceProfileRpcMethodsFormGroup: UntypedFormGroup;

  downlinkCommands: ProtocolTemplateCommandDefinition[] = [];
  protocolTemplateBundleSelected = false;

  private propagateChange: (v: DeviceProfileRpcMethod[]) => void = () => undefined;
  private cvaDisabled = false;

  get effectiveTransportType(): DeviceTransportType {
    return resolveHttpProfileTransportTypeForDisplay(this.transportType, this.transportConfiguration);
  }

  constructor(private store: Store<AppState>,
              private fb: UntypedFormBuilder,
              private bundleService: ProtocolTemplateBundleService,
              private destroyRef: DestroyRef) {
  }

  get rpcMethodsFormArray(): UntypedFormArray {
    return this.deviceProfileRpcMethodsFormGroup.get('rpcMethods') as UntypedFormArray;
  }

  ngOnInit(): void {
    this.deviceProfileRpcMethodsFormGroup = this.fb.group({
      rpcMethods: this.fb.array([])
    });
    this.deviceProfileRpcMethodsFormGroup.valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(() => this.updateModel());
    this.refreshTemplateCommands();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.readOnly || changes.disabled) {
      this.syncDisabledState();
    }
    if (changes.transportConfiguration || changes.transportType) {
      this.refreshTemplateCommands();
    }
  }

  registerOnChange(fn: (v: DeviceProfileRpcMethod[]) => void): void {
    this.propagateChange = fn;
  }

  registerOnTouched(): void {
  }

  setDisabledState(isDisabled: boolean): void {
    this.cvaDisabled = isDisabled;
    this.syncDisabledState();
  }

  private syncDisabledState(): void {
    this.disabled = this.cvaDisabled || this.readOnly;
    this.applyDisabledToForm();
  }

  private applyDisabledToForm(): void {
    if (!this.deviceProfileRpcMethodsFormGroup) {
      return;
    }
    if (this.disabled) {
      this.deviceProfileRpcMethodsFormGroup.disable({ emitEvent: false });
    } else {
      this.deviceProfileRpcMethodsFormGroup.enable({ emitEvent: false });
    }
  }

  writeValue(value: DeviceProfileRpcMethod[] | null): void {
    if (!this.deviceProfileRpcMethodsFormGroup) {
      setTimeout(() => this.writeValue(value), 0);
      return;
    }
    this.rpcMethodsFormArray.clear({ emitEvent: false });
    const list = value ?? [];
    list.forEach((m, i) => {
      this.rpcMethodsFormArray.push(this.createRpcMethodControl(m), { emitEvent: false });
    });
    this.applyDisabledToForm();
  }

  addRpcMethod(): void {
    const binding = isProtocolTemplateWireTransport(this.effectiveTransportType)
      ? DeviceProfileRpcBindingType.TCP_TEMPLATE
      : (isHttpPullProfileTransport(this.transportType, this.transportConfiguration)
        ? DeviceProfileRpcBindingType.HTTP_OUTBOUND
        : DeviceProfileRpcBindingType.NATIVE);
    const draft: DeviceProfileRpcMethod = {
      id: `cmd_${guid().substring(0, 8)}`,
      oneWay: true,
      bindingType: binding
    };
    this.rpcMethodsFormArray.insert(0, this.createRpcMethodControl(draft));
  }

  removeRpcMethod(index: number): void {
    this.rpcMethodsFormArray.removeAt(index);
  }

  trackByIndex(index: number): number {
    return index;
  }

  hasDuplicateMethodIds(): boolean {
    const ids = new Set<string>();
    for (const c of this.rpcMethodsFormArray?.controls ?? []) {
      const id = (c.value as DeviceProfileRpcMethod)?.id?.trim();
      if (!id) {
        continue;
      }
      if (ids.has(id)) {
        return true;
      }
      ids.add(id);
    }
    return false;
  }

  validate(_control: UntypedFormControl): ValidationErrors | null {
    if (!this.deviceProfileRpcMethodsFormGroup) {
      return null;
    }
    const childInvalid = this.rpcMethodsFormArray.controls.some(c => c.invalid);
    if (childInvalid) {
      return { rpcMethods: { valid: false } };
    }
    const ids = new Set<string>();
    for (const c of this.rpcMethodsFormArray.controls) {
      const id = (c.value as DeviceProfileRpcMethod)?.id?.trim();
      if (!id) {
        continue;
      }
      if (ids.has(id)) {
        return { rpcMethodIdUnique: true };
      }
      ids.add(id);
    }
    return null;
  }

  private createRpcMethodControl(value: DeviceProfileRpcMethod): UntypedFormControl {
    return this.fb.control(value, { validators: Validators.required });
  }

  private updateModel(): void {
    const methods: DeviceProfileRpcMethod[] = this.rpcMethodsFormArray.controls
      .map(c => c.value as DeviceProfileRpcMethod)
      .filter(m => m && m.id);
    this.propagateChange(methods);
  }

  private refreshTemplateCommands(): void {
    this.downlinkCommands = [];
    this.protocolTemplateBundleSelected = false;
    if (!isProtocolTemplateWireTransport(this.effectiveTransportType) || !this.transportConfiguration) {
      return;
    }
    const bundleId = wireProfileProtocolTemplateBundleId(
      this.transportConfiguration as TcpDeviceProfileTransportConfiguration | UdpDeviceProfileTransportConfiguration);
    if (!bundleId) {
      return;
    }
    const payloadType = wireProfileTransportPayloadType(
      this.transportConfiguration as TcpDeviceProfileTransportConfiguration | UdpDeviceProfileTransportConfiguration);
    if (payloadType !== TransportTcpDataType.RAW_BYTES
      && payloadType !== TransportTcpDataType.PROTOCOL_TEMPLATE
      && payloadType !== TransportUdpDataType.RAW_BYTES
      && payloadType !== TransportUdpDataType.PROTOCOL_TEMPLATE) {
      return;
    }
    this.protocolTemplateBundleSelected = true;
    this.bundleService.getBundles().pipe(takeUntilDestroyed(this.destroyRef)).subscribe(bundles => {
      const b = bundles.find(x => x.id === bundleId);
      const cmds = b?.protocolCommands ?? b?.monitoringCommands ?? [];
      this.downlinkCommands = cmds.filter(c => c && (
        c.direction === ProtocolTemplateCommandDirection.DOWNLINK
        || c.direction === ProtocolTemplateCommandDirection.BOTH
      ));
    });
  }
}
