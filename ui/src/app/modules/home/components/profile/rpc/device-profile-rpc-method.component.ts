///
/// Copyright © 2016-2025 The Thingsboard Authors
///

import { Component, DestroyRef, EventEmitter, forwardRef, Input, OnChanges, OnInit, Output, SimpleChanges } from '@angular/core';
import {
  ControlValueAccessor,
  NG_VALIDATORS,
  NG_VALUE_ACCESSOR,
  UntypedFormBuilder,
  UntypedFormControl,
  UntypedFormGroup,
  ValidationErrors,
  Validator,
  Validators
} from '@angular/forms';
import {
  DeviceProfileRpcBindingType,
  DeviceProfileRpcMethod,
  DeviceProfileTransportConfiguration,
  DeviceTransportType,
  isHttpOutboundRpcBinding,
  isHttpPullProfileTransport,
  isMqttCustomRpcBinding,
  isMqttProfileRpcTransport,
  isMqttPullProfileTransport,
  isProtocolTemplateWireTransport,
  isProtocolTemplateRpcBinding,
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateCommandDirection,
  resolveHttpProfileTransportTypeForDisplay
} from '@shared/models/device.models';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

export function protocolTemplateCommandRef(c: ProtocolTemplateCommandDefinition): string {
  return `${c.templateId ?? ''}\u0001${c.commandValue ?? 0}\u0001${c.name ?? ''}`;
}

export function parseProtocolTemplateCommandRef(ref: string): Pick<DeviceProfileRpcMethod, 'templateId' | 'commandValue' | 'templateCommandName'> | null {
  if (!ref) {
    return null;
  }
  const parts = ref.split('\u0001');
  if (parts.length < 2) {
    return null;
  }
  const templateId = parts[0];
  const commandValue = Number(parts[1]);
  const templateCommandName = parts[2] || undefined;
  if (!templateId || Number.isNaN(commandValue)) {
    return null;
  }
  return { templateId, commandValue, templateCommandName: templateCommandName || undefined };
}

@Component({
  selector: 'tb-device-profile-rpc-method',
  templateUrl: './device-profile-rpc-method.component.html',
  styleUrls: ['./device-profile-rpc-method.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => DeviceProfileRpcMethodComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => DeviceProfileRpcMethodComponent),
      multi: true
    }
  ]
})
export class DeviceProfileRpcMethodComponent implements ControlValueAccessor, OnInit, OnChanges, Validator {

  readonly DeviceProfileRpcBindingType = DeviceProfileRpcBindingType;
  readonly DeviceTransportType = DeviceTransportType;

  @Input() disabled: boolean;
  @Input() readOnly = false;
  @Input() expanded = false;
  @Input() transportType: DeviceTransportType;
  @Input() transportConfiguration: DeviceProfileTransportConfiguration | null;
  @Input() downlinkCommands: ProtocolTemplateCommandDefinition[] = [];
  @Input() protocolTemplateBundleSelected = false;

  @Output() removeRpcMethod = new EventEmitter<void>();

  rpcMethodFormGroup: UntypedFormGroup;

  private propagateChange: (v: DeviceProfileRpcMethod | null) => void = () => undefined;

  constructor(private fb: UntypedFormBuilder,
              private destroyRef: DestroyRef) {
  }

  get templateBindingOnly(): boolean {
    return isProtocolTemplateWireTransport(this.effectiveTransportType);
  }

  get effectiveTransportType(): DeviceTransportType {
    return resolveHttpProfileTransportTypeForDisplay(this.transportType, this.transportConfiguration);
  }

  get httpPullRpcTransport(): boolean {
    return isHttpPullProfileTransport(this.transportType, this.transportConfiguration);
  }

  get mqttRpcTransport(): boolean {
    return isMqttProfileRpcTransport(this.transportType, this.transportConfiguration);
  }

  get mqttPullRpcTransport(): boolean {
    return isMqttPullProfileTransport(this.transportType, this.transportConfiguration);
  }

  get httpOutboundBinding(): boolean {
    const bt = this.rpcMethodFormGroup?.get('bindingType')?.value as DeviceProfileRpcBindingType;
    return this.httpPullRpcTransport && isHttpOutboundRpcBinding(bt);
  }

  get mqttCustomBinding(): boolean {
    const bt = this.rpcMethodFormGroup?.get('bindingType')?.value as DeviceProfileRpcBindingType;
    return this.mqttRpcTransport && isMqttCustomRpcBinding(bt);
  }

  get nativeBindingOnly(): boolean {
    if (this.templateBindingOnly) {
      return false;
    }
    if (this.httpPullRpcTransport || this.mqttRpcTransport) {
      const bt = this.rpcMethodFormGroup?.get('bindingType')?.value as DeviceProfileRpcBindingType;
      return bt === DeviceProfileRpcBindingType.NATIVE;
    }
    return true;
  }

  get bindingTypeSelectable(): boolean {
    return this.httpPullRpcTransport || this.mqttRpcTransport;
  }

  readonly httpPullRpcBindingTypes = [
    DeviceProfileRpcBindingType.HTTP_OUTBOUND,
    DeviceProfileRpcBindingType.NATIVE
  ];

  readonly mqttRpcBindingTypes = [
    DeviceProfileRpcBindingType.NATIVE,
    DeviceProfileRpcBindingType.MQTT_CUSTOM
  ];

  get isLocked(): boolean {
    return !!this.disabled || this.readOnly;
  }

  commandLabel(c: ProtocolTemplateCommandDefinition): string {
    const name = c.name?.trim();
    if (name) {
      return `${name} (${c.templateId}, 0x${(c.commandValue >>> 0).toString(16)})`;
    }
    return `${c.templateId}, 0x${(c.commandValue >>> 0).toString(16)}`;
  }

  commandRef(c: ProtocolTemplateCommandDefinition): string {
    return protocolTemplateCommandRef(c);
  }

  registerOnChange(fn: (v: DeviceProfileRpcMethod | null) => void): void {
    this.propagateChange = fn;
  }

  registerOnTouched(): void {
  }

  ngOnChanges(changes: SimpleChanges): void {
    if ((changes.readOnly || changes.transportType || changes.transportConfiguration) && this.rpcMethodFormGroup) {
      this.applyFormDisabledState();
      if (changes.transportType || changes.transportConfiguration) {
        const bt = this.rpcMethodFormGroup.get('bindingType').value as DeviceProfileRpcBindingType;
        this.applyBindingFieldState(bt);
      }
    }
  }

  ngOnInit(): void {
    const defaultBinding = this.templateBindingOnly
      ? DeviceProfileRpcBindingType.TCP_TEMPLATE
      : (this.httpPullRpcTransport ? DeviceProfileRpcBindingType.HTTP_OUTBOUND : DeviceProfileRpcBindingType.NATIVE);

    this.rpcMethodFormGroup = this.fb.group({
      id: ['', [Validators.required, Validators.pattern(/^[a-zA-Z][a-zA-Z0-9_]*$/)]],
      displayName: [''],
      oneWay: [true],
      timeoutMs: [null as number | null],
      bindingType: [defaultBinding],
      templateCommandRef: [''],
      paramMapJson: ['{}'],
      deviceMethod: [''],
      paramsTemplateJson: [''],
      httpUrl: [''],
      httpMethod: ['POST'],
      httpBody: [''],
      httpHeadersJson: ['{}'],
      requiresAuth: [null as boolean | null],
      mqttRequestTopic: [''],
      mqttResponseTopic: [''],
      mqttPayloadTemplate: [''],
      mqttQos: [1]
    });

    this.applyBindingFieldState(defaultBinding);

    this.rpcMethodFormGroup.get('bindingType').valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe((bt: DeviceProfileRpcBindingType) => this.applyBindingFieldState(bt));
    this.rpcMethodFormGroup.get('oneWay').valueChanges.pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(() => {
        this.applyMqttResponseTopicValidator();
        this.rpcMethodFormGroup.get('mqttResponseTopic')?.updateValueAndValidity({ emitEvent: false });
      });

    this.rpcMethodFormGroup.valueChanges.pipe(takeUntilDestroyed(this.destroyRef)).subscribe(() => this.updateModel());
    this.applyFormDisabledState();
  }

  setDisabledState(isDisabled: boolean): void {
    this.disabled = isDisabled;
    this.applyFormDisabledState();
  }

  private applyFormDisabledState(): void {
    const locked = this.disabled || this.readOnly;
    if (!this.rpcMethodFormGroup) {
      return;
    }
    if (locked) {
      this.rpcMethodFormGroup.disable({ emitEvent: false });
      return;
    }
    this.rpcMethodFormGroup.enable({ emitEvent: false });
    if (!this.bindingTypeSelectable) {
      this.rpcMethodFormGroup.get('bindingType').disable({ emitEvent: false });
    }
    const bt = this.rpcMethodFormGroup.get('bindingType').value as DeviceProfileRpcBindingType;
    this.applyBindingFieldState(bt);
  }

  private applyBindingFieldState(bindingType: DeviceProfileRpcBindingType): void {
    if (!this.rpcMethodFormGroup) {
      return;
    }
    const httpFields = ['httpUrl', 'httpMethod', 'httpBody', 'httpHeadersJson', 'requiresAuth'];
    const nativeFields = ['deviceMethod', 'paramsTemplateJson'];
    const templateFields = ['templateCommandRef', 'paramMapJson'];
    const mqttTopicFields = ['mqttRequestTopic', 'mqttResponseTopic', 'mqttQos'];
    const mqttCustomFields = ['mqttPayloadTemplate'];

    const disableAll = (fields: string[]) => fields.forEach(f => this.rpcMethodFormGroup.get(f)?.disable({ emitEvent: false }));
    const enableAll = (fields: string[]) => fields.forEach(f => this.rpcMethodFormGroup.get(f)?.enable({ emitEvent: false }));

    disableAll([...httpFields, ...nativeFields, ...templateFields, ...mqttTopicFields, ...mqttCustomFields]);

    this.rpcMethodFormGroup.get('mqttRequestTopic').clearValidators();
    this.rpcMethodFormGroup.get('mqttResponseTopic').clearValidators();
    this.rpcMethodFormGroup.get('mqttPayloadTemplate').clearValidators();

    if (this.templateBindingOnly) {
      enableAll(templateFields);
      this.rpcMethodFormGroup.get('templateCommandRef').setValidators([Validators.required]);
      this.rpcMethodFormGroup.get('deviceMethod').clearValidators();
      this.rpcMethodFormGroup.get('httpUrl').clearValidators();
    } else if (this.httpPullRpcTransport && isHttpOutboundRpcBinding(bindingType)) {
      enableAll(httpFields);
      this.rpcMethodFormGroup.get('httpUrl').setValidators([Validators.required]);
      this.rpcMethodFormGroup.get('httpMethod').setValidators([Validators.required]);
      this.rpcMethodFormGroup.get('deviceMethod').clearValidators();
    } else if (this.mqttRpcTransport && isMqttCustomRpcBinding(bindingType)) {
      enableAll([...mqttTopicFields, ...mqttCustomFields]);
      this.rpcMethodFormGroup.get('mqttRequestTopic').setValidators([Validators.required]);
      this.rpcMethodFormGroup.get('mqttPayloadTemplate').setValidators([Validators.required]);
      this.rpcMethodFormGroup.get('deviceMethod').clearValidators();
      this.rpcMethodFormGroup.get('httpUrl').clearValidators();
      this.rpcMethodFormGroup.get('httpMethod').clearValidators();
      this.applyMqttResponseTopicValidator();
    } else {
      enableAll(nativeFields);
      this.rpcMethodFormGroup.get('deviceMethod').setValidators([Validators.required]);
      this.rpcMethodFormGroup.get('httpUrl').clearValidators();
      this.rpcMethodFormGroup.get('httpMethod').clearValidators();
      if (this.mqttRpcTransport) {
        enableAll(mqttTopicFields);
        if (this.mqttPullRpcTransport) {
          this.rpcMethodFormGroup.get('mqttRequestTopic').setValidators([Validators.required]);
        }
        this.applyMqttResponseTopicValidator();
      }
    }
    this.rpcMethodFormGroup.get('deviceMethod').updateValueAndValidity({ emitEvent: false });
    this.rpcMethodFormGroup.get('httpUrl').updateValueAndValidity({ emitEvent: false });
    this.rpcMethodFormGroup.get('httpMethod').updateValueAndValidity({ emitEvent: false });
    this.rpcMethodFormGroup.get('templateCommandRef').updateValueAndValidity({ emitEvent: false });
    this.rpcMethodFormGroup.get('mqttRequestTopic').updateValueAndValidity({ emitEvent: false });
    this.rpcMethodFormGroup.get('mqttResponseTopic').updateValueAndValidity({ emitEvent: false });
    this.rpcMethodFormGroup.get('mqttPayloadTemplate').updateValueAndValidity({ emitEvent: false });
  }

  private applyMqttResponseTopicValidator(): void {
    const oneWay = this.rpcMethodFormGroup.get('oneWay')?.value !== false;
    const responseCtrl = this.rpcMethodFormGroup.get('mqttResponseTopic');
    if (!oneWay && (this.mqttCustomBinding || this.mqttPullRpcTransport)) {
      responseCtrl?.setValidators([Validators.required]);
    } else {
      responseCtrl?.clearValidators();
    }
  }

  writeValue(value: DeviceProfileRpcMethod | null): void {
    if (!this.rpcMethodFormGroup) {
      setTimeout(() => this.writeValue(value), 0);
      return;
    }
    if (!value) {
      return;
    }
    const ref = value.templateId != null && value.commandValue != null
      ? `${value.templateId}\u0001${value.commandValue}\u0001${value.templateCommandName ?? ''}`
      : '';
    let paramMapJson = '{}';
    if (value.paramMap && Object.keys(value.paramMap).length) {
      paramMapJson = JSON.stringify(value.paramMap);
    }
    let httpHeadersJson = '{}';
    if (value.httpHeaders && Object.keys(value.httpHeaders).length) {
      httpHeadersJson = JSON.stringify(value.httpHeaders);
    }
    this.rpcMethodFormGroup.patchValue({
      id: value.id ?? '',
      displayName: value.displayName ?? '',
      oneWay: value.oneWay !== false,
      timeoutMs: value.timeoutMs ?? null,
      bindingType: value.bindingType ?? (this.templateBindingOnly
        ? DeviceProfileRpcBindingType.TCP_TEMPLATE
        : (this.httpPullRpcTransport ? DeviceProfileRpcBindingType.HTTP_OUTBOUND : DeviceProfileRpcBindingType.NATIVE)),
      templateCommandRef: ref,
      paramMapJson,
      deviceMethod: value.deviceMethod ?? '',
      paramsTemplateJson: value.paramsTemplateJson ?? '',
      httpUrl: value.httpUrl ?? '',
      httpMethod: value.httpMethod ?? 'POST',
      httpBody: value.httpBody ?? '',
      httpHeadersJson,
      requiresAuth: value.requiresAuth ?? null,
      mqttRequestTopic: value.mqttRequestTopic ?? '',
      mqttResponseTopic: value.mqttResponseTopic ?? '',
      mqttPayloadTemplate: value.mqttPayloadTemplate ?? '',
      mqttQos: value.mqttQos ?? 1
    }, { emitEvent: false });
    this.applyFormDisabledState();
  }

  private updateModel(): void {
    const raw = this.rpcMethodFormGroup.getRawValue();
    let bindingType = raw.bindingType as DeviceProfileRpcBindingType;
    if (this.templateBindingOnly) {
      bindingType = DeviceProfileRpcBindingType.TCP_TEMPLATE;
    } else if (!this.httpPullRpcTransport && !this.mqttRpcTransport) {
      bindingType = DeviceProfileRpcBindingType.NATIVE;
    }

    const out: DeviceProfileRpcMethod = {
      id: (raw.id as string)?.trim(),
      displayName: (raw.displayName as string)?.trim() || undefined,
      oneWay: !!raw.oneWay,
      timeoutMs: raw.timeoutMs != null && raw.timeoutMs !== '' ? Number(raw.timeoutMs) : undefined,
      bindingType
    };

    if (isProtocolTemplateRpcBinding(bindingType)) {
      const parsed = parseProtocolTemplateCommandRef(raw.templateCommandRef);
      if (parsed) {
        out.templateId = parsed.templateId;
        out.commandValue = parsed.commandValue;
        out.templateCommandName = parsed.templateCommandName;
      }
      const pm = parseJsonObject(raw.paramMapJson);
      if (pm) {
        out.paramMap = pm;
      }
    } else if (isHttpOutboundRpcBinding(bindingType)) {
      out.httpUrl = (raw.httpUrl as string)?.trim();
      out.httpMethod = (raw.httpMethod as string)?.trim() || 'POST';
      const body = (raw.httpBody as string)?.trim();
      out.httpBody = body || undefined;
      const headers = parseJsonObject(raw.httpHeadersJson);
      if (headers && Object.keys(headers).length) {
        out.httpHeaders = headers;
      }
      if (raw.requiresAuth === true || raw.requiresAuth === false) {
        out.requiresAuth = raw.requiresAuth;
      }
    } else if (isMqttCustomRpcBinding(bindingType)) {
      out.mqttRequestTopic = (raw.mqttRequestTopic as string)?.trim();
      const resp = (raw.mqttResponseTopic as string)?.trim();
      out.mqttResponseTopic = resp || undefined;
      out.mqttPayloadTemplate = (raw.mqttPayloadTemplate as string)?.trim();
      out.mqttQos = raw.mqttQos != null && raw.mqttQos !== '' ? Number(raw.mqttQos) : 1;
      const pt = (raw.paramsTemplateJson as string)?.trim();
      out.paramsTemplateJson = pt || undefined;
    } else {
      out.deviceMethod = (raw.deviceMethod as string)?.trim();
      const pt = (raw.paramsTemplateJson as string)?.trim();
      out.paramsTemplateJson = pt || undefined;
      if (this.mqttRpcTransport) {
        const req = (raw.mqttRequestTopic as string)?.trim();
        out.mqttRequestTopic = req || undefined;
        const resp = (raw.mqttResponseTopic as string)?.trim();
        out.mqttResponseTopic = resp || undefined;
        out.mqttQos = raw.mqttQos != null && raw.mqttQos !== '' ? Number(raw.mqttQos) : 1;
      }
    }

    this.propagateChange(out);
  }

  validate(): ValidationErrors | null {
    if (!this.rpcMethodFormGroup) {
      return null;
    }
    if (this.rpcMethodFormGroup.invalid) {
      return { rpcMethod: { valid: false } };
    }
    const raw = this.rpcMethodFormGroup.getRawValue();
    if (this.templateBindingOnly) {
      if (parseJsonObject(raw.paramMapJson) === null && (raw.paramMapJson as string)?.trim()) {
        return { paramMapJson: true };
      }
    } else if (this.httpOutboundBinding) {
      if (parseJsonObject(raw.httpHeadersJson) === null && (raw.httpHeadersJson as string)?.trim()) {
        return { httpHeadersJson: true };
      }
    } else if (this.mqttCustomBinding) {
      if (!(raw.mqttRequestTopic as string)?.trim()) {
        return { mqttRequestTopic: true };
      }
      if (!(raw.mqttPayloadTemplate as string)?.trim()) {
        return { mqttPayloadTemplate: true };
      }
    } else {
      const pt = (raw.paramsTemplateJson as string)?.trim();
      if (pt && !isValidJson(pt)) {
        return { paramsTemplateJson: true };
      }
    }
    return null;
  }

  onRemove(): void {
    this.removeRpcMethod.emit();
  }
}

function parseJsonObject(text: string): Record<string, string> | null {
  const t = (text ?? '').trim();
  if (!t || t === '{}') {
    return {};
  }
  try {
    const v = JSON.parse(t);
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      const out: Record<string, string> = {};
      for (const k of Object.keys(v)) {
        out[k] = String(v[k]);
      }
      return out;
    }
    return null;
  } catch {
    return null;
  }
}

function isValidJson(text: string): boolean {
  try {
    JSON.parse(text);
    return true;
  } catch {
    return false;
  }
}
