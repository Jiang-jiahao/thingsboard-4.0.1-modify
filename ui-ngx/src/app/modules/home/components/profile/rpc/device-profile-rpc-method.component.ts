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
  DeviceTransportType,
  isProtocolTemplateWireTransport,
  isProtocolTemplateRpcBinding,
  ProtocolTemplateCommandDefinition,
  ProtocolTemplateCommandDirection
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
  @Input() downlinkCommands: ProtocolTemplateCommandDefinition[] = [];
  @Input() protocolTemplateBundleSelected = false;

  @Output() removeRpcMethod = new EventEmitter<void>();

  rpcMethodFormGroup: UntypedFormGroup;

  private propagateChange: (v: DeviceProfileRpcMethod | null) => void = () => undefined;

  constructor(private fb: UntypedFormBuilder,
              private destroyRef: DestroyRef) {
  }

  get templateBindingOnly(): boolean {
    return isProtocolTemplateWireTransport(this.transportType);
  }

  get nativeBindingOnly(): boolean {
    return !isProtocolTemplateWireTransport(this.transportType);
  }

  /** 查看模式或表单 CVA 禁用 */
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
    if (changes.readOnly && this.rpcMethodFormGroup) {
      this.applyFormDisabledState();
    }
  }

  ngOnInit(): void {
    const defaultBinding = this.templateBindingOnly
      ? DeviceProfileRpcBindingType.TCP_TEMPLATE
      : DeviceProfileRpcBindingType.NATIVE;

    this.rpcMethodFormGroup = this.fb.group({
      id: ['', [Validators.required, Validators.pattern(/^[a-zA-Z][a-zA-Z0-9_]*$/)]],
      displayName: [''],
      oneWay: [true],
      timeoutMs: [null as number | null],
      bindingType: [{ value: defaultBinding, disabled: true }],
      templateCommandRef: [''],
      paramMapJson: ['{}'],
      deviceMethod: [''],
      paramsTemplateJson: ['']
    });

    if (this.templateBindingOnly) {
      this.rpcMethodFormGroup.get('templateCommandRef').setValidators([Validators.required]);
      this.rpcMethodFormGroup.get('deviceMethod').disable({ emitEvent: false });
      this.rpcMethodFormGroup.get('paramsTemplateJson').disable({ emitEvent: false });
    } else {
      this.rpcMethodFormGroup.get('deviceMethod').setValidators([Validators.required]);
      this.rpcMethodFormGroup.get('templateCommandRef').disable({ emitEvent: false });
      this.rpcMethodFormGroup.get('paramMapJson').disable({ emitEvent: false });
    }

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
    this.rpcMethodFormGroup.get('bindingType').disable({ emitEvent: false });
    if (this.templateBindingOnly) {
      this.rpcMethodFormGroup.get('deviceMethod').disable({ emitEvent: false });
      this.rpcMethodFormGroup.get('paramsTemplateJson').disable({ emitEvent: false });
      this.rpcMethodFormGroup.get('templateCommandRef').enable({ emitEvent: false });
      this.rpcMethodFormGroup.get('paramMapJson').enable({ emitEvent: false });
    } else {
      this.rpcMethodFormGroup.get('templateCommandRef').disable({ emitEvent: false });
      this.rpcMethodFormGroup.get('paramMapJson').disable({ emitEvent: false });
      this.rpcMethodFormGroup.get('deviceMethod').enable({ emitEvent: false });
      this.rpcMethodFormGroup.get('paramsTemplateJson').enable({ emitEvent: false });
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
    this.rpcMethodFormGroup.patchValue({
      id: value.id ?? '',
      displayName: value.displayName ?? '',
      oneWay: value.oneWay !== false,
      timeoutMs: value.timeoutMs ?? null,
      bindingType: value.bindingType ?? (this.templateBindingOnly ? DeviceProfileRpcBindingType.TCP_TEMPLATE : DeviceProfileRpcBindingType.NATIVE),
      templateCommandRef: ref,
      paramMapJson,
      deviceMethod: value.deviceMethod ?? '',
      paramsTemplateJson: value.paramsTemplateJson ?? ''
    }, { emitEvent: false });
    this.applyFormDisabledState();
  }

  private updateModel(): void {
    const raw = this.rpcMethodFormGroup.getRawValue();
    const bindingType = this.templateBindingOnly
      ? DeviceProfileRpcBindingType.TCP_TEMPLATE
      : DeviceProfileRpcBindingType.NATIVE;

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
    } else {
      out.deviceMethod = (raw.deviceMethod as string)?.trim();
      const pt = (raw.paramsTemplateJson as string)?.trim();
      out.paramsTemplateJson = pt || undefined;
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
