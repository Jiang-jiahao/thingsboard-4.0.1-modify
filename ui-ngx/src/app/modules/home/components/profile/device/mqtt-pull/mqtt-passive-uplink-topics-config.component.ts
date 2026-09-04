///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { Component, forwardRef, Input, OnDestroy, OnInit } from '@angular/core';
import {
  AbstractControl,
  ControlValueAccessor,
  NG_VALIDATORS,
  NG_VALUE_ACCESSOR,
  UntypedFormArray,
  UntypedFormBuilder,
  UntypedFormControl,
  UntypedFormGroup,
  ValidationErrors,
  Validator,
  ValidatorFn,
  Validators
} from '@angular/forms';
import {
  HttpPullPollDataType,
  MqttUplinkTopicMapping
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-mqtt-passive-uplink-topics-config',
  templateUrl: './mqtt-passive-uplink-topics-config.component.html',
  styleUrls: ['./mqtt-passive-uplink-topics-config.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => MqttPassiveUplinkTopicsConfigComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => MqttPassiveUplinkTopicsConfigComponent),
      multi: true
    }
  ]
})
export class MqttPassiveUplinkTopicsConfigComponent implements OnInit, OnDestroy, ControlValueAccessor, Validator {

  @Input() disabled: boolean;

  form: UntypedFormGroup;
  dataTypes = Object.keys(HttpPullPollDataType);

  private destroy$ = new Subject<void>();
  private propagateChange: (v: MqttUplinkTopicMapping[]) => void = () => {};

  constructor(private fb: UntypedFormBuilder) {}

  get mappingsArray(): UntypedFormArray {
    return this.form.get('mappings') as UntypedFormArray;
  }

  ngOnInit(): void {
    this.form = this.fb.group({
      mappings: this.fb.array([])
    }, { validators: this.uniqueEnabledTopicsValidator });
    this.form.valueChanges.pipe(takeUntil(this.destroy$)).subscribe(() => this.updateModel());
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  registerOnChange(fn: any): void {
    this.propagateChange = fn;
  }

  registerOnTouched(_fn: any): void {}

  setDisabledState(isDisabled: boolean): void {
    this.disabled = isDisabled;
    this.applyDisabledState();
  }

  writeValue(value: MqttUplinkTopicMapping[] | null): void {
    if (!this.form) {
      return;
    }
    this.mappingsArray.clear({ emitEvent: false });
    const mappings = value?.length ? value : [this.createDefaultMapping()];
    mappings.forEach(m => this.mappingsArray.push(this.createMappingGroup(m), { emitEvent: false }));
    this.form.updateValueAndValidity({ emitEvent: false });
    this.applyDisabledState();
  }

  validate(): ValidationErrors | null {
    return this.form.valid && this.mappingsArray.length > 0 ? null : { uplinkTopicMappings: true };
  }

  addMapping(): void {
    this.mappingsArray.push(this.createMappingGroup(this.createDefaultMapping()));
  }

  removeMapping(index: number): void {
    if (this.mappingsArray.length > 1) {
      this.mappingsArray.removeAt(index);
    }
  }

  showTelemetryKey(mapping: UntypedFormGroup): boolean {
    return mapping.get('dataType')?.value === HttpPullPollDataType.TELEMETRY;
  }

  private createDefaultMapping(): MqttUplinkTopicMapping {
    return {
      id: this.newId(),
      name: `topic-${this.mappingsArray.length + 1}`,
      enabled: true,
      topic: '',
      dataType: HttpPullPollDataType.TELEMETRY,
      telemetryPayloadKey: ''
    };
  }

  private createMappingGroup(m: MqttUplinkTopicMapping): UntypedFormGroup {
    return this.fb.group({
      id: [m.id || this.newId()],
      name: [m.name || ''],
      enabled: [m.enabled !== false],
      topic: [m.topic || '', [Validators.required, this.validationMQTTTopic()]],
      dataType: [m.dataType || HttpPullPollDataType.TELEMETRY, Validators.required],
      telemetryPayloadKey: [m.telemetryPayloadKey || '']
    });
  }

  private updateModel(): void {
    const mappings: MqttUplinkTopicMapping[] = this.mappingsArray.getRawValue().map((v: any) => ({
      id: v.id,
      name: v.name || undefined,
      enabled: v.enabled,
      topic: v.topic,
      dataType: v.dataType,
      telemetryPayloadKey: v.telemetryPayloadKey || undefined
    }));
    this.propagateChange(mappings);
  }

  private applyDisabledState(): void {
    if (!this.form) {
      return;
    }
    if (this.disabled) {
      this.form.disable({ emitEvent: false });
    } else {
      this.form.enable({ emitEvent: false });
    }
  }

  private uniqueEnabledTopicsValidator: ValidatorFn = (control: AbstractControl): ValidationErrors | null => {
    const mappings = (control.get('mappings') as UntypedFormArray)?.getRawValue() as MqttUplinkTopicMapping[];
    if (!mappings?.length) {
      return null;
    }
    const seen = new Set<string>();
    for (const mapping of mappings) {
      if (mapping.enabled === false || !mapping.topic) {
        continue;
      }
      if (seen.has(mapping.topic)) {
        return { uniqueUplinkTopics: true };
      }
      seen.add(mapping.topic);
    }
    return null;
  };

  private validationMQTTTopic(): ValidatorFn {
    return (c: UntypedFormControl) => {
      const newTopic = c.value;
      if (!newTopic) {
        return null;
      }
      const wildcardSymbols = /[#+]/g;
      let findSymbol = wildcardSymbols.exec(newTopic);
      while (findSymbol) {
        const index = findSymbol.index;
        const currentSymbol = findSymbol[0];
        const prevSymbol = index > 0 ? newTopic[index - 1] : null;
        const nextSymbol = index < (newTopic.length - 1) ? newTopic[index + 1] : null;
        if (currentSymbol === '#' && (index !== (newTopic.length - 1) || (prevSymbol !== null && prevSymbol !== '/'))) {
          return { invalidMultiTopicCharacter: { valid: false } };
        }
        if (currentSymbol === '+' && ((prevSymbol !== null && prevSymbol !== '/') || (nextSymbol !== null && nextSymbol !== '/'))) {
          return { invalidSingleTopicCharacter: { valid: false } };
        }
        findSymbol = wildcardSymbols.exec(newTopic);
      }
      return null;
    };
  }

  private newId(): string {
    return typeof crypto !== 'undefined' && crypto.randomUUID
      ? crypto.randomUUID()
      : `uplink-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
  }
}
