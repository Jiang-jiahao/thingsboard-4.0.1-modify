///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { Component, forwardRef, Input, OnChanges, OnDestroy, OnInit, SimpleChanges } from '@angular/core';
import {
  ControlValueAccessor,
  NG_VALIDATORS,
  NG_VALUE_ACCESSOR,
  UntypedFormBuilder,
  UntypedFormGroup,
  ValidationErrors,
  Validator
} from '@angular/forms';
import {
  DeviceTransportConfiguration,
  DeviceTransportType,
  formatHttpPullPollUrlOverrideForDisplay,
  HttpPullRoutingMode,
  resolveHttpPullPollUrlOverride
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-http-pull-device-transport-configuration',
  templateUrl: './http-pull-device-transport-configuration.component.html',
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => HttpPullDeviceTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => HttpPullDeviceTransportConfigurationComponent),
      multi: true
    }]
})
export class HttpPullDeviceTransportConfigurationComponent implements OnInit, OnChanges, OnDestroy, ControlValueAccessor, Validator {

  @Input()
  httpPullRoutingMode: HttpPullRoutingMode | null = null;

  /** 档案级 pollUrl，用于将设备级「IP:端口」合并为完整拉取地址 */
  @Input()
  httpPullProfilePollUrl: string | null = null;

  form: UntypedFormGroup;
  private destroy$ = new Subject<void>();
  private propagateChange: (v: DeviceTransportConfiguration) => void = () => {};

  get singleDeviceMode(): boolean {
    return this.httpPullRoutingMode === HttpPullRoutingMode.SINGLE_DEVICE
      || this.httpPullRoutingMode == null;
  }

  get effectivePollUrl(): string | null {
    if (!this.form) {
      return null;
    }
    const raw = this.form.getRawValue().pollUrlOverride;
    const resolved = resolveHttpPullPollUrlOverride(raw, this.httpPullProfilePollUrl);
    return resolved || this.httpPullProfilePollUrl || null;
  }

  constructor(private fb: UntypedFormBuilder) {}

  ngOnInit(): void {
    this.form = this.fb.group({
      collector: [true],
      externalDeviceId: [''],
      pollUrlOverride: ['']
    });
    this.form.valueChanges.pipe(takeUntil(this.destroy$)).subscribe(() => this.updateModel());
    this.form.get('collector').valueChanges.pipe(takeUntil(this.destroy$)).subscribe((collector: boolean) => {
      if (collector) {
        this.form.patchValue({ externalDeviceId: '' }, { emitEvent: false });
      }
      this.updateModel();
    });
    this.applyRoutingModeToForm();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (!this.form) {
      return;
    }
    if (changes.httpPullRoutingMode) {
      this.applyRoutingModeToForm();
      this.updateModel();
    }
    if (changes.httpPullProfilePollUrl) {
      this.refreshPollUrlOverrideDisplay();
    }
  }

  private refreshPollUrlOverrideDisplay(): void {
    const current = this.form.get('pollUrlOverride')?.value;
    if (current == null || current === '') {
      return;
    }
    this.form.patchValue({
      pollUrlOverride: formatHttpPullPollUrlOverrideForDisplay(current, this.httpPullProfilePollUrl)
    }, { emitEvent: false });
  }

  private applyRoutingModeToForm(): void {
    if (!this.form) {
      return;
    }
    if (this.singleDeviceMode) {
      this.form.patchValue({ collector: true, externalDeviceId: '' }, { emitEvent: false });
      this.form.get('collector').disable({ emitEvent: false });
    } else {
      this.form.get('collector').enable({ emitEvent: false });
    }
  }

  private updateModel(): void {
    const v = this.form.getRawValue();
    const collector = this.singleDeviceMode ? true : !!v.collector;
    const pollOverrideRaw = (v.pollUrlOverride || '').trim();
    this.propagateChange({
      collector,
      externalDeviceId: (!collector && !this.singleDeviceMode) ? (v.externalDeviceId || undefined) : undefined,
      pollUrlOverride: collector && pollOverrideRaw ? pollOverrideRaw : undefined,
      type: DeviceTransportType.HTTP_PULL
    });
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
    if (isDisabled) {
      this.form.disable({ emitEvent: false });
    } else {
      this.form.enable({ emitEvent: false });
    }
  }

  writeValue(value: DeviceTransportConfiguration | null): void {
    if (!value) {
      return;
    }
    const { type, ...deviceConfig } = value;
    this.form.patchValue({
      ...deviceConfig,
      pollUrlOverride: formatHttpPullPollUrlOverrideForDisplay(
        deviceConfig.pollUrlOverride,
        this.httpPullProfilePollUrl
      )
    }, { emitEvent: false });
  }

  validate(): ValidationErrors | null {
    return this.form.valid ? null : { httpPullDevice: true };
  }
}
