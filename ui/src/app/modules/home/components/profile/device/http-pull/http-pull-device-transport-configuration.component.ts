///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { ChangeDetectorRef, Component, forwardRef, Input, OnChanges, OnDestroy, OnInit, SimpleChanges } from '@angular/core';
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
  HttpPullDeviceTransportConfiguration,
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

  /** 档案级 pollUrl，用于将设备级「IP:端口」合并为完整拉取地址 */
  @Input()
  httpPullProfilePollUrl: string | null = null;

  form: UntypedFormGroup;

  private destroy$ = new Subject<void>();
  private propagateChange: (v: DeviceTransportConfiguration) => void = () => {};
  private pendingValue: HttpPullDeviceTransportConfiguration | null = null;
  private valueReady = false;

  get effectivePollUrl(): string | null {
    if (!this.form) {
      return null;
    }
    const raw = this.form.getRawValue().pollUrlOverride;
    const resolved = resolveHttpPullPollUrlOverride(raw, this.httpPullProfilePollUrl);
    return resolved || this.httpPullProfilePollUrl || null;
  }

  constructor(private fb: UntypedFormBuilder,
              private cd: ChangeDetectorRef) {}

  ngOnInit(): void {
    this.form = this.fb.group({
      pollUrlOverride: ['']
    });
    this.form.valueChanges.pipe(takeUntil(this.destroy$)).subscribe(() => {
      if (this.valueReady) {
        this.updateModel();
      }
    });
    this.applyPendingValue();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (!this.form) {
      return;
    }
    if (changes.httpPullProfilePollUrl) {
      this.refreshPollUrlOverrideDisplay();
    }
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
    if (!this.form) {
      return;
    }
    if (isDisabled) {
      this.form.disable({ emitEvent: false });
    } else {
      this.form.enable({ emitEvent: false });
      this.applyPendingValue();
    }
  }

  writeValue(value: DeviceTransportConfiguration | null): void {
    if (!value) {
      return;
    }
    const cfg = value as HttpPullDeviceTransportConfiguration;
    this.pendingValue = {
      ...cfg,
      pollUrlOverride: cfg.pollUrlOverride
    };
    this.applyPendingValue();
  }

  validate(): ValidationErrors | null {
    return this.form?.valid ? null : { httpPullDevice: true };
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

  private applyPendingValue(): void {
    if (!this.form || !this.pendingValue) {
      return;
    }
    this.valueReady = false;
    this.form.patchValue({
      pollUrlOverride: formatHttpPullPollUrlOverrideForDisplay(
        this.pendingValue.pollUrlOverride,
        this.httpPullProfilePollUrl
      )
    }, { emitEvent: false });
    this.valueReady = true;
    this.updateModel();
    this.cd.markForCheck();
  }

  private updateModel(): void {
    const v = this.form.getRawValue();
    const pollOverrideRaw = (v.pollUrlOverride || '').trim();
    this.propagateChange({
      pollUrlOverride: pollOverrideRaw || undefined,
      type: DeviceTransportType.HTTP_PULL
    });
  }
}
