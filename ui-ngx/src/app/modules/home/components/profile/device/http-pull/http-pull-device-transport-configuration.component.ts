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
  HttpPullRoutingMode,
  resolveHttpPullDeviceIsCollector,
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
  private pendingValue: HttpPullDeviceTransportConfiguration | null = null;
  private valueReady = false;

  get singleDeviceMode(): boolean {
    return this.httpPullRoutingMode === HttpPullRoutingMode.SINGLE_DEVICE;
  }

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

  isCollectorOn(): boolean {
    if (!this.form) {
      return true;
    }
    return resolveHttpPullDeviceIsCollector(this.form.getRawValue());
  }

  ngOnInit(): void {
    this.form = this.fb.group({
      collector: [true],
      externalDeviceId: [''],
      pollUrlOverride: ['']
    });
    this.form.valueChanges.pipe(takeUntil(this.destroy$)).subscribe(() => {
      if (this.valueReady) {
        this.updateModel();
      }
    });
    this.form.get('collector').valueChanges.pipe(takeUntil(this.destroy$)).subscribe((collector: boolean) => {
      if (collector) {
        this.form.patchValue({ externalDeviceId: '' }, { emitEvent: false });
      }
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
    if (changes.httpPullRoutingMode) {
      this.applyPendingValue();
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

  private applyPendingValue(): void {
    if (!this.form || !this.pendingValue) {
      return;
    }
    const deviceConfig = this.pendingValue;
    const singleDevice = this.singleDeviceMode;
    const collector = singleDevice ? true : resolveHttpPullDeviceIsCollector(deviceConfig);
    this.valueReady = false;
    this.form.patchValue({
      collector,
      externalDeviceId: singleDevice ? '' : (deviceConfig.externalDeviceId || ''),
      pollUrlOverride: collector ? formatHttpPullPollUrlOverrideForDisplay(
        deviceConfig.pollUrlOverride,
        this.httpPullProfilePollUrl
      ) : ''
    }, { emitEvent: false });
    const collectorCtrl = this.form.get('collector');
    if (singleDevice) {
      collectorCtrl?.disable({ emitEvent: false });
    } else {
      collectorCtrl?.enable({ emitEvent: false });
    }
    this.valueReady = true;
    this.cd.markForCheck();
  }

  private updateModel(): void {
    const v = this.form.getRawValue();
    const singleDevice = this.singleDeviceMode;
    const collector = singleDevice ? true : !!v.collector;
    const pollOverrideRaw = (v.pollUrlOverride || '').trim();
    const externalDeviceId = (v.externalDeviceId || '').trim();
    this.propagateChange({
      collector,
      externalDeviceId: !singleDevice && !collector ? (externalDeviceId || undefined) : undefined,
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
      collector: resolveHttpPullDeviceIsCollector(cfg),
      externalDeviceId: (cfg.externalDeviceId || '').trim() || undefined,
      pollUrlOverride: resolveHttpPullDeviceIsCollector(cfg) ? cfg.pollUrlOverride : undefined
    };
    this.applyPendingValue();
  }

  validate(): ValidationErrors | null {
    return this.form.valid ? null : { httpPullDevice: true };
  }
}
