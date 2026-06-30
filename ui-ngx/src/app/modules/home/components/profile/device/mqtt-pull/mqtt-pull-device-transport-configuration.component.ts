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
  Validator,
  Validators
} from '@angular/forms';
import { DeviceService } from '@core/http/device.service';
import {
  DeviceInfo,
  DeviceTransportConfiguration,
  DeviceTransportType,
  formatMqttPullBrokerUrlOverrideForDisplay,
  HttpPullRoutingMode,
  isMqttPullCollectorCandidate,
  MqttPullDeviceTransportConfiguration,
  resolveMqttPullBrokerUrlOverride,
  resolveMqttPullDeviceIsCollector
} from '@shared/models/device.models';
import { PageLink } from '@shared/models/page/page-link';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'tb-mqtt-pull-device-transport-configuration',
  templateUrl: './mqtt-pull-device-transport-configuration.component.html',
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => MqttPullDeviceTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => MqttPullDeviceTransportConfigurationComponent),
      multi: true
    }]
})
export class MqttPullDeviceTransportConfigurationComponent implements OnInit, OnChanges, OnDestroy, ControlValueAccessor, Validator {

  @Input()
  mqttPullRoutingMode: HttpPullRoutingMode | null = null;

  @Input()
  mqttPullProfileBrokerUrl: string | null = null;

  @Input()
  deviceProfileId: string | null = null;

  @Input()
  editingDeviceId: string | null = null;

  form: UntypedFormGroup;
  collectorOptions: DeviceInfo[] = [];

  private destroy$ = new Subject<void>();
  private propagateChange: (v: DeviceTransportConfiguration) => void = () => {};
  private pendingValue: MqttPullDeviceTransportConfiguration | null = null;
  private valueReady = false;

  get singleDeviceMode(): boolean {
    return this.mqttPullRoutingMode === HttpPullRoutingMode.SINGLE_DEVICE;
  }

  get showCollectorSelect(): boolean {
    return !this.singleDeviceMode && !this.isCollectorOn() && this.collectorOptions.length > 0;
  }

  get effectiveBrokerUrl(): string | null {
    if (!this.form) {
      return null;
    }
    const raw = this.form.getRawValue().brokerUrlOverride;
    const resolved = resolveMqttPullBrokerUrlOverride(raw, this.mqttPullProfileBrokerUrl);
    return resolved || this.mqttPullProfileBrokerUrl || null;
  }

  constructor(private fb: UntypedFormBuilder,
              private cd: ChangeDetectorRef,
              private deviceService: DeviceService) {}

  isCollectorOn(): boolean {
    if (!this.form) {
      return true;
    }
    return resolveMqttPullDeviceIsCollector(this.form.getRawValue());
  }

  ngOnInit(): void {
    this.form = this.fb.group({
      collector: [true],
      collectorDeviceId: [''],
      externalDeviceId: [''],
      brokerUrlOverride: ['']
    });
    this.form.valueChanges.pipe(takeUntil(this.destroy$)).subscribe(() => {
      if (this.valueReady) {
        this.updateModel();
      }
    });
    this.form.get('collector').valueChanges.pipe(takeUntil(this.destroy$)).subscribe((collector: boolean) => {
      if (collector) {
        this.form.patchValue({ externalDeviceId: '', collectorDeviceId: '' }, { emitEvent: false });
      }
      this.refreshCollectorDeviceIdValidators();
      if (this.valueReady) {
        this.updateModel();
      }
    });
    this.form.get('externalDeviceId').valueChanges.pipe(takeUntil(this.destroy$)).subscribe((externalDeviceId: string) => {
      if ((externalDeviceId || '').trim() && this.form.get('collector').value !== false) {
        this.form.patchValue({ collector: false }, { emitEvent: false });
      }
      if (this.valueReady) {
        this.updateModel();
      }
    });
    this.loadCollectorOptions();
    this.applyPendingValue();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (!this.form) {
      return;
    }
    if (changes.mqttPullRoutingMode) {
      this.applyPendingValue();
    }
    if (changes.mqttPullProfileBrokerUrl) {
      this.refreshBrokerUrlOverrideDisplay();
    }
    if (changes.deviceProfileId || changes.editingDeviceId) {
      this.loadCollectorOptions();
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
    const cfg = value as MqttPullDeviceTransportConfiguration;
    const isCollector = resolveMqttPullDeviceIsCollector(cfg);
    this.pendingValue = {
      ...cfg,
      collector: isCollector,
      collectorDeviceId: isCollector ? undefined : (cfg.collectorDeviceId || undefined),
      externalDeviceId: (cfg.externalDeviceId || '').trim() || undefined,
      brokerUrlOverride: isCollector ? cfg.brokerUrlOverride : undefined
    };
    this.applyPendingValue();
  }

  validate(): ValidationErrors | null {
    if (!this.form || this.singleDeviceMode || this.isCollectorOn()) {
      return null;
    }
    if (this.collectorOptions.length > 1 && !this.form.get('collectorDeviceId')?.value) {
      return { mqttPullCollectorRequired: true };
    }
    return this.form.valid ? null : { mqttPullDevice: true };
  }

  private loadCollectorOptions(): void {
    if (!this.deviceProfileId) {
      this.collectorOptions = [];
      this.refreshCollectorDeviceIdValidators();
      this.cd.markForCheck();
      return;
    }
    this.deviceService.getTenantDeviceInfosByDeviceProfileId(new PageLink(200), this.deviceProfileId)
      .pipe(takeUntil(this.destroy$))
      .subscribe(page => {
        this.collectorOptions = (page?.data || []).filter(device => {
          if (this.editingDeviceId && device.id?.id === this.editingDeviceId) {
            return false;
          }
          const tc = device.deviceData?.transportConfiguration as MqttPullDeviceTransportConfiguration | undefined;
          return isMqttPullCollectorCandidate(tc);
        });
        this.reconcileCollectorDeviceIdSelection();
        this.maybeAutoSelectCollector();
        this.refreshCollectorDeviceIdValidators();
        this.cd.markForCheck();
      });
  }

  private reconcileCollectorDeviceIdSelection(): void {
    if (!this.form || this.isCollectorOn()) {
      return;
    }
    const savedId = (this.pendingValue?.collectorDeviceId || this.form.get('collectorDeviceId')?.value || '').trim();
    if (!savedId) {
      return;
    }
    if (this.form.get('collectorDeviceId')?.value !== savedId) {
      this.form.patchValue({ collectorDeviceId: savedId }, { emitEvent: false });
    }
    if (this.collectorOptions.some(c => c.id?.id === savedId)) {
      return;
    }
    this.deviceService.getDevice(savedId).pipe(takeUntil(this.destroy$)).subscribe(device => {
      const tc = device?.deviceData?.transportConfiguration as MqttPullDeviceTransportConfiguration | undefined;
      if (!device?.id?.id || !isMqttPullCollectorCandidate(tc)) {
        return;
      }
      if (!this.collectorOptions.some(c => c.id?.id === device.id.id)) {
        this.collectorOptions = [...this.collectorOptions, device as DeviceInfo];
        this.refreshCollectorDeviceIdValidators();
        this.cd.markForCheck();
      }
    });
  }

  private maybeAutoSelectCollector(): void {
    if (!this.form || this.isCollectorOn() || this.collectorOptions.length !== 1) {
      return;
    }
    const current = this.form.get('collectorDeviceId')?.value;
    if (!current) {
      this.form.patchValue({ collectorDeviceId: this.collectorOptions[0].id.id }, { emitEvent: false });
      if (this.valueReady) {
        this.updateModel();
      }
    }
  }

  private refreshCollectorDeviceIdValidators(): void {
    const ctrl = this.form?.get('collectorDeviceId');
    if (!ctrl) {
      return;
    }
    if (this.showCollectorSelect && this.collectorOptions.length > 1) {
      ctrl.setValidators([Validators.required]);
    } else {
      ctrl.clearValidators();
    }
    ctrl.updateValueAndValidity({ emitEvent: false });
  }

  private refreshBrokerUrlOverrideDisplay(): void {
    const current = this.form.get('brokerUrlOverride')?.value;
    if (current == null || current === '') {
      return;
    }
    this.form.patchValue({
      brokerUrlOverride: formatMqttPullBrokerUrlOverrideForDisplay(current, this.mqttPullProfileBrokerUrl)
    }, { emitEvent: false });
  }

  private applyPendingValue(): void {
    if (!this.form || !this.pendingValue) {
      return;
    }
    const deviceConfig = this.pendingValue;
    const singleDevice = this.singleDeviceMode;
    const collector = singleDevice ? true : resolveMqttPullDeviceIsCollector(deviceConfig);
    this.valueReady = false;
    this.form.patchValue({
      collector,
      collectorDeviceId: collector ? '' : (deviceConfig.collectorDeviceId || ''),
      externalDeviceId: singleDevice ? '' : (deviceConfig.externalDeviceId || ''),
      brokerUrlOverride: collector ? formatMqttPullBrokerUrlOverrideForDisplay(
        deviceConfig.brokerUrlOverride,
        this.mqttPullProfileBrokerUrl
      ) : ''
    }, { emitEvent: false });
    const collectorCtrl = this.form.get('collector');
    if (singleDevice) {
      collectorCtrl?.disable({ emitEvent: false });
    } else {
      collectorCtrl?.enable({ emitEvent: false });
    }
    this.reconcileCollectorDeviceIdSelection();
    this.maybeAutoSelectCollector();
    this.refreshCollectorDeviceIdValidators();
    this.valueReady = true;
    this.updateModel();
    this.cd.markForCheck();
  }

  private updateModel(): void {
    const v = this.form.getRawValue();
    const singleDevice = this.singleDeviceMode;
    const collector = singleDevice ? true : resolveMqttPullDeviceIsCollector(v);
    const brokerOverrideRaw = (v.brokerUrlOverride || '').trim();
    const externalDeviceId = (v.externalDeviceId || '').trim();
    const collectorDeviceId = (v.collectorDeviceId || '').trim();
    if (!singleDevice && v.collector !== collector) {
      this.form.patchValue({ collector }, { emitEvent: false });
    }
    this.propagateChange({
      collector,
      collectorDeviceId: !singleDevice && !collector ? (collectorDeviceId || undefined) : undefined,
      externalDeviceId: !singleDevice && !collector ? (externalDeviceId || undefined) : undefined,
      brokerUrlOverride: collector && brokerOverrideRaw ? brokerOverrideRaw : undefined,
      type: DeviceTransportType.MQTT_PULL
    });
  }
}
