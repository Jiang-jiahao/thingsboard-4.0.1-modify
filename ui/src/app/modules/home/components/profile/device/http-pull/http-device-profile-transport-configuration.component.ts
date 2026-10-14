import { Component, forwardRef, Input, OnChanges, OnDestroy, OnInit, SimpleChanges } from '@angular/core';
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
import {
  createDeviceProfileTransportConfiguration,
  DeviceProfileTransportConfiguration,
  DeviceTransportType,
  HttpTransportMode,
  isHttpPullProfileTransportConfiguration,
  resolveHttpProfileTransportTypeForDisplay
} from '@shared/models/device.models';
import { Subject } from 'rxjs';
import { filter, takeUntil } from 'rxjs/operators';

@Component({
  selector: 'jnks-iot-http-device-profile-transport-configuration',
  templateUrl: './http-device-profile-transport-configuration.component.html',
  styleUrls: ['./http-device-profile-transport-configuration.component.scss'],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => HttpDeviceProfileTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => HttpDeviceProfileTransportConfigurationComponent),
      multi: true
    }
  ]
})
export class HttpDeviceProfileTransportConfigurationComponent implements OnInit, OnChanges, OnDestroy, ControlValueAccessor, Validator {

  @Input() disabled: boolean;
  /** 档案已保存的 transportType，用于配置 JSON 缺少 type 时恢复工作模式 */
  @Input() entityTransportType: DeviceTransportType;
  /** 父级判定：必须为 HTTP Pull 档案时强制展示主动拉取面板 */
  @Input() httpPullActive = false;
  /** 新建档案（或刚切换传输类型）：不预选工作模式，必须手选 */
  @Input() isAdd = false;

  form: UntypedFormGroup;
  httpTransportMode = HttpTransportMode;

  private destroy$ = new Subject<void>();
  private propagateChange: (v: DeviceProfileTransportConfiguration | null) => void = () => {};
  private onValidatorChange: () => void = () => {};
  private pendingValue: DeviceProfileTransportConfiguration | null = null;
  private formReady = false;
  /** 用户是否手动选过工作模式（新建时用来区分「预填的拉取形状」与「用户的选择」） */
  private modeChosen = false;

  constructor(private fb: UntypedFormBuilder) {}

  ngOnInit(): void {
    this.form = this.fb.group({
      // 默认「被动接收」，不空着：空着既是无效态、保存也会被拦。
      // （后端要求工作模式显式声明，所以这里给的是真实选择，不是占位。）
      httpMode: [HttpTransportMode.PASSIVE, Validators.required],
      pullConfiguration: [createDeviceProfileTransportConfiguration(DeviceTransportType.HTTP_PULL)],
      passiveConfiguration: [createDeviceProfileTransportConfiguration(DeviceTransportType.DEFAULT)]
    });
    this.form.get('httpMode').valueChanges.pipe(takeUntil(this.destroy$)).subscribe(() => {
      this.modeChosen = true;
      this.updateModel();
    });
    this.form.get('pullConfiguration').valueChanges.pipe(
      takeUntil(this.destroy$),
      filter(() => this.form.get('httpMode').value === HttpTransportMode.PULL)
    ).subscribe(() => this.updateModel());
    this.form.get('passiveConfiguration').valueChanges.pipe(
      takeUntil(this.destroy$),
      filter(() => this.form.get('httpMode').value === HttpTransportMode.PASSIVE)
    ).subscribe(() => this.updateModel());
    this.formReady = true;
    this.applyPendingValue();
    // 没有待回显的值（新建档案 / 刚切到 HTTP）时要把默认值报给父级 ——
    // 否则父级的 transportConfiguration 仍是 null，保存会被它的 Validators.required 拦住。
    if (!this.pendingValue) {
      this.updateModel();
    }
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.entityTransportType || changes.httpPullActive || changes.isAdd) {
      this.applyPendingValue();
    }
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  registerOnChange(fn: any): void {
    this.propagateChange = fn;
  }

  registerOnValidatorChange(fn: () => void): void {
    this.onValidatorChange = fn;
  }

  registerOnTouched(_fn: any): void {}

  setDisabledState(isDisabled: boolean): void {
    if (isDisabled) {
      this.form.disable({ emitEvent: false });
    } else {
      this.form.enable({ emitEvent: false });
    }
  }

  writeValue(value: DeviceProfileTransportConfiguration | null): void {
    this.pendingValue = value ?? null;
    if (!this.pendingValue) {
      return;
    }
    this.applyPendingValue();
    // 嵌套 CVA（pull/passive 子面板）可能尚未挂载，延迟再应用一次
    setTimeout(() => this.applyPendingValue(), 0);
  }

  private applyPendingValue(): void {
    if (!this.formReady || !this.form || !this.pendingValue) {
      return;
    }
    const value = this.pendingValue;
    const mode = this.resolveMode(value);
    this.form.patchValue({
      httpMode: mode,
      pullConfiguration: mode === HttpTransportMode.PULL ? value : createDeviceProfileTransportConfiguration(DeviceTransportType.HTTP_PULL),
      passiveConfiguration: mode === HttpTransportMode.PASSIVE ? value : createDeviceProfileTransportConfiguration(DeviceTransportType.DEFAULT)
    }, { emitEvent: false });
    // 「未选工作模式」是一处无效态，得让父级重跑校验，否则保存不会被拦
    this.onValidatorChange();
  }

  /**
   * 解析应回显的工作模式。
   * <p>
   * 已保存的档案按配置里的标记/形状回显；新建档案（或刚切换传输类型）默认**被动接收** ——
   * 不能靠"拉取形状"去推断（HTTP 的默认配置本身就带 poll-1，推成主动采集会误导），
   * 但也不能留空（空的值会被后端以"工作模式必须显式声明"拒绝，父级也会因 required 存不了）。
   * 父级若已判定必须是拉取档案（`httpPullActive`），则默认主动采集。
   */
  private resolveMode(value: DeviceProfileTransportConfiguration): HttpTransportMode {
    if (this.isAdd && !this.modeChosen) {
      return this.httpPullActive ? HttpTransportMode.PULL : HttpTransportMode.PASSIVE;
    }
    if (value.httpTransportMode === HttpTransportMode.PULL) {
      return HttpTransportMode.PULL;
    }
    if (value.httpTransportMode === HttpTransportMode.PASSIVE) {
      return HttpTransportMode.PASSIVE;
    }
    const pull = this.httpPullActive
      || isHttpPullProfileTransportConfiguration(value)
      || resolveHttpProfileTransportTypeForDisplay(this.entityTransportType, value as Record<string, unknown>)
        === DeviceTransportType.HTTP_PULL;
    return pull ? HttpTransportMode.PULL : HttpTransportMode.PASSIVE;
  }

  validate(): ValidationErrors | null {
    if (this.form.get('httpMode').value == null) {
      return { httpMode: true };
    }
    if (this.form.get('httpMode').value === HttpTransportMode.PULL) {
      return this.form.get('pullConfiguration').valid ? null : { httpPull: true };
    }
    return null;
  }

  private updateModel(): void {
    const mode = this.form.get('httpMode').value as HttpTransportMode | null;
    if (mode == null) {
      this.pendingValue = null;
      this.propagateChange(null);
    } else if (mode === HttpTransportMode.PULL) {
      const pull = this.form.get('pullConfiguration').value || {};
      const configuration: DeviceProfileTransportConfiguration = {
        ...pull,
        type: DeviceTransportType.HTTP_PULL,
        httpTransportMode: HttpTransportMode.PULL
      };
      this.pendingValue = configuration;
      this.propagateChange(configuration);
    } else {
      const configuration: DeviceProfileTransportConfiguration = {
        type: DeviceTransportType.DEFAULT,
        httpTransportMode: HttpTransportMode.PASSIVE
      };
      this.pendingValue = configuration;
      this.propagateChange(configuration);
    }
    this.onValidatorChange();
  }
}
