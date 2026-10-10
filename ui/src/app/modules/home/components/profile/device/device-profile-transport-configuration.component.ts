import { Component, DestroyRef, forwardRef, Input, OnInit } from '@angular/core';
import {
  ControlValueAccessor,
  NG_VALIDATORS,
  NG_VALUE_ACCESSOR,
  UntypedFormBuilder,
  UntypedFormControl,
  UntypedFormGroup,
  ValidationErrors,
  Validator
} from '@angular/forms';
import { Store } from '@ngrx/store';
import { AppState } from '@app/core/core.state';
import {
  BasicTransportType,
  DeviceProfileTransportConfiguration,
  DeviceTransportType,
  normalizeProfileTransportConfigurationForDisplay,
  resolveHttpProfileTransportTypeForDisplay,
  resolveMqttProfileTransportTypeForDisplay,
  toUiTransportType,
  TransportType
} from '@shared/models/device.models';
import { deepClone } from '@core/utils';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
  selector: 'jnks-iot-device-profile-transport-configuration',
  templateUrl: './device-profile-transport-configuration.component.html',
  styleUrls: [],
  providers: [
    {
      provide: NG_VALUE_ACCESSOR,
      useExisting: forwardRef(() => DeviceProfileTransportConfigurationComponent),
      multi: true
    },
    {
      provide: NG_VALIDATORS,
      useExisting: forwardRef(() => DeviceProfileTransportConfigurationComponent),
      multi: true,
    }
  ]
})
export class DeviceProfileTransportConfigurationComponent implements ControlValueAccessor, OnInit, Validator {

  deviceTransportType = DeviceTransportType;
  basicTransportType = BasicTransportType;

  deviceProfileTransportConfigurationFormGroup: UntypedFormGroup;

  @Input()
  disabled: boolean;

  @Input()
  isAdd: boolean;

  /** 设备配置页选择的传输类型（优先；与 configuration.type 解耦，避免 HTTP 被动模式切走 HTTP 面板） */
  @Input()
  selectedTransportType: TransportType;

  /** 已保存的档案 transportType（HTTP 回显工作模式） */
  @Input()
  entityTransportType: DeviceTransportType;

  private configurationTransportType: TransportType;

  private propagateChange = (v: any) => { };

  get activeTransportType(): TransportType | null {
    return toUiTransportType((this.selectedTransportType ?? this.configurationTransportType) as DeviceTransportType);
  }

  /** 档案 transportType=HTTP_PULL 或配置标记为主动拉取时强制展示主动拉取面板 */
  get httpPullActive(): boolean {
    const cfg = this.deviceProfileTransportConfigurationFormGroup?.getRawValue()?.configuration;
    return resolveHttpProfileTransportTypeForDisplay(this.entityTransportType, cfg) === DeviceTransportType.HTTP_PULL;
  }

  /** 档案 transportType=MQTT_PULL 或配置标记为主动拉取时强制展示主动拉取面板 */
  get mqttPullActive(): boolean {
    const cfg = this.deviceProfileTransportConfigurationFormGroup?.getRawValue()?.configuration;
    return resolveMqttProfileTransportTypeForDisplay(this.entityTransportType, cfg) === DeviceTransportType.MQTT_PULL;
  }

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
    this.deviceProfileTransportConfigurationFormGroup = this.fb.group({
      configuration: [null]
    });
    this.deviceProfileTransportConfigurationFormGroup.valueChanges.pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(() => {
      // isAdd（新建档案 / 刚切换传输类型）时不抑制：此时「用户没选完」这类无效态必须能传到上层，
      // 否则保存拦不住；防脏只对「打开已保存档案」这件场景有意义。
      if (!this.suppressModelUpdate || this.isAdd) {
        this.updateModel();
      }
    });
  }

  /**
   * 加载期**不能**回写模型。
   * <p>
   * 子组件（TCP/UDP 的档案传输配置）在 `writeValue()` 里会调 `notifyValidatorChange()` 让本组件的
   * `configuration` 控件重跑校验；`updateValueAndValidity()` 会发射 valueChanges，于是走到本组件的
   * `updateModel() → propagateChange(configuration)` —— 在加载阶段往自己的宿主控件
   * （`profileData.transportConfiguration`）写了一次值，Angular 记成"用户改动"，
   * 打开档案页点「编辑」、什么都没改，顶层表单就是 `ng-dirty`，保存按钮直接可点。
   * <p>
   * **每次写入都重新起算**，并在**写入安静下来之后**（见 {@link #SUPPRESSION_QUIET_MS}）才放开：
   * 一次加载会分几轮写入 —— 本组件一轮、子组件在 `setTimeout(0)` 里再一轮，
   * 只包住同步那一段挡不住后面那轮（踩过：包同步段后点编辑仍然 dirty）。
   */
  private static readonly SUPPRESSION_QUIET_MS = 50;
  private suppressModelUpdate = true;
  private releaseTimer: ReturnType<typeof setTimeout> | null = null;

  private armModelUpdateSuppression(): void {
    this.suppressModelUpdate = true;
    if (this.releaseTimer !== null) {
      clearTimeout(this.releaseTimer);
    }
    this.releaseTimer = setTimeout(() => {
      this.suppressModelUpdate = false;
    }, DeviceProfileTransportConfigurationComponent.SUPPRESSION_QUIET_MS);
  }

  setDisabledState(isDisabled: boolean): void {
    this.disabled = isDisabled;
    if (this.disabled) {
      this.deviceProfileTransportConfigurationFormGroup.disable({emitEvent: false});
    } else {
      this.deviceProfileTransportConfigurationFormGroup.enable({emitEvent: false});
    }
  }

  writeValue(value: DeviceProfileTransportConfiguration | null): void {
    this.armModelUpdateSuppression();
    if (!value) {
      return;
    }
    const effectiveTransportType = resolveHttpProfileTransportTypeForDisplay(
      this.entityTransportType,
      value as Record<string, unknown>
    );
    this.configurationTransportType = resolveMqttProfileTransportTypeForDisplay(
      effectiveTransportType,
      value as Record<string, unknown>
    );
    const configuration = normalizeProfileTransportConfigurationForDisplay(
      this.configurationTransportType,
      deepClone(value) as Record<string, unknown>
    );
    const patchConfiguration = () => {
      if (this.deviceProfileTransportConfigurationFormGroup) {
        this.deviceProfileTransportConfigurationFormGroup.patchValue({configuration}, {emitEvent: false});
      }
    };
    patchConfiguration();
    // HTTP 子组件在 ngSwitch 下晚于首次 writeValue 挂载，补一次写入（抑制期由 arm 的静默计时兜住）
    setTimeout(() => patchConfiguration(), 0);
  }

  private updateModel() {
    if (this.suppressModelUpdate && !this.isAdd) {
      return;
    }
    const configuration = this.deviceProfileTransportConfigurationFormGroup.getRawValue().configuration;
    if (configuration == null) {
      return;
    }
    this.propagateChange(configuration);
  }

  public validate(c: UntypedFormControl): ValidationErrors | null {
    // 嵌套子 CVA（HTTP/TCP/UDP 等）的内部表单不在本组件的控制树里，
    // 不主动触发的话，子组件的无效态（如「HTTP 工作模式未选」）反映不到 group.valid 上，保存就拦不住。
    this.deviceProfileTransportConfigurationFormGroup.get('configuration')?.updateValueAndValidity({ emitEvent: false });
    return (this.deviceProfileTransportConfigurationFormGroup.valid) ? null : {
      configuration: {
        valid: false,
      },
    };
  }
}
