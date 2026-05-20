///
/// Copyright © 2016-2025 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { ChangeDetectorRef, Component, ViewChild } from '@angular/core';
import { MatDialogRef } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { DialogComponent } from '@shared/components/dialog.component';
import { Router } from '@angular/router';
import {
  createDeviceConfiguration,
  createDeviceTransportConfiguration,
  Device,
  DeviceData,
  DeviceProfile,
  DeviceProfileInfo,
  DeviceProfileType,
  DeviceTransportType,
  TcpDeviceProfileTransportConfiguration,
  TcpTransportConnectMode,
  TcpWireAuthenticationMode
} from '@shared/models/device.models';
import { MatStepper, StepperOrientation } from '@angular/material/stepper';
import { EntityType } from '@shared/models/entity-type.models';
import { Observable, throwError } from 'rxjs';
import { catchError, map } from 'rxjs/operators';
import { DeviceService } from '@core/http/device.service';
import { StepperSelectionEvent } from '@angular/cdk/stepper';
import { BreakpointObserver } from '@angular/cdk/layout';
import { MediaBreakpoints } from '@shared/models/constants';
import { deepTrim } from '@core/utils';
import { CustomerId } from '@shared/models/id/customer-id';
import { HttpErrorResponse } from '@angular/common/http';
import { DeviceProfileService } from '@core/http/device-profile.service';
import { DeviceProfileId } from '@shared/models/id/device-profile-id';

@Component({
  selector: 'tb-device-wizard',
  templateUrl: './device-wizard-dialog.component.html',
  styleUrls: ['./device-wizard-dialog.component.scss']
})
export class DeviceWizardDialogComponent extends DialogComponent<DeviceWizardDialogComponent, Device> {

  @ViewChild('addDeviceWizardStepper', {static: true}) addDeviceWizardStepper: MatStepper;

  stepperOrientation: Observable<StepperOrientation>;

  stepperLabelPosition: Observable<'bottom' | 'end'>;

  selectedIndex = 0;

  credentialsOptionalStep = true;

  showNext = true;

  entityType = EntityType;

  deviceWizardFormGroup: FormGroup;

  credentialsFormGroup: FormGroup;

  private currentDeviceProfileTransportType = DeviceTransportType.DEFAULT;

  /** 与设备详情页一致，供 tb-device-data 展示传输配置（含 TCP 协议设备 ID） */
  tcpProfileWireAuthMode: TcpWireAuthenticationMode | null = null;

  tcpProfileTransportConnectMode: TcpTransportConnectMode | null = null;

  readonly deviceWizardDeviceScope: 'tenant' = 'tenant';

  constructor(protected store: Store<AppState>,
              protected router: Router,
              public dialogRef: MatDialogRef<DeviceWizardDialogComponent, Device>,
              private deviceService: DeviceService,
              private breakpointObserver: BreakpointObserver,
              private fb: FormBuilder,
              private deviceProfileService: DeviceProfileService,
              private cd: ChangeDetectorRef) {
    super(store, router, dialogRef);

    this.stepperOrientation = this.breakpointObserver.observe(MediaBreakpoints['gt-sm'])
      .pipe(map(({matches}) => matches ? 'horizontal' : 'vertical'));

    this.stepperLabelPosition = this.breakpointObserver.observe(MediaBreakpoints['gt-sm'])
      .pipe(map(({matches}) => matches ? 'end' : 'bottom'));

    this.deviceWizardFormGroup = this.fb.group({
        name: ['', [Validators.required, Validators.maxLength(255)]],
        label: ['', Validators.maxLength(255)],
        gateway: [false],
        overwriteActivityTime: [false],
        customerId: [null as CustomerId | null],
        deviceProfileId: [null, Validators.required],
        deviceData: [null as DeviceData | null, Validators.required],
        description: ['']
      }
    );

    this.credentialsFormGroup  = this.fb.group({
        credential: []
      }
    );
  }

  cancel(): void {
    this.dialogRef.close(null);
  }

  previousStep(): void {
    this.addDeviceWizardStepper.previous();
  }

  nextStep(): void {
    this.addDeviceWizardStepper.next();
  }

  getFormLabel(index: number): string {
    switch (index) {
      case 0:
        return 'device.wizard.device-details';
      case 1:
        return 'device.credentials';
    }
  }

  get maxStepperIndex(): number {
    return this.addDeviceWizardStepper?._steps?.length - 1;
  }

  add(): void {
    if (this.allValid()) {
      this.createDevice().subscribe(
        (device) => this.dialogRef.close(device)
      );
    }
  }

  get deviceTransportType(): DeviceTransportType {
    return this.currentDeviceProfileTransportType;
  }

  /**
   * 与设备详情页 onDeviceProfileChanged 对齐：拉取完整档案以得到 transportType 与 TCP 链路上鉴权模式，
   * 并初始化 deviceData（添加设备向导原先不采集 deviceData，导致 TCP DEFERRED 等无法配置）。
   */
  deviceProfileChanged(deviceProfile: DeviceProfileInfo | null): void {
    if (!deviceProfile) {
      this.applyTcpProfileWireAuthFromDeviceProfile(null);
      this.currentDeviceProfileTransportType = DeviceTransportType.DEFAULT;
      this.credentialsOptionalStep = true;
      this.deviceWizardFormGroup.patchValue({ deviceData: null }, { emitEvent: false });
      this.cd.markForCheck();
      return;
    }
    const prev: DeviceData = this.deviceWizardFormGroup.getRawValue().deviceData;
    const apply = (profileType: DeviceProfileType, transportType: DeviceTransportType) => {
      if (!transportType) {
        return;
      }
      if (!prev) {
        const deviceData: DeviceData = {
          configuration: createDeviceConfiguration(profileType),
          transportConfiguration: createDeviceTransportConfiguration(transportType)
        };
        this.deviceWizardFormGroup.patchValue({ deviceData });
        return;
      }
      let next: DeviceData = prev;
      if (prev.configuration?.type !== profileType) {
        next = {...next, configuration: createDeviceConfiguration(profileType)};
      }
      if (prev.transportConfiguration?.type !== transportType) {
        next = {
          ...next,
          transportConfiguration: createDeviceTransportConfiguration(transportType)
        };
      }
      if (next !== prev) {
        this.deviceWizardFormGroup.patchValue({ deviceData: next });
      }
    };
    const profileType = deviceProfile.type;
    const transportTypeFromAutocomplete = deviceProfile.transportType;
    const uuid = this.resolveDeviceProfileUuid(deviceProfile);
    if (!uuid) {
      this.applyTcpProfileWireAuthFromDeviceProfile(null);
      apply(profileType, transportTypeFromAutocomplete);
      this.currentDeviceProfileTransportType = transportTypeFromAutocomplete ?? DeviceTransportType.DEFAULT;
      this.credentialsOptionalStep = this.currentDeviceProfileTransportType !== DeviceTransportType.LWM2M;
      this.cd.markForCheck();
      return;
    }
    this.deviceProfileService.getDeviceProfile(uuid).subscribe({
      next: (dp: DeviceProfile) => {
        this.applyTcpProfileWireAuthFromDeviceProfile(dp);
        const resolvedProfileType = dp?.type ?? profileType;
        const resolvedTransportType = dp?.transportType ?? transportTypeFromAutocomplete;
        apply(resolvedProfileType, resolvedTransportType);
        this.currentDeviceProfileTransportType = resolvedTransportType ?? DeviceTransportType.DEFAULT;
        this.credentialsOptionalStep = this.currentDeviceProfileTransportType !== DeviceTransportType.LWM2M;
        this.cd.markForCheck();
      },
      error: () => {
        this.applyTcpProfileWireAuthFromDeviceProfile(null);
        apply(profileType, transportTypeFromAutocomplete);
        this.currentDeviceProfileTransportType = transportTypeFromAutocomplete ?? DeviceTransportType.DEFAULT;
        this.credentialsOptionalStep = this.currentDeviceProfileTransportType !== DeviceTransportType.LWM2M;
        this.cd.markForCheck();
      }
    });
  }

  private resolveDeviceProfileUuid(profileRef: DeviceProfileInfo | DeviceProfileId | null | undefined): string | null {
    if (profileRef == null || profileRef.id == null) {
      return null;
    }
    const idField = profileRef.id as string | { id?: string };
    if (typeof idField === 'string') {
      return idField.length ? idField : null;
    }
    if (typeof idField === 'object' && typeof idField.id === 'string' && idField.id.length) {
      return idField.id;
    }
    return null;
  }

  private applyTcpProfileWireAuthFromDeviceProfile(dp: DeviceProfile | null): void {
    if (!dp || dp.transportType !== DeviceTransportType.TCP) {
      this.tcpProfileWireAuthMode = null;
      this.tcpProfileTransportConnectMode = null;
      return;
    }
    const raw = dp.profileData?.transportConfiguration as TcpDeviceProfileTransportConfiguration | undefined;
    if (raw && (raw.type === DeviceTransportType.TCP || raw.type == null || raw.type === undefined)) {
      this.tcpProfileWireAuthMode = raw.tcpWireAuthenticationMode ?? null;
      this.tcpProfileTransportConnectMode = raw.tcpTransportConnectMode ?? null;
    } else {
      this.tcpProfileWireAuthMode = null;
      this.tcpProfileTransportConnectMode = null;
    }
  }

  private createDevice(): Observable<Device> {
    const device: Device = {
      name: this.deviceWizardFormGroup.get('name').value,
      label: this.deviceWizardFormGroup.get('label').value,
      deviceProfileId: this.deviceWizardFormGroup.get('deviceProfileId').value,
      deviceData: this.deviceWizardFormGroup.get('deviceData').value,
      additionalInfo: {
        gateway: this.deviceWizardFormGroup.get('gateway').value,
        overwriteActivityTime: this.deviceWizardFormGroup.get('overwriteActivityTime').value,
        description: this.deviceWizardFormGroup.get('description').value
      },
      customerId: this.deviceWizardFormGroup.get('customerId').value
    };
    if (this.addDeviceWizardStepper.steps.last.completed || this.addDeviceWizardStepper.selectedIndex > 0) {
      return this.deviceService.saveDeviceWithCredentials(deepTrim(device), deepTrim(this.credentialsFormGroup.value.credential)).pipe(
        catchError((e: HttpErrorResponse) => {
          if (e.error.message.includes('Device credentials')) {
            this.addDeviceWizardStepper.selectedIndex = 1;
          } else {
            this.addDeviceWizardStepper.selectedIndex = 0;
          }
          return throwError(() => e);
        })
      );
    }
    return this.deviceService.saveDevice(deepTrim(device)).pipe(
      catchError(e => {
        this.addDeviceWizardStepper.selectedIndex = 0;
        return throwError(e);
      })
    );
  }

  allValid(): boolean {
    return !this.addDeviceWizardStepper.steps.find((item, index) => {
      if (item.stepControl.invalid) {
        item.interacted = true;
        this.addDeviceWizardStepper.selectedIndex = index;
        return true;
      } else {
        return false;
      }
    });
  }

  changeStep($event: StepperSelectionEvent): void {
    this.selectedIndex = $event.selectedIndex;
    this.showNext = this.selectedIndex !== this.maxStepperIndex;
  }
}
