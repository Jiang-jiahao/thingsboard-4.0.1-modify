import { Component, DestroyRef } from '@angular/core';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { EntityTabsComponent } from '../../components/entity/entity-tabs.component';
import {
  DeviceProfile,
  DEVICE_PROVISIONING_UI_ENABLED,
  deviceProfileTransportTypeOptions,
  deviceTransportTypeHintMap,
  deviceTransportTypeTranslationMap,
  DeviceTransportType,
  resolveHttpProfileTransportTypeForDisplay
} from '@shared/models/device.models';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
  selector: 'tb-device-profile-tabs',
  templateUrl: './device-profile-tabs.component.html',
  styleUrls: []
})
export class DeviceProfileTabsComponent extends EntityTabsComponent<DeviceProfile> {

  deviceTransportTypes = deviceProfileTransportTypeOptions;

  deviceTransportTypeTranslations = deviceTransportTypeTranslationMap;

  deviceTransportTypeHints = deviceTransportTypeHintMap;

  isTransportTypeChanged = false;

  readonly deviceProvisioningUiEnabled = DEVICE_PROVISIONING_UI_ENABLED;

  constructor(protected store: Store<AppState>,
              private destroyRef: DestroyRef) {
    super(store);
  }

  ngOnInit() {
    super.ngOnInit();
    this.detailsForm.get('transportType').valueChanges.pipe(
      takeUntilDestroyed(this.destroyRef)
    ).subscribe(() => {
      this.isTransportTypeChanged = true;
    });
  }

  /** HTTP 工作模式回显：结合已保存配置，不单独依赖 entity.transportType */
  get httpEntityTransportType(): DeviceTransportType {
    const cfg = this.detailsForm?.get('profileData.transportConfiguration')?.value;
    return resolveHttpProfileTransportTypeForDisplay(this.entity?.transportType, cfg);
  }

}
