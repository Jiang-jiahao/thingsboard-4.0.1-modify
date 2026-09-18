import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SharedModule } from '@shared/shared.module';
import { DeviceComponent } from '@modules/home/pages/device/device.component';
import { DeviceRoutingModule } from './device-routing.module';
import { DeviceTableHeaderComponent } from '@modules/home/pages/device/device-table-header.component';
import { DeviceCredentialsDialogComponent } from '@modules/home/pages/device/device-credentials-dialog.component';
import { HomeDialogsModule } from '../../dialogs/home-dialogs.module';
import { HomeComponentsModule } from '@modules/home/components/home-components.module';
import { DeviceTabsComponent } from '@home/pages/device/device-tabs.component';
import { DeviceDataFormsModule } from '@home/pages/device/device-data-forms.module';
import { DeviceCredentialsModule } from '@home/components/device/device-credentials.module';
import { DeviceProfileCommonModule } from '@home/components/profile/device/common/device-profile-common.module';
import { DeviceCheckConnectivityDialogComponent } from './device-check-connectivity-dialog.component';

@NgModule({
  declarations: [
    DeviceComponent,
    DeviceTabsComponent,
    DeviceTableHeaderComponent,
    DeviceCredentialsDialogComponent,
    DeviceCheckConnectivityDialogComponent
  ],
  imports: [
    CommonModule,
    SharedModule,
    DeviceDataFormsModule,
    HomeComponentsModule,
    HomeDialogsModule,
    DeviceCredentialsModule,
    DeviceProfileCommonModule,
    DeviceRoutingModule
  ]
})
export class DeviceModule { }
