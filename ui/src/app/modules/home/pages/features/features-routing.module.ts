import { RouterModule, Routes } from '@angular/router';
import { Authority } from '@shared/models/authority.enum';
import { NgModule } from '@angular/core';
import { otaUpdatesRoutes } from '@home/pages/ota-update/ota-update-routing.module';
import { vcRoutes } from '@home/pages/vc/vc-routing.module';
import { MenuId } from '@core/services/menu.models';
import { OTA_UI_ENABLED, VERSION_CONTROL_UI_ENABLED } from '@shared/models/device.models';

const featureChildren: Routes = [
  ...(OTA_UI_ENABLED ? otaUpdatesRoutes : []),
  ...(VERSION_CONTROL_UI_ENABLED ? vcRoutes : [])
];

const routes: Routes = (OTA_UI_ENABLED || VERSION_CONTROL_UI_ENABLED) ? [
  {
    path: 'features',
    data: {
      auth: [Authority.TENANT_ADMIN],
      breadcrumb: {
        menuId: MenuId.features
      }
    },
    children: [
      {
        path: '',
        children: [],
        data: {
          auth: [Authority.TENANT_ADMIN],
          redirectTo: OTA_UI_ENABLED ? '/features/otaUpdates' : '/features/vc'
        }
      },
      ...featureChildren
    ]
  }
] : [];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class FeaturesRoutingModule { }
