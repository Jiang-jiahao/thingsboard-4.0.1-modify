import { NgModule } from '@angular/core';

import { AdminModule } from './admin/admin.module';
import { HomeLinksModule } from './home-links/home-links.module';
import { ProfileModule } from './profile/profile.module';
import { SecurityModule } from '@home/pages/security/security.module';
import { TenantModule } from '@modules/home/pages/tenant/tenant.module';
import { CustomerModule } from '@modules/home/pages/customer/customer.module';
import { AuditLogModule } from '@modules/home/pages/audit-log/audit-log.module';
import { UserModule } from '@modules/home/pages/user/user.module';
import { DeviceModule } from '@modules/home/pages/device/device.module';
import { AssetModule } from '@modules/home/pages/asset/asset.module';
import { EntityViewModule } from '@modules/home/pages/entity-view/entity-view.module';
import { RuleChainModule } from '@modules/home/pages/rulechain/rulechain.module';
import { WidgetLibraryModule } from '@modules/home/pages/widget/widget-library.module';
import { DashboardModule } from '@modules/home/pages/dashboard/dashboard.module';
import { TenantProfileModule } from './tenant-profile/tenant-profile.module';
import { DeviceProfileModule } from './device-profile/device-profile.module';
import { ApiUsageModule } from '@home/pages/api-usage/api-usage.module';
import { MOBILE_UI_ENABLED, OTA_UI_ENABLED, VERSION_CONTROL_UI_ENABLED } from '@shared/models/device.models';
import { OtaUpdateModule } from '@home/pages/ota-update/ota-update.module';
import { VcModule } from '@home/pages/vc/vc.module';
import { AssetProfileModule } from '@home/pages/asset-profile/asset-profile.module';
import { ProfilesModule } from '@home/pages/profiles/profiles.module';
import { AlarmModule } from '@home/pages/alarm/alarm.module';
import { EntitiesModule } from '@home/pages/entities/entities.module';
import { FeaturesModule } from '@home/pages/features/features.module';
import { NotificationModule } from '@home/pages/notification/notification.module';
import { AccountModule } from '@home/pages/account/account.module';
import { ScadaSymbolModule } from '@home/pages/scada-symbol/scada-symbol.module';
import { GatewaysModule } from '@home/pages/gateways/gateways.module';
import { MobileModule } from '@home/pages/mobile/mobile.module';

@NgModule({
  exports: [
    AdminModule,
    HomeLinksModule,
    ProfileModule,
    SecurityModule,
    TenantProfileModule,
    TenantModule,
    DeviceProfileModule,
    AssetProfileModule,
    ProfilesModule,
    EntitiesModule,
    ...((OTA_UI_ENABLED || VERSION_CONTROL_UI_ENABLED) ? [FeaturesModule] : []),
    ...(MOBILE_UI_ENABLED ? [MobileModule] : []),
    NotificationModule,
    DeviceModule,
    AssetModule,
    AlarmModule,
    EntityViewModule,
    CustomerModule,
    RuleChainModule,
    WidgetLibraryModule,
    DashboardModule,
    AuditLogModule,
    ApiUsageModule,
    GatewaysModule,
    ...(OTA_UI_ENABLED ? [OtaUpdateModule] : []),
    UserModule,
    ...(VERSION_CONTROL_UI_ENABLED ? [VcModule] : []),
    AccountModule,
    ScadaSymbolModule
  ]
})
export class HomePagesModule { }
