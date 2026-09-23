import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { SharedModule } from '@app/shared/shared.module';
import { ClusterInfoTableComponent } from '@home/components/widget/lib/home-page/cluster-info-table.component';
import { ConfiguredFeaturesComponent } from '@home/components/widget/lib/home-page/configured-features.component';
import { VersionInfoComponent } from '@home/components/widget/lib/home-page/version-info.component';
import { DocLinkComponent } from '@home/components/widget/lib/home-page/doc-link.component';
import { EditLinksDialogComponent } from '@home/components/widget/lib/home-page/edit-links-dialog.component';
import { UsageInfoWidgetComponent } from '@home/components/widget/lib/home-page/usage-info-widget.component';
import { QuickLinksWidgetComponent } from '@home/components/widget/lib/home-page/quick-links-widget.component';
import { QuickLinkComponent } from '@home/components/widget/lib/home-page/quick-link.component';
import { AddQuickLinkDialogComponent } from '@home/components/widget/lib/home-page/add-quick-link-dialog.component';
import {
  RecentDashboardsWidgetComponent
} from '@home/components/widget/lib/home-page/recent-dashboards-widget.component';

@NgModule({
  declarations:
    [
      ClusterInfoTableComponent,
      ConfiguredFeaturesComponent,
      VersionInfoComponent,
      DocLinkComponent,
      EditLinksDialogComponent,
      UsageInfoWidgetComponent,
      QuickLinksWidgetComponent,
      QuickLinkComponent,
      AddQuickLinkDialogComponent,
      RecentDashboardsWidgetComponent
    ],
  imports: [
    CommonModule,
    SharedModule
  ],
  exports: [
    ClusterInfoTableComponent,
    ConfiguredFeaturesComponent,
    VersionInfoComponent,
    DocLinkComponent,
    EditLinksDialogComponent,
    UsageInfoWidgetComponent,
    QuickLinksWidgetComponent,
    QuickLinkComponent,
    AddQuickLinkDialogComponent,
    RecentDashboardsWidgetComponent
  ]
})
export class HomePageWidgetsModule { }
