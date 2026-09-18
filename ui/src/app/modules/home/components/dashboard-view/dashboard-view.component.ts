import { Component } from '@angular/core';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { PageComponent } from '@shared/components/page.component';
import { Dashboard } from '@shared/models/dashboard.models';
import { ActivatedRoute } from '@angular/router';
import { GatewayLocaleMergeService } from '@core/translate/gateway-locale-merge.service';
import { GATEWAY_UI_ENABLED } from '@shared/models/device.models';

@Component({
  selector: 'tb-dashboard-view',
  templateUrl: './dashboard-view.component.html',
  styleUrls: ['./dashboard-view.component.scss']
})
export class DashboardViewComponent extends PageComponent {

  dashboard: Dashboard = this.route.snapshot.data.dashboard;

  constructor(protected store: Store<AppState>,
              private route: ActivatedRoute,
              private gatewayLocaleMerge: GatewayLocaleMergeService) {
    super(store);
    if (GATEWAY_UI_ENABLED && this.route.snapshot.url.some(s => s.path === 'gateways')) {
      this.gatewayLocaleMerge.applyWithRetry();
    }
  }
}
