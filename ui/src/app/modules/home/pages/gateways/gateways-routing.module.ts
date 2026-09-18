import { inject, NgModule } from '@angular/core';
import { ActivatedRouteSnapshot, ResolveFn, RouterModule, RouterStateSnapshot, Routes } from '@angular/router';
import { Authority } from '@shared/models/authority.enum';
import { Dashboard } from '@shared/models/dashboard.models';
import { ResourcesService } from '@core/services/resources.service';
import { GatewayLocaleMergeService } from '@core/translate/gateway-locale-merge.service';
import { Observable, tap } from 'rxjs';
import { MenuId } from '@core/services/menu.models';
import { DashboardViewComponent } from '@home/components/dashboard-view/dashboard-view.component';

/** Bundled dashboard (zh/i18n); avoids stale English copy in DB system resource */
const gatewaysDashboardJson = '/assets/dashboards/gateways_dashboard.json';

export const gatewaysDashboardResolver: ResolveFn<Dashboard> = (
  route: ActivatedRouteSnapshot,
  state: RouterStateSnapshot,
  resourcesService = inject(ResourcesService),
  gatewayLocaleMerge = inject(GatewayLocaleMergeService)
): Observable<Dashboard> => resourcesService.loadJsonResource(gatewaysDashboardJson).pipe(
  tap(() => gatewayLocaleMerge.applyWithRetry())
);

export const gatewaysRoutes: Routes = [
  {
    path: 'gateways',
    component: DashboardViewComponent,
    data: {
      auth: [Authority.TENANT_ADMIN],
      title: 'gateway.gateways',
      breadcrumb: {
        menuId: MenuId.gateways
      }
    },
    resolve: {
      dashboard: gatewaysDashboardResolver
    }
  }
];

const routes: Routes = [
  {
    path: 'gateways',
    pathMatch: 'full',
    redirectTo: '/entities/gateways'
  },
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class GatewaysRoutingModule { }
