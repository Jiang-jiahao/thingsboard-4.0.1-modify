import { ActivatedRouteSnapshot, Params } from '@angular/router';
import { TranslateService } from '@ngx-translate/core';
import { HasUUID } from '@shared/models/id/has-uuid';
import { MenuId } from '@core/services/menu.models';

export interface BreadCrumb extends HasUUID{
  label: string;
  customTranslate: boolean;
  labelFunction?: () => string;
  icon: string;
  link: any[];
  queryParams: Params;
}

export type BreadCrumbLabelFunction<C> = (route: ActivatedRouteSnapshot, translate: TranslateService, component: C, data?: any) => string;

export interface BreadCrumbConfig<C> {
  labelFunction: BreadCrumbLabelFunction<C>;
  menuId?: MenuId;
  label?: string;
  icon?: string;
  skip: boolean;
}
