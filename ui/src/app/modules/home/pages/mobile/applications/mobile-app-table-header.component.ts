import { Component } from '@angular/core';
import { EntityTableHeaderComponent } from '@home/components/entity/entity-table-header.component';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { MobileApp } from '@shared/models/mobile-app.models';

@Component({
  selector: 'tb-mobile-app-table-header',
  templateUrl: './mobile-app-table-header.component.html',
  styleUrls: ['./mobile-app-table-header.component.scss']
})
export class MobileAppTableHeaderComponent extends EntityTableHeaderComponent<MobileApp> {

  constructor(protected store: Store<AppState>) {
    super(store);
  }
}
