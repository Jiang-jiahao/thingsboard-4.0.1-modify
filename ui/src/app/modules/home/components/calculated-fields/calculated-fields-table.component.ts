import {
  ChangeDetectionStrategy,
  ChangeDetectorRef,
  Component,
  DestroyRef,
  effect,
  input,
  Renderer2,
  ViewChild,
} from '@angular/core';
import { EntityId } from '@shared/models/id/entity-id';
import { EntitiesTableComponent } from '@home/components/entity/entities-table.component';
import { TranslateService } from '@ngx-translate/core';
import { MatDialog } from '@angular/material/dialog';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { CalculatedFieldsTableConfig } from '@home/components/calculated-fields/calculated-fields-table-config';
import { CalculatedFieldsService } from '@core/http/calculated-fields.service';
import { ImportExportService } from '@shared/import-export/import-export.service';
import { EntityDebugSettingsService } from '@home/components/entity/debug/entity-debug-settings.service';
import { DatePipe } from '@angular/common';

@Component({
  selector: 'tb-calculated-fields-table',
  templateUrl: './calculated-fields-table.component.html',
  styleUrls: ['./calculated-fields-table.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  providers: [EntityDebugSettingsService]
})
export class CalculatedFieldsTableComponent {

  @ViewChild(EntitiesTableComponent, {static: true}) entitiesTable: EntitiesTableComponent;

  active = input<boolean>();
  entityId = input<EntityId>();
  entityName = input<string>();

  calculatedFieldsTableConfig: CalculatedFieldsTableConfig;

  constructor(private calculatedFieldsService: CalculatedFieldsService,
              private translate: TranslateService,
              private dialog: MatDialog,
              private store: Store<AppState>,
              private datePipe: DatePipe,
              private cd: ChangeDetectorRef,
              private renderer: Renderer2,
              private importExportService: ImportExportService,
              private entityDebugSettingsService: EntityDebugSettingsService,
              private destroyRef: DestroyRef) {

    effect(() => {
      if (this.active()) {
        this.calculatedFieldsTableConfig = new CalculatedFieldsTableConfig(
          this.calculatedFieldsService,
          this.translate,
          this.dialog,
          this.datePipe,
          this.entityId(),
          this.store,
          this.destroyRef,
          this.renderer,
          this.entityName(),
          this.importExportService,
          this.entityDebugSettingsService,
        );
        this.cd.markForCheck();
      }
    });
  }
}
