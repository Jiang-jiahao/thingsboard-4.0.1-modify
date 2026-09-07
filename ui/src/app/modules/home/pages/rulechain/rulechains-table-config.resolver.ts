///
/// Copyright © 2016-2025 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { Injectable } from '@angular/core';

import { ActivatedRouteSnapshot, Router } from '@angular/router';
import {
  CellActionDescriptor,
  checkBoxCell,
  DateEntityTableColumn,
  EntityColumn,
  EntityTableColumn,
  EntityTableConfig,
  GroupActionDescriptor,
  HeaderActionDescriptor
} from '@home/models/entity/entities-table-config.models';
import { TranslateService } from '@ngx-translate/core';
import { DatePipe } from '@angular/common';
import { EntityType, entityTypeResources, entityTypeTranslations } from '@shared/models/entity-type.models';
import { EntityAction } from '@home/models/entity/entity-component.models';
import { RuleChain, RuleChainType } from '@shared/models/rule-chain.models';
import { RuleChainService } from '@core/http/rule-chain.service';
import { RuleChainComponent } from '@modules/home/pages/rulechain/rulechain.component';
import { DialogService } from '@core/services/dialog.service';
import { RuleChainTabsComponent } from '@home/pages/rulechain/rulechain-tabs.component';
import { ImportExportService } from '@shared/import-export/import-export.service';
import { ItemBufferService } from '@core/services/item-buffer.service';
import { Observable } from 'rxjs';
import { isUndefined } from '@core/utils';
import { PageLink } from '@shared/models/page/page-link';
import { PageData } from '@shared/models/page/page-data';
import { CustomTranslatePipe } from '@shared/pipe/custom-translate.pipe';

@Injectable()
export class RuleChainsTableConfigResolver  {

  private readonly config: EntityTableConfig<RuleChain> = new EntityTableConfig<RuleChain>();

  constructor(private ruleChainService: RuleChainService,
              private dialogService: DialogService,
              private importExport: ImportExportService,
              private itembuffer: ItemBufferService,
              private translate: TranslateService,
              private datePipe: DatePipe,
              private router: Router,
              private customTranslate: CustomTranslatePipe) {
    this.config.entityType = EntityType.RULE_CHAIN;
    this.config.entityComponent = RuleChainComponent;
    this.config.entityTabsComponent = RuleChainTabsComponent;
    this.config.entityTranslations = entityTypeTranslations.get(EntityType.RULE_CHAIN);
    this.config.entityResources = entityTypeResources.get(EntityType.RULE_CHAIN);

    this.config.rowPointer = true;

    this.config.deleteEntityTitle = ruleChain => this.translate.instant('rulechain.delete-rulechain-title',
      {ruleChainName: ruleChain.name});
    this.config.deleteEntityContent = () => this.translate.instant('rulechain.delete-rulechain-text');
    this.config.deleteEntitiesTitle = count => this.translate.instant('rulechain.delete-rulechains-title', {count});
    this.config.deleteEntitiesContent = () => this.translate.instant('rulechain.delete-rulechains-text');
    this.config.loadEntity = id => this.ruleChainService.getRuleChain(id.id);
    this.config.saveEntity = ruleChain => this.saveRuleChain(ruleChain);
    this.config.deleteEntity = id => this.ruleChainService.deleteRuleChain(id.id);
    this.config.onEntityAction = action => this.onRuleChainAction(action);
    this.config.handleRowClick = ($event, ruleChain) => {
      if (this.config.isDetailsOpen()) {
        this.config.toggleEntityDetails($event, ruleChain);
      } else {
        this.openRuleChain($event, ruleChain);
      }
      return true;
    };
  }

  resolve(route: ActivatedRouteSnapshot): EntityTableConfig<RuleChain> {
    const ruleChainScope = route.data?.ruleChainsType ? route.data?.ruleChainsType : 'tenant';
    this.config.componentsData = {
      ruleChainScope
    };
    this.config.columns = this.configureEntityTableColumns(ruleChainScope);
    this.config.entitiesFetchFunction = this.configureEntityFunctions(ruleChainScope);
    this.config.groupActionDescriptors = this.configureGroupActions();
    this.config.addActionDescriptors = this.configureAddActions(ruleChainScope);
    this.config.cellActionDescriptors = this.configureCellActions(ruleChainScope);
    this.config.entitySelectionEnabled = ruleChain => ruleChain && !ruleChain.root;
    this.config.deleteEnabled = (ruleChain) => ruleChain && !ruleChain.root;
    this.config.entitiesDeleteEnabled = true;
    this.config.tableTitle = this.configureTableTitle();
    return this.config;
  }

  configureEntityTableColumns(ruleChainScope: string): Array<EntityColumn<RuleChain>> {
    const columns: Array<EntityColumn<RuleChain>> = [];
    columns.push(
      new DateEntityTableColumn<RuleChain>('createdTime', 'common.created-time', this.datePipe, '150px'),
      new EntityTableColumn<RuleChain>('name', 'rulechain.name', '50%'),
      new EntityTableColumn<RuleChain>('description', 'rulechain.description', '50%',
        entity => this.customTranslate.transform(entity.additionalInfo?.description || ''),
        () => ({}), false)
    );
    columns.push(
      new EntityTableColumn<RuleChain>('root', 'rulechain.root', '60px',
        entity => checkBoxCell(entity.root))
    );
    return columns;
  }

  configureAddActions(ruleChainScope: string): Array<HeaderActionDescriptor> {
    const actions: Array<HeaderActionDescriptor> = [];
    if (ruleChainScope === 'tenant') {
      actions.push(
        {
          name: this.translate.instant('rulechain.create-new-rulechain'),
          icon: 'insert_drive_file',
          isEnabled: () => true,
          onAction: ($event) => this.config.getTable().addEntity($event)
        },
        {
          name: this.translate.instant('rulechain.import'),
          icon: 'file_upload',
          isEnabled: () => true,
          onAction: ($event) => this.importRuleChain($event)
        }
      );
    }
    return actions;
  }

  configureEntityFunctions(ruleChainScope: string): (pageLink) => Observable<PageData<RuleChain>> {
    return pageLink => this.fetchRuleChains(pageLink);
  }

  configureTableTitle(): string {
    return this.translate.instant('rulechain.rulechains');
  }

  configureGroupActions(): Array<GroupActionDescriptor<RuleChain>> {
    return [];
  }

  configureCellActions(ruleChainScope: string): Array<CellActionDescriptor<RuleChain>> {
    const actions: Array<CellActionDescriptor<RuleChain>> = [];
    actions.push(
      {
        name: this.translate.instant('rulechain.export'),
        icon: 'file_download',
        isEnabled: () => true,
        onAction: ($event, entity) => this.exportRuleChain($event, entity)
      }
    );
    if (ruleChainScope === 'tenant') {
      actions.push(
        {
          name: this.translate.instant('rulechain.set-root'),
          icon: 'flag',
          isEnabled: (entity) => this.isNonRootRuleChain(entity),
          onAction: ($event, entity) => this.setRootRuleChain($event, entity)
        }
      );
    }
    actions.push(
      {
        name: this.translate.instant('rulechain.rulechain-details'),
          icon: 'edit',
        isEnabled: () => true,
        onAction: ($event, entity) => this.config.toggleEntityDetails($event, entity)
      }
    );
    return actions;
  }

  importRuleChain($event: Event) {
    if ($event) {
      $event.stopPropagation();
    }
    this.importExport.importRuleChain(RuleChainType.CORE).subscribe((ruleChainImport) => {
      if (ruleChainImport) {
        this.itembuffer.storeRuleChainImport(ruleChainImport);
        this.router.navigateByUrl(`ruleChains/ruleChain/import`);
      }
    });
  }

  openRuleChain($event: Event, ruleChain: RuleChain) {
    if ($event) {
      $event.stopPropagation();
    }
    this.router.navigateByUrl(`ruleChains/${ruleChain.id.id}`);
  }

  saveRuleChain(ruleChain: RuleChain) {
    if (isUndefined(ruleChain.type)) {
      ruleChain.type = RuleChainType.CORE;
    }
    return this.ruleChainService.saveRuleChain(ruleChain);
  }

  exportRuleChain($event: Event, ruleChain: RuleChain) {
    if ($event) {
      $event.stopPropagation();
    }
    this.importExport.exportRuleChain(ruleChain.id.id);
  }

  setRootRuleChain($event: Event, ruleChain: RuleChain) {
    if ($event) {
      $event.stopPropagation();
    }
    this.dialogService.confirm(
      this.translate.instant('rulechain.set-root-rulechain-title', {ruleChainName: ruleChain.name}),
      this.translate.instant('rulechain.set-root-rulechain-text'),
      this.translate.instant('action.no'),
      this.translate.instant('action.yes'),
      true
    ).subscribe((res) => {
        if (res) {
          this.ruleChainService.setRootRuleChain(ruleChain.id.id).subscribe(
            () => {
              this.config.updateData();
            }
          );
        }
      }
    );
  }

  onRuleChainAction(action: EntityAction<RuleChain>): boolean {
    switch (action.action) {
      case 'open':
        this.openRuleChain(action.event, action.entity);
        return true;
      case 'export':
        this.exportRuleChain(action.event, action.entity);
        return true;
      case 'setRoot':
        this.setRootRuleChain(action.event, action.entity);
        return true;
    }
    return false;
  }

  isNonRootRuleChain(ruleChain: RuleChain) {
    return !ruleChain.root;
  }

  fetchRuleChains(pageLink: PageLink) {
    return this.ruleChainService.getRuleChains(pageLink, RuleChainType.CORE);
  }
}
