///
/// Copyright © 2016-2025 The Thingsboard Authors
///
import { SelectionModel } from '@angular/cdk/collections';
import { AfterViewInit, Component, OnInit, ViewChild } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { MatSort } from '@angular/material/sort';
import { MatTableDataSource } from '@angular/material/table';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { selectAuthUser, selectIsUserLoaded } from '@core/auth/auth.selectors';
import { ProtocolTemplateBundleService } from '@core/services/protocol-template-bundle.service';
import { DialogService } from '@core/services/dialog.service';
import { ActionNotificationShow } from '@core/notification/notification.actions';
import { TranslateService } from '@ngx-translate/core';
import { NULL_UUID } from '@shared/models/id/has-uuid';
import { ProtocolTemplateBundle } from '@shared/models/device.models';
import {
  ProtocolTemplateBundleDialogComponent,
  ProtocolTemplateBundleDialogData
} from '@home/pages/profiles/protocol-template-bundle-dialog.component';
import {
  ProtocolTemplateHexTestDialogComponent,
  ProtocolTemplateHexTestDialogData
} from '@home/pages/profiles/protocol-template-hex-test-dialog.component';
import { combineLatest, forkJoin } from 'rxjs';
import { filter, switchMap, take } from 'rxjs/operators';

@Component({
  selector: 'tb-protocol-template-bundles-page',
  templateUrl: './protocol-template-bundles-page.component.html',
  styleUrls: ['./protocol-template-bundles-page.component.scss']
})
export class ProtocolTemplateBundlesPageComponent implements OnInit, AfterViewInit {

  @ViewChild(MatSort) sort: MatSort;

  isLoading = true;
  textSearchMode = false;
  searchText = '';

  bundles: ProtocolTemplateBundle[] = [];

  dataSource = new MatTableDataSource<ProtocolTemplateBundle>([]);
  selection = new SelectionModel<ProtocolTemplateBundle>(true, [], true, (a, b) => a.id === b.id);

  displayedColumns: string[] = ['select', 'createdTime', 'name', 'bundleType', 'transport', 'description', 'actions'];

  constructor(
    private store: Store<AppState>,
    private bundleService: ProtocolTemplateBundleService,
    private translate: TranslateService,
    private dialog: MatDialog,
    private dialogService: DialogService
  ) {
    this.dataSource.filterPredicate = (data, filter) => {
      if (!filter) {
        return true;
      }
      const q = filter;
      const name = (data.name || '').toLowerCase();
      const tid = String(this.primaryTemplateId(data)).toLowerCase();
      const id = (data.id || '').toLowerCase();
      return name.includes(q) || tid.includes(q) || id.includes(q);
    };
  }

  ngOnInit(): void {
    this.loadBundles();
  }

  ngAfterViewInit(): void {
    this.dataSource.sort = this.sort;
    this.dataSource.sortingDataAccessor = (item, prop) => {
      switch (prop) {
        case 'createdTime':
          return item.createdTime ?? 0;
        case 'name':
          return (item.name || this.primaryTemplateId(item) || '').toLowerCase();
        case 'description':
          return this.rowDescriptionText(item).toLowerCase();
        default:
          return '';
      }
    };
  }

  loadBundles(): void {
    this.isLoading = true;
    combineLatest([
      this.store.select(selectIsUserLoaded),
      this.store.select(selectAuthUser)
    ]).pipe(
      filter(([loaded, u]) => loaded && !!u?.tenantId && u.tenantId !== NULL_UUID),
      take(1),
      switchMap(() => this.bundleService.getBundles())
    ).subscribe({
      next: (list) => {
        this.bundles = (list ?? []).slice().sort((a, b) => (b.createdTime ?? 0) - (a.createdTime ?? 0));
        this.syncTable();
        this.isLoading = false;
      },
      error: () => {
        this.isLoading = false;
      }
    });
  }

  private syncTable(): void {
    this.dataSource.data = this.bundles.slice();
    this.selection.clear();
    this.applySearchFilter();
  }

  enterSearchMode(): void {
    this.textSearchMode = true;
  }

  exitSearchMode(): void {
    this.textSearchMode = false;
    this.searchText = '';
    this.applySearchFilter();
  }

  applySearchFilter(): void {
    this.dataSource.filter = this.searchText.trim().toLowerCase();
  }

  isAllSelected(): boolean {
    const n = this.dataSource.filteredData.length;
    return n > 0 && this.selection.selected.length === n;
  }

  masterToggle(): void {
    if (this.isAllSelected()) {
      this.selection.clear();
    } else {
      for (const row of this.dataSource.filteredData) {
        this.selection.select(row);
      }
    }
  }

  checkboxLabel(row?: ProtocolTemplateBundle): string {
    if (!row) {
      return `${this.isAllSelected() ? 'select' : 'deselect'} all`;
    }
    return `${this.selection.isSelected(row) ? 'deselect' : 'select'} row ${row.id}`;
  }

  /** 展示用：优先包名称，否则帧模板逻辑 id（与顶部名称同步后二者一致） */
  primaryTemplateId(row: ProtocolTemplateBundle): string {
    const nm = row.name && String(row.name).trim();
    if (nm) {
      return nm;
    }
    const t = (row.protocolTemplates ?? row.monitoringTemplates)?.[0]?.id;
    return t && String(t).trim() ? String(t).trim() : '—';
  }

  rowDescriptionText(row: ProtocolTemplateBundle): string {
    return this.translate.instant('profiles.protocol-templates-desc-summary', {
      tid: this.primaryTemplateId(row),
      tc: this.templateCount(row),
      cc: this.commandCount(row)
    });
  }

  openHexTestDialog(): void {
    this.dialog.open<ProtocolTemplateHexTestDialogComponent, ProtocolTemplateHexTestDialogData, void>(
      ProtocolTemplateHexTestDialogComponent,
      {
        width: '720px',
        maxWidth: '96vw',
        maxHeight: '90vh',
        autoFocus: false,
        restoreFocus: false,
        panelClass: ['tb-dialog', 'tb-protocol-template-hex-test-dialog'],
        data: { bundles: this.bundles.slice() }
      }
    );
  }

  openCreateDialog(): void {
    const ref = this.dialog.open<ProtocolTemplateBundleDialogComponent, ProtocolTemplateBundleDialogData, ProtocolTemplateBundle | undefined>(
      ProtocolTemplateBundleDialogComponent,
      {
        width: '980px',
        maxWidth: '96vw',
        maxHeight: '92vh',
        autoFocus: false,
        restoreFocus: false,
        panelClass: ['tb-dialog', 'tb-protocol-template-bundle-dialog'],
        data: { bundle: null, isNew: true }
      }
    );
    ref.afterClosed().subscribe(result => {
      if (result) {
        this.persistBundle(result);
      }
    });
  }

  openEditDialog(bundle: ProtocolTemplateBundle, event?: MouseEvent): void {
    event?.stopPropagation();
    const ref = this.dialog.open<ProtocolTemplateBundleDialogComponent, ProtocolTemplateBundleDialogData, ProtocolTemplateBundle | undefined>(
      ProtocolTemplateBundleDialogComponent,
      {
        width: '980px',
        maxWidth: '96vw',
        maxHeight: '92vh',
        autoFocus: false,
        restoreFocus: false,
        panelClass: ['tb-dialog', 'tb-protocol-template-bundle-dialog'],
        data: { bundle: { ...bundle }, isNew: false }
      }
    );
    ref.afterClosed().subscribe(result => {
      if (result) {
        this.persistBundle(result);
      }
    });
  }

  private persistBundle(bundle: ProtocolTemplateBundle): void {
    this.bundleService.saveBundle(bundle).subscribe({
      next: (saved) => {
        const idx = this.bundles.findIndex(b => b.id === saved.id);
        if (idx >= 0) {
          this.bundles = [...this.bundles.slice(0, idx), saved, ...this.bundles.slice(idx + 1)];
        } else {
          this.bundles = [...this.bundles, saved];
        }
        this.bundles.sort((a, b) => (b.createdTime ?? 0) - (a.createdTime ?? 0));
        this.syncTable();
        this.store.dispatch(new ActionNotificationShow({
          message: this.translate.instant('profiles.protocol-templates-save-success'),
          type: 'success',
          duration: 2000,
          verticalPosition: 'bottom',
          horizontalPosition: 'right'
        }));
      },
      error: () => {
        this.store.dispatch(new ActionNotificationShow({
          message: this.translate.instant('action.operation-failed'),
          type: 'error',
          duration: 3500,
          verticalPosition: 'top',
          horizontalPosition: 'right'
        }));
      }
    });
  }

  confirmDelete(bundle: ProtocolTemplateBundle, event: MouseEvent): void {
    event.stopPropagation();
    this.dialogService.confirm(
      this.translate.instant('profiles.protocol-templates-delete-title'),
      this.translate.instant('profiles.protocol-templates-delete-content', { name: bundle.name || this.primaryTemplateId(bundle) }),
      this.translate.instant('action.cancel'),
      this.translate.instant('action.delete'),
      false
    ).subscribe(ok => {
      if (ok) {
        this.bundleService.deleteBundle(bundle.id).subscribe({
          next: () => {
            this.bundles = this.bundles.filter(b => b.id !== bundle.id);
            this.syncTable();
          },
          error: () => {
            this.store.dispatch(new ActionNotificationShow({
              message: this.translate.instant('action.operation-failed'),
              type: 'error',
              duration: 3500,
              verticalPosition: 'top',
              horizontalPosition: 'right'
            }));
          }
        });
      }
    });
  }

  deleteSelected(event?: MouseEvent): void {
    event?.stopPropagation();
    const selected = this.selection.selected;
    if (!selected.length) {
      return;
    }
    this.dialogService.confirm(
      this.translate.instant('profiles.protocol-templates-batch-delete-title'),
      this.translate.instant('profiles.protocol-templates-batch-delete-content', { count: selected.length }),
      this.translate.instant('action.cancel'),
      this.translate.instant('action.delete'),
      false
    ).subscribe(ok => {
      if (!ok) {
        return;
      }
      forkJoin(selected.map(b => this.bundleService.deleteBundle(b.id))).subscribe({
        next: () => {
          const ids = new Set(selected.map(b => b.id));
          this.bundles = this.bundles.filter(b => !ids.has(b.id));
          this.syncTable();
          this.store.dispatch(new ActionNotificationShow({
            message: this.translate.instant('profiles.protocol-templates-batch-delete-success'),
            type: 'success',
            duration: 2000,
            verticalPosition: 'bottom',
            horizontalPosition: 'right'
          }));
        },
        error: () => {
          this.store.dispatch(new ActionNotificationShow({
            message: this.translate.instant('action.operation-failed'),
            type: 'error',
            duration: 3500,
            verticalPosition: 'top',
            horizontalPosition: 'right'
          }));
        }
      });
    });
  }

  exportBundle(bundle: ProtocolTemplateBundle, event: MouseEvent): void {
    event.stopPropagation();
    const json = JSON.stringify(bundle, null, 2);
    const blob = new Blob([json], { type: 'application/json;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    const raw = bundle.name || this.primaryTemplateId(bundle) || bundle.id;
    const safe = String(raw).replace(/[^\w\-\u4e00-\u9fa5]+/g, '_').slice(0, 80);
    a.download = `protocol-template-bundle-${safe}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }

  templateCount(b: ProtocolTemplateBundle): number {
    return (b.protocolTemplates ?? b.monitoringTemplates)?.length ?? 0;
  }

  commandCount(b: ProtocolTemplateBundle): number {
    return (b.protocolCommands ?? b.monitoringCommands)?.length ?? 0;
  }

  trackByBundleId(_index: number, row: ProtocolTemplateBundle): string {
    return row.id;
  }
}
