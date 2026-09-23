import { ChangeDetectionStrategy, Component, Input, OnInit } from '@angular/core';
import { MenuSection } from '@core/services/menu.models';
import { Router } from '@angular/router';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { ActionPreferencesUpdateOpenedMenuSection } from '@core/auth/auth.actions';

@Component({
  selector: 'jnks-iot-menu-toggle',
  templateUrl: './menu-toggle.component.html',
  styleUrls: ['./menu-toggle.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class MenuToggleComponent implements OnInit {

  @Input() section: MenuSection;

  constructor(private router: Router,
              private store: Store<AppState>) {
  }

  ngOnInit() {
  }

  sectionHeight(): string {
    if (this.section.opened) {
      // 展开高度 = 条目数 × 行高。行高必须跟 side-menu.component.scss 里
      // `.jnks-iot-side-menu ul a.mat-mdc-button` 的 height 一致，否则每项会多出
      // 一截死区（上游是 40px，我们收到 28px 后没同步这儿，实体/配置之间就空了一大块）。
      return this.section.pages.length * 28 + 'px';
    } else {
      return '0px';
    }
  }

  toggleSection(event: MouseEvent) {
    event.stopPropagation();
    this.section.opened = !this.section.opened;
    this.store.dispatch(new ActionPreferencesUpdateOpenedMenuSection({path: this.section.path, opened: this.section.opened}));
  }

  trackBySectionPages(index: number, section: MenuSection){
    return section.id;
  }
}
