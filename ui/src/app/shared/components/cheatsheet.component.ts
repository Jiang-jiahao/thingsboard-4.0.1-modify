import { Component, ElementRef, Input, OnDestroy, OnInit } from '@angular/core';
import { Hotkey, HotkeysService } from 'angular2-hotkeys';
import { MousetrapInstance } from 'mousetrap';
import Mousetrap from 'mousetrap';

@Component({
  selector : 'jnks-iot-hotkeys-cheatsheet',
  styles : [`
.jnks-iot-hotkeys-container {
  display: table !important;
  position: fixed;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  color: #333;
  font-size: 1em;
  background-color: rgba(255,255,255,0.9);
  outline: 0;
}
.jnks-iot-hotkeys-container.fade {
  z-index: -1024;
  visibility: hidden;
  opacity: 0;
  -webkit-transition: opacity 0.15s linear;
  -moz-transition: opacity 0.15s linear;
  -o-transition: opacity 0.15s linear;
  transition: opacity 0.15s linear;
}
.jnks-iot-hotkeys-container.fade.in {
  z-index: 10002;
  visibility: visible;
  opacity: 1;
}
.jnks-iot-hotkeys-title {
  font-weight: bold;
  text-align: center;
  font-size: 1.2em;
}
.jnks-iot-hotkeys {
  width: 100%;
  height: 100%;
  display: table-cell;
  vertical-align: middle;
}
.jnks-iot-hotkeys table {
  margin: auto;
  color: #333;
}
.jnks-iot-content {
  display: table-cell;
  vertical-align: middle;
}
.jnks-iot-hotkeys-keys {
  padding: 5px;
  text-align: right;
}
.jnks-iot-hotkeys-key {
  display: inline-block;
  color: #fff;
  background-color: #333;
  border: 1px solid #333;
  border-radius: 5px;
  text-align: center;
  margin-right: 5px;
  box-shadow: inset 0 1px 0 #666, 0 1px 0 #bbb;
  padding: 5px 9px;
  font-size: 1em;
}
.jnks-iot-hotkeys-text {
  padding-left: 10px;
  font-size: 1em;
}
.jnks-iot-hotkeys-close {
  position: fixed;
  top: 20px;
  right: 20px;
  font-size: 2em;
  font-weight: bold;
  padding: 5px 10px;
  border: 1px solid #ddd;
  border-radius: 5px;
  min-height: 45px;
  min-width: 45px;
  text-align: center;
}
.jnks-iot-hotkeys-close:hover {
  background-color: #fff;
  cursor: pointer;
}
@media all and (max-width: 500px) {
  .jnks-iot-hotkeys {
    font-size: 0.8em;
  }
}
@media all and (min-width: 750px) {
  .jnks-iot-hotkeys {
    font-size: 1.2em;
  }
}  `],
  template : `<div tabindex="-1" class="jnks-iot-hotkeys-container fade" [class.in]="helpVisible" style="display:none"><div class="jnks-iot-hotkeys">
  <h4 class="jnks-iot-hotkeys-title">{{ title }}</h4>
  <table *ngIf="helpVisible"><tbody>
    <tr *ngFor="let hotkey of hotkeysList">
      <td class="jnks-iot-hotkeys-keys">
        <span *ngFor="let key of hotkey.formatted" class="jnks-iot-hotkeys-key">{{ key }}</span>
      </td>
      <td class="jnks-iot-hotkeys-text">{{ hotkey.description }}</td>
    </tr>
  </tbody></table>
  <div class="jnks-iot-hotkeys-close" (click)="toggleCheatSheet()">&#215;</div>
</div></div>`,
})
export class JnksIotCheatSheetComponent implements OnInit, OnDestroy {

  helpVisible = false;
  @Input() title = 'Keyboard Shortcuts:';

  @Input()
  hotkeys: Hotkey[];

  hotkeysList: Hotkey[];

  private mousetrap: MousetrapInstance;

  constructor(private elementRef: ElementRef,
              private hotkeysService: HotkeysService) {
    this.mousetrap = new Mousetrap(this.elementRef.nativeElement);
    this.mousetrap.bind('?', (event: KeyboardEvent, combo: string) => {
      this.toggleCheatSheet();
    });
  }

  public ngOnInit(): void {
    if (this.hotkeys) {
      this.hotkeysList = this.hotkeys.filter(hotkey => hotkey.description);
    }
  }

  public setHotKeys(hotkeys: Hotkey[]) {
    this.hotkeysList = hotkeys.filter(hotkey => hotkey.description);
  }

  public toggleCheatSheet(): void {
    this.helpVisible = !this.helpVisible;
  }

  ngOnDestroy() {
    this.mousetrap.unbind('?');
  }
}
