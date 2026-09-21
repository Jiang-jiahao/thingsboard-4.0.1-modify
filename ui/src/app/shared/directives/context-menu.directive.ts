import { Directive, ElementRef, EventEmitter, OnDestroy, Output } from '@angular/core';
import { JnksIotContextMenuEvent } from '@shared/models/jquery-event.models';

@Directive({
  selector: '[tbcontextmenu]'
})
export class ContextMenuDirective implements OnDestroy {

  @Output()
  tbcontextmenu = new EventEmitter<JnksIotContextMenuEvent>();

  constructor(private el: ElementRef) {
    $(this.el.nativeElement).on('tbcontextmenu', (e: JnksIotContextMenuEvent) => this.tbcontextmenu.emit(e));
  }

  ngOnDestroy() {
    $(this.el.nativeElement).off('tbcontextmenu');
  }
}
