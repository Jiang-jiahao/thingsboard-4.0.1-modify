import { TbContextMenuEvent } from '@shared/models/jquery-event.models';

interface JQuery {
  terminal(options?: any): any;
  on(events: 'tbcontextmenu', handler: (e: TbContextMenuEvent) => void): this;
}
