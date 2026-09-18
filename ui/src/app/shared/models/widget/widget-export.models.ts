import { Widget } from '@shared/models/widget.models';
import { Dashboard } from '@shared/models/dashboard.models';
import { EntityAliases } from '@shared/models/alias.models';
import { Filters } from '@shared/models/query/query.models';
import { MapExportDefinition } from '@shared/models/widget/maps/map-export.models';

export interface WidgetExportDefinition<T = any> {
  testWidget(widget: Widget): boolean;
  prepareExportInfo(dashboard: Dashboard, widget: Widget): T;
  updateFromExportInfo(widget: Widget, entityAliases: EntityAliases, filters: Filters, info: T): void;
}

const widgetExportDefinitions: WidgetExportDefinition[] = [
  MapExportDefinition
];

export const getWidgetExportDefinition = (widget: Widget): WidgetExportDefinition => {
  return widgetExportDefinitions.find(def => def.testWidget(widget));
}
