import {
  DataLayerTooltipSettings,
  DataLayerTooltipTrigger, processTooltipTemplate,
  JnksIotMapDatasource
} from '@shared/models/widget/maps/map.models';
import { JnksIotMap } from '@home/components/widget/lib/maps/map';
import { FormattedData } from '@shared/models/widget.models';
import L from 'leaflet';
import { DataLayerPatternProcessor } from '@home/components/widget/lib/maps/data-layer/map-data-layer';

export const createTooltip = (map: JnksIotMap<any>,
                              layer: L.Layer,
                              settings: DataLayerTooltipSettings,
                              data: FormattedData<JnksIotMapDatasource>,
                              canOpen: () => boolean): L.Popup => {
  const tooltip = L.popup({autoClose: settings.autoclose, closeOnClick: false});
  (tooltip as any)._source = layer;
  layer.on('move', (e) => {
    tooltip.setLatLng((e as any).latlng);
  });
  layer.on('remove', () => {
    tooltip.close();
  });
  if (settings.trigger === DataLayerTooltipTrigger.click) {
    layer.on('click', (e) => {
      L.DomEvent.stop(e);
      if (tooltip.isOpen()) {
        tooltip.close();
      } else if (canOpen()) {
        if ((tooltip as any)._prepareOpen((layer as any)._latlng)) {
          tooltip.openOn(map.getMap());
        }
      }
    });
  } else if (settings.trigger === DataLayerTooltipTrigger.hover) {
    layer.on('mouseover', () => {
      if (canOpen()) {
        if ((tooltip as any)._prepareOpen((layer as any)._latlng)) {
          tooltip.openOn(map.getMap());
        }
      }
    });
    layer.on('mouseout', () => {
      tooltip.close();
    });
  }
  layer.on('popupopen', () => {
    bindTooltipActions(map, tooltip, settings, data);
  });
  return tooltip;
}

export const updateTooltip = (map: JnksIotMap<any>,
                              tooltip: L.Popup,
                              settings: DataLayerTooltipSettings,
                              processor: DataLayerPatternProcessor,
                              data: FormattedData<JnksIotMapDatasource>,
                              dsData: FormattedData<JnksIotMapDatasource>[]): void => {
  let tooltipTemplate = processor.processPattern(data, dsData);
  tooltipTemplate = processTooltipTemplate(tooltipTemplate);
  tooltip.setContent(tooltipTemplate);
  if (tooltip.isOpen() && tooltip.getElement()) {
    bindTooltipActions(map, tooltip, settings, data);
  }
}

const bindTooltipActions = (map: JnksIotMap<any>, tooltip: L.Popup, settings: DataLayerTooltipSettings, data: FormattedData<JnksIotMapDatasource>): void => {
  const actions = tooltip.getElement().getElementsByClassName('jnks-iot-custom-action');
  Array.from(actions).forEach(
    (element: HTMLElement) => {
      const actionName = element.getAttribute('data-action-name');
      if (settings?.tagActions) {
        const action = settings.tagActions.find(action => action.name === actionName);
        if (action) {
          element.onclick = ($event) =>
          {
            map.dataItemClick($event, action, data);
            return false;
          };
        }
      }
    }
  );
}
