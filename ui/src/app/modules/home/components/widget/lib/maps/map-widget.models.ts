import { defaultMapSettings, MapSetting, MapType } from '@shared/models/widget/maps/map.models';
import { BackgroundSettings, BackgroundType } from '@shared/models/widget-settings.models';
import { mergeDeep } from '@core/utils';
import { WidgetContext } from '@home/models/widget-component.models';
import { DeepPartial } from '@shared/models/common';
import { JnksIotMap } from '@home/components/widget/lib/maps/map';
import { JnksIotGeoMap } from '@home/components/widget/lib/maps/geo-map';
import { JnksIotImageMap } from '@home/components/widget/lib/maps/image-map';

export interface MapWidgetSettings extends MapSetting {
  background: BackgroundSettings;
  padding: string;
}

export const mapWidgetDefaultSettings: MapWidgetSettings =
  mergeDeep({} as MapWidgetSettings, defaultMapSettings as MapWidgetSettings, {
    background: {
      type: BackgroundType.color,
      color: '#fff',
      overlay: {
        enabled: false,
        color: 'rgba(255,255,255,0.72)',
        blur: 3
      }
    },
    padding: '8px'
} as MapWidgetSettings);

export const createMap = (ctx: WidgetContext,
                          inputSettings: DeepPartial<MapSetting>,
                          mapElement: HTMLElement): JnksIotMap<MapSetting> => {
  switch (inputSettings.mapType) {
    case MapType.geoMap:
      return new JnksIotGeoMap(ctx, inputSettings, mapElement);
    case MapType.image:
      return new JnksIotImageMap(ctx, inputSettings, mapElement);
  }
}
