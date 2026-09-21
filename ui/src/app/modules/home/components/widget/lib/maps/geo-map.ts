import {
  DEFAULT_ZOOM_LEVEL,
  defaultGeoMapSettings,
  GeoMapSettings,
  latLngPointToBounds,
  MapZoomAction,
  JnksIotCircleData,
  JnksIotPolygonCoordinate,
  JnksIotPolygonCoordinates,
  JnksIotPolygonRawCoordinate,
  JnksIotPolygonRawCoordinates
} from '@shared/models/widget/maps/map.models';
import { WidgetContext } from '@home/models/widget-component.models';
import { DeepPartial } from '@shared/models/common';
import { forkJoin, Observable, of } from 'rxjs';
import L from 'leaflet';
import { map, tap } from 'rxjs/operators';
import { JnksIotMapLayer } from '@home/components/widget/lib/maps/map-layer';
import { JnksIotMap } from '@home/components/widget/lib/maps/map';

export class JnksIotGeoMap extends JnksIotMap<GeoMapSettings> {

  constructor(protected ctx: WidgetContext,
              protected inputSettings: DeepPartial<GeoMapSettings>,
              protected containerElement: HTMLElement) {
    super(ctx, inputSettings, containerElement);
  }

  protected defaultSettings(): GeoMapSettings {
    return defaultGeoMapSettings;
  }

  protected createMap(): Observable<L.Map> {
    const map = L.map(this.mapElement, {
      scrollWheelZoom: this.settings.zoomActions.includes(MapZoomAction.scroll),
      doubleClickZoom: this.settings.zoomActions.includes(MapZoomAction.doubleClick),
      zoomControl: this.settings.zoomActions.includes(MapZoomAction.controlButtons),
      zoom: this.settings.defaultZoomLevel || DEFAULT_ZOOM_LEVEL,
      center: this.defaultCenterPosition
    }).setView(this.defaultCenterPosition, this.settings.defaultZoomLevel || DEFAULT_ZOOM_LEVEL);
    return of(map);
  }

  protected onResize(): void {}

  protected fitBounds(bounds: L.LatLngBounds) {
    if (bounds.isValid()) {
      if (!this.settings.fitMapBounds && this.settings.defaultZoomLevel) {
        this.map.setZoom(this.settings.defaultZoomLevel, { animate: false });
        if (this.settings.useDefaultCenterPosition) {
          this.map.panTo(this.defaultCenterPosition, { animate: false });
        }
        else {
          this.map.panTo(bounds.getCenter());
        }
      } else {
        this.map.once('zoomend', () => {
          let minZoom = this.settings.minZoomLevel;
          if (this.settings.defaultZoomLevel) {
            minZoom = Math.max(minZoom, this.settings.defaultZoomLevel);
          }
          if (this.map.getZoom() > minZoom) {
            this.map.setZoom(minZoom, { animate: false });
          }
        });
        if (this.settings.useDefaultCenterPosition) {
          bounds = bounds.extend(this.defaultCenterPosition);
        }
        this.map.fitBounds(bounds, { padding: [50, 50], animate: false });
        this.map.invalidateSize();
      }
    }
  }

  protected doSetupControls(): Observable<any> {
    return this.loadLayers().pipe(
      tap((layers: L.TB.LayerData[]) => {
        if (layers.length) {
          const layer = layers[0];
          layer.layer.addTo(this.map);
          this.map.attributionControl.setPrefix(layer.attributionPrefix);
          if (layers.length > 1) {
            const sidebar = this.getSidebar();
            L.TB.layers({
              layers,
              sidebar,
              position: this.settings.controlsPosition,
              uiClass: 'jnks-iot-layers',
              paneTitle: this.ctx.translate.instant('widgets.maps.layer.map-layers'),
              buttonTitle: this.ctx.translate.instant('widgets.maps.layer.layers'),
            }).addTo(this.map);
          }
        }
      })
    );

  }

  private loadLayers(): Observable<L.TB.LayerData[]> {
    const layers = this.settings.layers.map(settings => JnksIotMapLayer.fromSettings(this.ctx, settings));
    return forkJoin(layers.map(layer => layer.loadLayer(this.map))).pipe(
      map((layersData) => {
        return layersData.filter(l => l !== null);
      })
    );
  }

  public locationDataToLatLng(position: {x: number; y: number}): L.LatLng {
    return L.latLng(position.x, position.y) as L.LatLng;
  }

  public latLngToLocationData(position: L.LatLng): {x: number; y: number} {
    position = position ? latLngPointToBounds(position, this.southWest, this.northEast, 0) : {lat: null, lng: null} as L.LatLng;
    return {
      x: position.lat,
      y: position.lng
    }
  }

  public polygonDataToCoordinates(expression: JnksIotPolygonRawCoordinates): JnksIotPolygonRawCoordinates {
    return (expression).map((el: JnksIotPolygonRawCoordinate) => {
      if (!Array.isArray(el[0]) && !Array.isArray(el[1]) && el.length === 2) {
        return el;
      } else if (Array.isArray(el) && el.length) {
        return this.polygonDataToCoordinates(el as JnksIotPolygonRawCoordinates) as JnksIotPolygonRawCoordinate;
      } else {
        return null;
      }
    }).filter(el => !!el);
  }

  public coordinatesToPolygonData(coordinates: JnksIotPolygonCoordinates): JnksIotPolygonRawCoordinates {
    if (coordinates.length) {
      return coordinates.map((point: JnksIotPolygonCoordinate) => {
        if (Array.isArray(point)) {
          return this.coordinatesToPolygonData(point) as JnksIotPolygonRawCoordinate;
        } else {
          const convertPoint = latLngPointToBounds(point, this.southWest, this.northEast);
          return [convertPoint.lat, convertPoint.lng];
        }
      });
    }
    return [];
  }

  public circleDataToCoordinates(circle: JnksIotCircleData): JnksIotCircleData {
    const centerPoint = latLngPointToBounds(new L.LatLng(circle.latitude, circle.longitude), this.southWest, this.northEast);
    circle.latitude = centerPoint.lat;
    circle.longitude = centerPoint.lng;
    return circle;
  }

  public coordinatesToCircleData(center: L.LatLng, radius: number): JnksIotCircleData {
    let circleData: JnksIotCircleData = null;
    if (center) {
      const position = latLngPointToBounds(center, this.southWest, this.northEast);
      circleData = {
        latitude: position.lat,
        longitude: position.lng,
        radius
      };
    }
    return circleData;
  }


}
