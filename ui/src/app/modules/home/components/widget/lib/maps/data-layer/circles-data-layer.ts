import {
  CirclesDataLayerSettings,
  defaultBaseCirclesDataLayerSettings,
  isJSON, MapDataLayerType,
  JnksIotCircleData,
  JnksIotMapDatasource
} from '@shared/models/widget/maps/map.models';
import L from 'leaflet';
import { DataKey, FormattedData } from '@shared/models/widget.models';
import { ShapeStyleInfo, JnksIotShapesDataLayer } from '@home/components/widget/lib/maps/data-layer/shapes-data-layer';
import { JnksIotMap } from '@home/components/widget/lib/maps/map';
import { Observable } from 'rxjs';
import { isNotEmptyStr } from '@core/utils';
import {
  JnksIotLatestDataLayerItem,
  UnplacedMapDataItem
} from '@home/components/widget/lib/maps/data-layer/latest-map-data-layer';
import { map } from 'rxjs/operators';

class JnksIotCircleDataLayerItem extends JnksIotLatestDataLayerItem<CirclesDataLayerSettings, JnksIotCirclesDataLayer> {

  private circle: L.Circle;
  private circleStyleInfo: ShapeStyleInfo;
  private editing = false;

  constructor(data: FormattedData<JnksIotMapDatasource>,
              dsData: FormattedData<JnksIotMapDatasource>[],
              protected settings: CirclesDataLayerSettings,
              protected dataLayer: JnksIotCirclesDataLayer) {
    super(data, dsData, settings, dataLayer);
  }

  public isEditing() {
    return this.editing;
  }

  public updateBubblingMouseEvents() {
    this.circle.options.bubblingMouseEvents =  !this.dataLayer.isEditMode();
  }

  public remove() {
    super.remove();
    if (this.circleStyleInfo?.patternId) {
      this.dataLayer.getMap().unUseShapePattern(this.circleStyleInfo.patternId);
    }
  }

  protected create(data: FormattedData<JnksIotMapDatasource>, dsData: FormattedData<JnksIotMapDatasource>[]): L.Layer {
    const circleData = this.dataLayer.extractCircleCoordinates(data);
    const center = new L.LatLng(circleData.latitude, circleData.longitude);
    this.circle = L.circle(center, {
      bubblingMouseEvents: !this.dataLayer.isEditMode(),
      radius: circleData.radius,
      snapIgnore: !this.dataLayer.isSnappable()
    });

    this.dataLayer.getShapeStyle(data, dsData, this.circleStyleInfo?.patternId).subscribe((styleInfo) => {
      this.circleStyleInfo = styleInfo;
      if (this.circle) {
        this.circle.setStyle(this.circleStyleInfo.style);
      }
    });

    this.updateLabel(data, dsData);
    return this.circle;
  }

  protected unbindLabel() {
    this.circle.unbindTooltip();
  }

  protected bindLabel(content: L.Content): void {
    this.circle.bindTooltip(content, { className: 'jnks-iot-circle-label', permanent: true, direction: 'center'})
    .openTooltip(this.circle.getLatLng());
  }

  protected doUpdate(data: FormattedData<JnksIotMapDatasource>, dsData: FormattedData<JnksIotMapDatasource>[]): void {
    this.dataLayer.getShapeStyle(data, dsData, this.circleStyleInfo?.patternId).subscribe((styleInfo) => {
      this.circleStyleInfo = styleInfo;
      this.updateCircleShape(data);
      this.updateTooltip(data, dsData);
      this.updateLabel(data, dsData);
      this.circle.setStyle(this.circleStyleInfo.style);
    });
  }

  protected doInvalidateCoordinates(data: FormattedData<JnksIotMapDatasource>, _dsData: FormattedData<JnksIotMapDatasource>[]): void {
    this.updateCircleShape(data);
  }

  protected addItemClass(clazz: string): void {
    if ((this.circle as any)._path) {
      L.DomUtil.addClass((this.circle as any)._path, clazz);
    }
  }

  protected removeItemClass(clazz: string): void {
    if ((this.circle as any)._path) {
      L.DomUtil.removeClass((this.circle as any)._path, clazz);
    }
  }

  protected enableDrag(): void {
    this.circle.pm.setOptions({
      snappable: this.dataLayer.isSnappable()
    });
    this.circle.pm.enableLayerDrag();
    this.circle.on('pm:dragstart', () => {
      this.editing = true;
    });
    this.circle.on('pm:dragend', () => {
      this.saveCircleCoordinates();
      this.editing = false;
    });
  }

  protected disableDrag(): void {
    this.circle.pm.disableLayerDrag();
    this.circle.off('pm:dragstart');
    this.circle.off('pm:dragend');
  }

  protected onSelected(): L.TB.ToolbarButtonOptions[] {
    if (this.dataLayer.isEditEnabled()) {
      this.circle.on('pm:markerdragstart', () => this.editing = true);
      this.circle.on('pm:markerdragend', () => this.editing = false);
      this.circle.on('pm:edit', () => this.saveCircleCoordinates());
      this.circle.pm.enable({draggable: true, snappable: this.dataLayer.isSnappable()});
    }
    return [];
  }

  protected onDeselected(): void {
    if (this.dataLayer.isEditEnabled()) {
      this.circle.pm.disable();
      this.circle.off('pm:markerdragstart');
      this.circle.off('pm:markerdragend');
      this.circle.off('pm:edit');
    }
  }

  protected removeDataItemTitle(): string {
    return this.dataLayer.getCtx().translate.instant('widgets.maps.data-layer.circle.remove-circle-for', {entityName: this.data.entityName});
  }

  protected removeDataItem(): Observable<any> {
    return this.dataLayer.saveCircleCoordinates(this.data, null, null);
  }

  private saveCircleCoordinates() {
    const center = this.circle.getLatLng();
    const radius = this.circle.getRadius();
    this.dataLayer.saveCircleCoordinates(this.data, center, radius).subscribe();
  }

  private updateCircleShape(data: FormattedData<JnksIotMapDatasource>) {
    if (this.editing) {
      return;
    }
    const circleData = this.dataLayer.extractCircleCoordinates(data);
    const center = new L.LatLng(circleData.latitude, circleData.longitude);
    if (!this.circle.getLatLng().equals(center)) {
      this.circle.setLatLng(center);
    }
    if (this.circle.getRadius() !== circleData.radius) {
      this.circle.setRadius(circleData.radius);
    }
  }
}

export class JnksIotCirclesDataLayer extends JnksIotShapesDataLayer<CirclesDataLayerSettings, JnksIotCirclesDataLayer> {

  constructor(protected map: JnksIotMap<any>,
              inputSettings: CirclesDataLayerSettings) {
    super(map, inputSettings);
  }

  public dataLayerType(): MapDataLayerType {
    return 'circles';
  }

  public placeItem(item: UnplacedMapDataItem, layer: L.Layer): void {
    if (layer instanceof L.Circle) {
      const center = layer.getLatLng();
      const radius = layer.getRadius();
      this.saveCircleCoordinates(item.entity, center, radius).subscribe(
        (converted) => {
          item.entity[this.settings.circleKey.label] = JSON.stringify(converted);
          this.createItemFromUnplaced(item);
        }
      );
    } else {
      console.warn('Unable to place item, layer is not a circle.');
    }
  }

  public extractCircleCoordinates(data: FormattedData<JnksIotMapDatasource>) {
    const circleData: JnksIotCircleData = JSON.parse(data[this.settings.circleKey.label]);
    return this.map.circleDataToCoordinates(circleData);
  }

  public saveCircleCoordinates(data: FormattedData<JnksIotMapDatasource>, center: L.LatLng, radius: number): Observable<JnksIotCircleData> {
    const converted = center ? this.map.coordinatesToCircleData(center, radius) : null;
    const circleData = [
      {
        dataKey: this.settings.circleKey,
        value: converted
      }
    ];
    return this.map.saveItemData(data.$datasource, circleData, this.settings.edit?.attributeScope).pipe(
      map(() => converted)
    );
  }

  protected getDataKeys(): DataKey[] {
    return [this.settings.circleKey];
  }

  protected defaultBaseSettings(map: JnksIotMap<any>): Partial<CirclesDataLayerSettings> {
    return defaultBaseCirclesDataLayerSettings(map.type());
  }

  protected doSetup(): Observable<void> {
    return super.doSetup();
  }

  protected isValidLayerData(layerData: FormattedData<JnksIotMapDatasource>): boolean {
    return layerData && isNotEmptyStr(layerData[this.settings.circleKey.label]) && isJSON(layerData[this.settings.circleKey.label]);
  }

  protected createLayerItem(data: FormattedData<JnksIotMapDatasource>, dsData: FormattedData<JnksIotMapDatasource>[]): JnksIotLatestDataLayerItem<CirclesDataLayerSettings, JnksIotCirclesDataLayer> {
    return new JnksIotCircleDataLayerItem(data, dsData, this.settings, this);
  }

}
