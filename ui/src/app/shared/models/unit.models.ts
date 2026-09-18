import { ResourcesService } from '@core/services/resources.service';
import { Observable } from 'rxjs';

export interface Unit {
  name: string;
  symbol: string;
  tags: string[];
}

export enum UnitsType {
  capacity = 'capacity'
}

export enum Units {
  percent = '%',
  liters = 'L'
}

export const unitBySymbol = (_units: Array<Unit>, symbol: string): Unit => _units.find(u => u.symbol === symbol);

const searchUnitTags = (unit: Unit, searchText: string): boolean =>
  !!unit.tags.find(t => t.toUpperCase().includes(searchText.toUpperCase()));

export const searchUnits = (_units: Array<Unit>, searchText: string): Array<Unit> => _units.filter(
    u => u.symbol.toUpperCase().includes(searchText.toUpperCase()) ||
      u.name.toUpperCase().includes(searchText.toUpperCase()) ||
      searchUnitTags(u, searchText)
);

export const getUnits = (resourcesService: ResourcesService): Observable<Array<Unit>> =>
  resourcesService.loadJsonResource('/assets/metadata/units.json');
