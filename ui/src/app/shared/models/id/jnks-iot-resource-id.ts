import { EntityId } from '@shared/models/id/entity-id';
import { EntityType } from '@shared/models/entity-type.models';

export class JnksIotResourceId implements EntityId {
  entityType = EntityType.JNKS_IOT_RESOURCE;
  id: string;
  constructor(id: string) {
    this.id = id;
  }
}
