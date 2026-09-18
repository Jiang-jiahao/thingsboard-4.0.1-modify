import { EntityId } from '@shared/models/id/entity-id';
import { EntityType } from '@shared/models/entity-type.models';

export class NotificationId implements EntityId {
  entityType = EntityType.NOTIFICATION;
  id: string;
  constructor(id: string) {
    this.id = id;
  }
}
