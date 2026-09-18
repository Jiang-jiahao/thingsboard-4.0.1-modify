import { EntityId } from '@shared/models/id/entity-id';
import { EntityType } from '@shared/models/entity-type.models';

export class NotificationTargetId implements EntityId {
  entityType = EntityType.NOTIFICATION_TARGET;
  id: string;
  constructor(id: string) {
    this.id = id;
  }
}
