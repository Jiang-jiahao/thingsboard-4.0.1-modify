import { EntityId } from '@shared/models/id/entity-id';
import { EntityType } from '@shared/models/entity-type.models';

export class NotificationTemplateId implements EntityId {
  entityType = EntityType.NOTIFICATION_TEMPLATE;
  id: string;
  constructor(id: string) {
    this.id = id;
  }
}
