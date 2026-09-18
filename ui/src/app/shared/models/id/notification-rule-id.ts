import { EntityId } from '@shared/models/id/entity-id';
import { EntityType } from '@shared/models/entity-type.models';

export class NotificationRuleId implements EntityId {
  entityType = EntityType.NOTIFICATION_RULE;
  id: string;
  constructor(id: string) {
    this.id = id;
  }
}
