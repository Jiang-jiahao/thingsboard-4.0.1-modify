import { HasUUID } from '@shared/models/id/has-uuid';

export class AlarmCommentId implements HasUUID {
  id: string;
  constructor(id: string) {
    this.id = id;
  }
}
