///
/// Copyright © 2016-2025 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { formatDate } from '@angular/common';
import { Injectable } from '@angular/core';
import { NativeDatetimeAdapter } from '@mat-datetimepicker/core';

const ISO_DATE = 'yyyy-MM-dd';
const ISO_DATETIME = 'yyyy-MM-dd HH:mm';
const ISO_TIME = 'HH:mm';

/** Matches MAT_NATIVE_DATETIME_FORMATS.display.dateInput */
function isDateInputFormat(fmt: Record<string, string>): boolean {
  return fmt.year === 'numeric'
    && fmt.month === '2-digit'
    && fmt.day === '2-digit'
    && fmt.hour === undefined
    && fmt.minute === undefined;
}

/** Matches MAT_NATIVE_DATETIME_FORMATS.display.datetimeInput */
function isDatetimeInputFormat(fmt: Record<string, string>): boolean {
  return fmt.year === 'numeric'
    && fmt.month === '2-digit'
    && fmt.day === '2-digit'
    && fmt.hour === '2-digit'
    && fmt.minute === '2-digit';
}

/** Matches MAT_NATIVE_DATETIME_FORMATS.display.timeInput */
function isTimeInputFormat(fmt: Record<string, string>): boolean {
  return fmt.hour === '2-digit'
    && fmt.minute === '2-digit'
    && fmt.year === undefined
    && fmt.month === undefined
    && fmt.day === undefined;
}

@Injectable()
export class CustomDateAdapter extends NativeDatetimeAdapter {

  override format(date: Date, displayFormat: Object): string {
    if (!this.isValid(date)) {
      return '';
    }
    const fmt = displayFormat as Record<string, string>;
    if (!fmt || typeof fmt !== 'object') {
      return super.format(date, displayFormat);
    }

    if (isDatetimeInputFormat(fmt)) {
      return formatDate(date, ISO_DATETIME, 'en-US');
    }
    if (isTimeInputFormat(fmt)) {
      return formatDate(date, ISO_TIME, 'en-US');
    }
    if (isDateInputFormat(fmt)) {
      return formatDate(date, ISO_DATE, 'en-US');
    }

    return super.format(date, displayFormat);
  }

  override parse(value: string | number): Date | null {
    if (value == null || value === '') {
      return null;
    }
    if (typeof value === 'number') {
      return new Date(value);
    }

    const trimmed = String(value).trim();

    let match = trimmed.match(
      /^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})(?:[,\sT]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
    );
    if (match) {
      const parsed = new Date(
        +match[1],
        +match[2] - 1,
        +match[3],
        +(match[4] ?? 0),
        +(match[5] ?? 0),
        +(match[6] ?? 0)
      );
      return isNaN(parsed.getTime()) ? null : parsed;
    }

    match = trimmed.match(/^(\d{1,2})[/.](\d{1,2})[/.](\d{4})(?:[,\s]+(\d{1,2}):(\d{2}))?$/);
    if (match) {
      const parsed = new Date(+match[3], +match[2] - 1, +match[1], +(match[4] ?? 0), +(match[5] ?? 0));
      return isNaN(parsed.getTime()) ? null : parsed;
    }

    let newDate = trimmed;
    const formatToParts = Intl.DateTimeFormat(this.locale).formatToParts();
    if (formatToParts[0]?.type?.toLowerCase() === 'day') {
      const literal = formatToParts[1].value;
      newDate = newDate.replace(new RegExp(`(\\d+[${literal}])(\\d+[${literal}])`), '$2$1');
    }
    const timestamp = Date.parse(newDate);
    return isNaN(timestamp) ? null : new Date(timestamp);
  }

}
