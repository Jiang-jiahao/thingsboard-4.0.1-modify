import { MissingTranslationHandler, MissingTranslationHandlerParams } from '@ngx-translate/core';
import { customTranslationsPrefix } from '@app/shared/models/constants';

export class TbMissingTranslationHandler implements MissingTranslationHandler {
  handle(params: MissingTranslationHandlerParams) {
    if (params.key && !params.key.startsWith(customTranslationsPrefix)) {
      console.warn('Translation for \'' + params.key + '\' doesn\'t exist');
      let translations: any;
      const parts = params.key.split('.');
      for (let i=parts.length-1; i>=0; i--) {
        const newTranslations = {};
        if (i === parts.length-1) {
          newTranslations[parts[i]] = params.key;
        } else {
          newTranslations[parts[i]] = translations;
        }
        translations = newTranslations;
      }
      params.translateService.setTranslation(params.translateService.currentLang, translations, true);
    }
  }
}
