export function tbMatDateLocaleFactory(): string {
  try {
    const lang = (document.documentElement?.lang || '').toLowerCase();
    if (lang.startsWith('zh')) {
      return 'zh-CN';
    }
    if (lang.startsWith('en')) {
      return 'en-US';
    }
  } catch (_) { /* SSR / tests */ }
  return 'zh-CN';
}
