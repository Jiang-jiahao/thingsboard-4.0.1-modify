import type { Plugin, PluginBuild, OutputFile } from 'esbuild';
import dirTree from 'directory-tree';
import * as packageJson from '../package.json';
import { gzip } from 'node:zlib';
import * as path from 'node:path';

const defineJnksIotVariablesPlugin: Plugin = {
  name: 'jnks-iot-define-variables',
  setup(build: PluginBuild) {
    const options = build.initialOptions;

    const langs: string[] = [];

    dirTree("./src/assets/locale/", {extensions: /\.json$/}, (item) => {
      /* It is expected what the name of a locale file has the following format: */
      /* 'locale.constant-LANG_CODE[_REGION_CODE].json', e.g. locale.constant-es.json or locale.constant-zh_CN.json*/
      langs.push(item.name.slice(item.name.lastIndexOf("-") + 1, -5));
    });
    options.define.JNKS_IOT_VERSION = JSON.stringify(packageJson.version);
    /* 构建时间戳：词条文件的 URL 会带上它，保证每次重建浏览器都会重新拉词条（见 translate-default-loader） */
    options.define.JNKS_IOT_BUILD_TS = JSON.stringify(String(Date.now()));
    options.define.SUPPORTED_LANGS = JSON.stringify(langs);
    options.define.ngJitMode = 'true';
  },
};

const resolveJQueryPlugin: Plugin = {
  name: 'jnks-iot-resolve-jquery-plugin',
  setup(build: PluginBuild) {
    if (isProduction()) {
      const jQueryPath = require.resolve('jquery');
      build.onResolve({filter: /^(jquery|\$)$/}, () => {
        return {path: jQueryPath};
      })
    }
  }
};

const compressFileTypes = ['.js', '.css', '.html', '.svg', '.png', '.jpg', '.ttf', '.gif', '.woff', '.woff2', '.eot', '.json'];
const compressThreshold = 10240;

const compressorPlugin: Plugin = {
  name: 'jnks-iot-compressor-plugin',
  setup(build) {
    build.onEnd(async result => {
      if (!result.outputFiles || !isProduction()) return;
      const outputExt = '.gz';
      const gzippedFiles: OutputFile[] = [];
      for (const file of result.outputFiles) {
        if (!compressFileTypes.some((ext) => ext === path.extname(file.path))) continue;
        if (file.contents.byteLength <= compressThreshold) continue;
        const compressedContent = await gzipContent(file.contents);
        const compressedFilePath = `${file.path}${outputExt}`;
        gzippedFiles.push(
          {
            path: compressedFilePath,
            hash: file.hash,
            contents: new Uint8Array(compressedContent),
            text: '',
          }
        );
      }
      result.outputFiles.push(...gzippedFiles);
    });
  },
};

async function gzipContent(content): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    gzip(content, (error, result) => {
      if (error) {
        reject(error);
      } else {
        resolve(result);
      }
    });
  });
}

function isProduction(): boolean {
  const configurationIndex = process.argv.indexOf('--configuration');
  let production = false;
  if (configurationIndex > -1) {
    const configurationValue = process.argv[configurationIndex + 1];
    production = configurationValue === 'production';
  }
  return production;
}

export default [defineJnksIotVariablesPlugin, resolveJQueryPlugin, compressorPlugin];
