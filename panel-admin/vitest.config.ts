import path from 'node:path';
import {defineConfig, type Plugin} from 'vitest/config';

/** Treat all .css imports as empty modules so jsdom tests don't error on CSS. */
const cssMockPlugin: Plugin = {
  name: 'vitest-css-mock',
  transform(_code, id) {
    if (id.endsWith('.css')) return {code: 'export default {}'};
  },
};

export default defineConfig({
  plugins: [cssMockPlugin],
  resolve: {
    alias: {
      '@': path.resolve(process.cwd(), 'src'),
    },
  },
  test: {
    include: ['tests/unit/**/*.test.ts', 'tests/unit/**/*.test.tsx'],
    environment: 'node',
    setupFiles: ['tests/unit/setup.ts'],
    globals: true,
    clearMocks: true,
    server: {
      deps: {
        // Force vitest to inline gravity-ui ESM so our CSS mock plugin runs on it
        inline: [/@gravity-ui\/.*/],
      },
    },
  },
});
