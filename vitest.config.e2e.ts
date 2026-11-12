import { defineConfig } from 'vitest/config';
import { TEST_DATABASE_URL } from './test/test-database.js';

export default defineConfig({
  resolve: { tsconfigPaths: true },
  test: {
    globals: true,
    root: './',
    include: ['**/*.e2e-spec.ts'],
    // Os testes e2e rodam em um banco próprio, criado pelo global setup.
    globalSetup: ['./test/test-database.ts'],
    env: { DATABASE_URL: TEST_DATABASE_URL },
  },
});
