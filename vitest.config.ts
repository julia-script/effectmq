import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    // testcontainers: first use in a worker may pull + boot a Redis image
    hookTimeout: 60_000,
    teardownTimeout: 30_000,
    testTimeout: 30_000,
  },
});
