import { defineConfig } from "vitest/config";

export default defineConfig({
  // The empty workspace-root tsconfig.json is invalid JSON; pin esbuild to
  // this package's tsconfig so it doesn't walk up and choke on it.
  esbuild: { tsconfigRaw: "{}" },
  test: {
    include: ["src/**/*.test.ts"],
  },
});
