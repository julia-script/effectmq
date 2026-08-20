import { execFileSync } from "node:child_process";
import {
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const directory = mkdtempSync(join(tmpdir(), "effectmq-package-"));
try {
  execFileSync("pnpm", ["pack", "--pack-destination", directory], {
    stdio: "pipe",
  });
  const tarball = join(
    directory,
    readdirSync(directory).find((file) => file.endsWith(".tgz")) ?? "",
  );
  const files = execFileSync("tar", ["-tzf", tarball], {
    encoding: "utf8",
  })
    .trim()
    .split("\n");
  const unintended = files.filter((file) =>
    /(?:\.test\.|\/testing\/|\/scratchpad\/|\.DS_Store$|\/openspec\/|\/docs\/research\/)/.test(
      file,
    ),
  );
  if (unintended.length > 0) {
    throw new Error(`Unintended tarball files:\n${unintended.join("\n")}`);
  }
  for (const required of [
    "package/package.json",
    "package/dist/index.js",
    "package/dist/index.d.ts",
    "package/dist/NodeRedisPool.js",
    "package/dist/Worker.js",
    "package/dist/cli/inspect-pre-release-data.js",
    "package/src/lua/taskEngine.lua",
  ]) {
    if (!files.includes(required)) throw new Error(`Missing ${required}`);
  }

  const consumer = join(directory, "consumer");
  execFileSync("mkdir", [consumer]);
  writeFileSync(
    join(consumer, "package.json"),
    JSON.stringify({
      name: "effectmq-esm-consumer",
      private: true,
      type: "module",
    }),
  );
  execFileSync(
    "pnpm",
    [
      "add",
      "--ignore-workspace",
      tarball,
      "effect@4.0.0-beta.107",
      "@effect/platform-node@4.0.0-beta.107",
      "typescript@7.0.2",
      "@types/node@22",
    ],
    { cwd: consumer, stdio: "pipe" },
  );
  const subpaths = [
    "NodeRedisPool",
    "Observability",
    "RedisPool",
    "Scheduler",
    "StorageProtocol",
    "Task",
    "TaskEngine",
    "TaskEvent",
    "TaskQueue",
    "TaskRecord",
    "Worker",
  ];
  const imports = subpaths
    .map((name) => `import * as ${name} from "@effectmq/core/${name}";`)
    .join("\n");
  const assertions = subpaths
    .map(
      (name) =>
        `if (Object.keys(${name}).length === 0) throw new Error("empty ${name} export");`,
    )
    .join("\n");
  const consumerSource = `import * as Core from "@effectmq/core";\nimport { NodeRuntime } from "@effect/platform-node";\n${imports}\nif (Object.keys(Core).length < 9) throw new Error("incomplete root export");\nif (NodeRuntime === undefined) throw new Error("platform-node beta is incompatible");\n${assertions}\nconsole.log("packed ESM exports load");\n`;
  const removedSubpathCheck = `\nfor (const path of ["@effectmq/core/MessagePack", "@effectmq/core/EngineRecord", "@effectmq/core/RetrySchedule", "@effectmq/core/Schemas"]) {\n  try {\n    await import(path);\n    throw new Error(\`removed subpath loaded: \${path}\`);\n  } catch (error) {\n    if (error instanceof Error && error.message.startsWith("removed subpath loaded:")) throw error;\n  }\n}\n`;
  writeFileSync(
    join(consumer, "index.mjs"),
    consumerSource + removedSubpathCheck,
  );
  writeFileSync(join(consumer, "index.ts"), consumerSource);
  writeFileSync(
    join(consumer, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        module: "NodeNext",
        moduleResolution: "NodeNext",
        noEmit: true,
        skipLibCheck: true,
        strict: true,
        target: "ES2022",
      },
      include: ["index.ts"],
    }),
  );
  execFileSync("node", ["index.mjs"], { cwd: consumer, stdio: "inherit" });
  execFileSync("pnpm", ["exec", "tsc", "-p", "tsconfig.json"], {
    cwd: consumer,
    stdio: "inherit",
  });

  const manifest = JSON.parse(
    readFileSync(
      join(consumer, "node_modules/@effectmq/core/package.json"),
      "utf8",
    ),
  ) as { bin?: Record<string, string> };
  if (manifest.bin?.["effectmq-storage-inspect"] === undefined) {
    throw new Error("storage inspection CLI is missing from packed manifest");
  }
  console.log(`Package verification passed (${files.length} tarball entries)`);
} finally {
  rmSync(directory, { force: true, recursive: true });
}
