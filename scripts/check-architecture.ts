import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const root = process.cwd();
const source = join(root, "src");
const failures: string[] = [];

const visit = (directory: string) => {
  for (const entry of readdirSync(directory)) {
    const path = join(directory, entry);
    if (statSync(path).isDirectory()) {
      if (entry !== "testing" && entry !== "scratchpad") visit(path);
      continue;
    }
    if (!entry.endsWith(".ts") || entry.endsWith(".test.ts")) continue;
    const text = readFileSync(path, "utf8");
    if (/^import[^;]+from ["']effect["']/m.test(text)) {
      failures.push(`${relative(root, path)} imports the broad effect root`);
    }
    if (/from ["']effect\/internal\//.test(text)) {
      failures.push(
        `${relative(root, path)} imports unsupported Effect internals`,
      );
    }
    if (/\bDate\.now\(\)|\bcrypto\.randomUUID\(/.test(text)) {
      failures.push(`${relative(root, path)} reads ambient time or randomness`);
    }
  }
};

visit(source);

for (const obsolete of ["src/Schemas.ts", "src/utils.ts"]) {
  if (existsSync(join(root, obsolete)))
    failures.push(`${obsolete} still exists`);
}

const manifest = JSON.parse(
  readFileSync(join(root, "package.json"), "utf8"),
) as {
  exports?: Record<string, unknown>;
};
for (const subpath of ["./Observability", "./TaskEvent", "./TaskRecord"]) {
  if (manifest.exports?.[subpath] === undefined) {
    failures.push(`package export ${subpath} is missing`);
  }
}

if (failures.length > 0) throw new Error(failures.join("\n"));
console.log("Architecture checks passed");
