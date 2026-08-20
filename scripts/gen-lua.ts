/**
 * Compiles every `src/**\/*.lua` file into a sibling `.ts` module that
 * default-exports the Lua source as a string, so scripts can be imported
 * like any other module. Run via `pnpm gen:lua`.
 */
import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";

const luaFiles = readdirSync("src", { recursive: true })
  .filter((file) => file.endsWith(".lua"))
  .map((file) => join("src", file))
  .sort();

for (const file of luaFiles) {
  const source = readFileSync(file, "utf8");
  const out = file.replace(/\.lua$/, ".ts");
  writeFileSync(
    out,
    `// generated from ${basename(file)} — do not edit\nexport default ${JSON.stringify(source)};\n`,
  );
  console.log(`${file} -> ${out}`);
}
