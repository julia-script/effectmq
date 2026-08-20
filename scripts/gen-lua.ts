/**
 * Compiles every `src/**\/*.lua` file into a sibling `.ts` module that
 * default-exports the Lua source as a string, so scripts can be imported
 * like any other module. Run via `pnpm gen:lua`.
 */
import { globSync, readFileSync, writeFileSync } from "node:fs";
import { basename } from "node:path";

for (const file of globSync("src/**/*.lua")) {
  const source = readFileSync(file, "utf8");
  const out = file.replace(/\.lua$/, ".ts");
  writeFileSync(
    out,
    `// generated from ${basename(file)} — do not edit\nexport default ${JSON.stringify(source)};\n`,
  );
  console.log(`${file} -> ${out}`);
}
