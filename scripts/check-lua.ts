import { globSync, readFileSync } from "node:fs";
import { basename } from "node:path";

const stale: string[] = [];
for (const file of globSync("src/**/*.lua")) {
  const source = readFileSync(file, "utf8");
  const generated = file.replace(/\.lua$/, ".ts");
  const expected = `// generated from ${basename(file)} — do not edit\nexport default ${JSON.stringify(source)};\n`;
  let actual = "";
  try {
    actual = readFileSync(generated, "utf8");
  } catch {
    stale.push(`${generated} is missing`);
    continue;
  }
  if (actual !== expected) stale.push(`${generated} is stale`);
}
if (stale.length > 0) {
  throw new Error(
    `${stale.join("\n")}\nRun pnpm gen:lua and commit the result.`,
  );
}
console.log("Lua generated artifacts are current");
