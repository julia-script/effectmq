import { readFileSync, readdirSync } from "node:fs";
import { basename, join } from "node:path";

const stale: string[] = [];
const luaFiles = readdirSync("src", { recursive: true, encoding: "utf8" })
  .filter((file) => file.endsWith(".lua"))
  .map((file) => join("src", file))
  .sort();

for (const file of luaFiles) {
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
