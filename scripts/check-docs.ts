import { existsSync, globSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

const files = ["README.md", ...globSync("docs/**/*.md")].filter(
  (file) => !file.startsWith("docs/research/"),
);
const errors: string[] = [];
const linkPattern = /\[[^\]]*\]\(([^)]+)\)/g;
for (const file of files) {
  const text = readFileSync(file, "utf8");
  for (const match of text.matchAll(linkPattern)) {
    const target = match[1]?.split("#", 1)[0];
    if (
      target &&
      !target.startsWith("http://") &&
      !target.startsWith("https://") &&
      !target.startsWith("mailto:") &&
      !existsSync(resolve(dirname(file), target))
    ) {
      errors.push(`${file}: broken local link ${target}`);
    }
  }
  const forbidden = [
    /hand (?:you|a worker) .*exactly once/i,
    /handler (?:runs|executes) exactly once/i,
    /it runs once,? retries/i,
  ];
  for (const pattern of forbidden) {
    if (pattern.test(text))
      errors.push(`${file}: affirmative single-run claim`);
  }
}
if (errors.length > 0) throw new Error(errors.join("\n"));
console.log(`Documentation checks passed (${files.length} files)`);
