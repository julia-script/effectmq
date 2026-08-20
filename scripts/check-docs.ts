import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, extname, join, resolve } from "node:path";

type CodeBlock = {
  readonly code: string;
  readonly file: string;
  readonly group?: string;
  readonly language: string;
  readonly line: number;
};

type TypeScriptSection = {
  readonly generatedEnd: number;
  readonly generatedStart: number;
  readonly sourceFile: string;
  readonly sourceStart: number;
};

type TypeScriptExample = {
  readonly code: string;
  readonly generatedFile: string;
  readonly label: string;
  readonly sections: ReadonlyArray<TypeScriptSection>;
};

const root = process.cwd();
const errors: string[] = [];

const markdownFiles = (directory: string) =>
  readdirSync(directory, { recursive: true, encoding: "utf8" })
    .filter((file) => [".md", ".mdx"].includes(extname(file)))
    .map((file) => join(directory, file));

const files = [
  "README.md",
  // Research notes preserve historical evidence and may intentionally quote
  // superseded code. The executable contract covers consumer documentation.
  ...markdownFiles("docs").filter(
    (file) => !file.startsWith(join("docs", "research")),
  ),
  ...markdownFiles(join("apps", "docs", "content")),
].sort();

const localTargetExists = (file: string, target: string) => {
  if (target.startsWith("/docs")) {
    const route = target.replace(/^\/docs\/?/, "");
    const contentRoot = join("apps", "docs", "content", "docs");
    return [
      join(contentRoot, `${route}.md`),
      join(contentRoot, `${route}.mdx`),
      join(contentRoot, route, "index.md"),
      join(contentRoot, route, "index.mdx"),
    ].some(existsSync);
  }
  return existsSync(resolve(dirname(file), target));
};

const extractCodeBlocks = (file: string, text: string) => {
  const blocks: CodeBlock[] = [];
  const lines = text.split(/\r?\n/);

  for (let index = 0; index < lines.length; index++) {
    const opening = /^```([^\s`]*)\s*(.*)$/.exec(lines[index] ?? "");
    if (!opening) continue;

    const language = (opening[1] ?? "").toLowerCase();
    const metadata = opening[2] ?? "";
    const group = /(?:^|\s)docs-check=([a-zA-Z0-9_-]+)(?:\s|$)/.exec(
      metadata,
    )?.[1];
    const line = index + 2;
    const body: string[] = [];
    let closed = false;

    for (index += 1; index < lines.length; index++) {
      if ((lines[index] ?? "").trim() === "```") {
        closed = true;
        break;
      }
      body.push(lines[index] ?? "");
    }

    if (!closed) {
      errors.push(`${file}:${line - 1}: unclosed code fence`);
      break;
    }
    blocks.push({ code: body.join("\n"), file, group, language, line });
  }

  return blocks;
};

const blocks: CodeBlock[] = [];
const linkPattern = /\[[^\]]*\]\(([^)]+)\)/g;
const forbidden = [
  /hand (?:you|a worker) .*exactly once/i,
  /handler (?:runs|executes) exactly once/i,
  /it runs once,? retries/i,
];

for (const file of files) {
  const text = readFileSync(file, "utf8");
  blocks.push(...extractCodeBlocks(file, text));

  for (const match of text.matchAll(linkPattern)) {
    const target = match[1]?.split("#", 1)[0];
    if (
      target &&
      !target.startsWith("http://") &&
      !target.startsWith("https://") &&
      !target.startsWith("mailto:") &&
      !localTargetExists(file, target)
    ) {
      errors.push(`${file}: broken local link ${target}`);
    }
  }

  for (const pattern of forbidden) {
    if (pattern.test(text))
      errors.push(`${file}: affirmative single-run claim`);
  }
}

const shellBlocks = blocks.filter((block) =>
  ["bash", "sh", "shell"].includes(block.language),
);
for (const block of shellBlocks) {
  const result = spawnSync("bash", ["-n"], {
    encoding: "utf8",
    input: block.code,
  });
  if (result.error) {
    errors.push(
      `${block.file}:${block.line}: could not validate ${block.language} example: ${result.error.message}`,
    );
  } else if (result.status !== 0) {
    errors.push(
      `${block.file}:${block.line}: invalid ${block.language} example\n${result.stderr.trim()}`,
    );
  }
}

const jsonBlocks = blocks.filter((block) => block.language === "json");
for (const block of jsonBlocks) {
  try {
    JSON.parse(block.code);
  } catch (cause) {
    errors.push(`${block.file}:${block.line}: invalid JSON example: ${cause}`);
  }
}

const typeScriptBlocks = blocks.filter((block) =>
  ["ts", "typescript"].includes(block.language),
);
const groups = new Map<string, CodeBlock[]>();
for (const block of typeScriptBlocks) {
  // Incremental snippets may share declarations with an earlier fence by using
  // the same `docs-check=<name>` metadata. Every TypeScript fence still belongs
  // to exactly one strict typecheck input.
  const key = block.group
    ? `${block.file}#${block.group}`
    : `${block.file}:${block.line}`;
  groups.set(key, [...(groups.get(key) ?? []), block]);
}

const examples: TypeScriptExample[] = [];
let exampleIndex = 0;
for (const [label, groupedBlocks] of groups) {
  const sections: TypeScriptSection[] = [];
  const generated: string[] = [];

  for (const block of groupedBlocks) {
    generated.push(`// Source: ${block.file}:${block.line}`);
    const generatedStart = generated.length + 1;
    generated.push(...block.code.split(/\r?\n/), "");
    sections.push({
      generatedEnd: generated.length - 1,
      generatedStart,
      sourceFile: block.file,
      sourceStart: block.line,
    });
  }
  generated.push("export {};");

  examples.push({
    code: generated.join("\n"),
    generatedFile: `${exampleIndex++}.ts`,
    label,
    sections,
  });
}

const temporaryDirectory = mkdtempSync(resolve(root, ".docs-check-"));
try {
  for (const example of examples) {
    writeFileSync(
      join(temporaryDirectory, example.generatedFile),
      example.code,
    );
  }
  writeFileSync(
    join(temporaryDirectory, "tsconfig.json"),
    JSON.stringify({
      compilerOptions: {
        esModuleInterop: true,
        forceConsistentCasingInFileNames: true,
        lib: ["dom", "esnext"],
        module: "nodenext",
        moduleResolution: "nodenext",
        noEmit: true,
        paths: {
          "@effectmq/core": ["../src/index.ts"],
          "@effectmq/core/*": ["../src/*.ts"],
        },
        skipLibCheck: true,
        strict: true,
        target: "esnext",
        types: ["node"],
      },
      include: ["./*.ts"],
    }),
  );

  const typecheck = spawnSync(
    resolve(root, "node_modules", ".bin", "tsc"),
    ["--project", join(temporaryDirectory, "tsconfig.json")],
    { encoding: "utf8" },
  );
  if (typecheck.error) {
    errors.push(
      `Could not typecheck TypeScript documentation examples: ${typecheck.error.message}`,
    );
  } else if (typecheck.status !== 0) {
    const diagnostics = `${typecheck.stdout}\n${typecheck.stderr}`
      .trim()
      .split(/\r?\n/)
      .filter(Boolean);
    for (const diagnostic of diagnostics) {
      const location = /([^/\\]+\.ts)\((\d+),(\d+)\): (.*)$/.exec(diagnostic);
      const example = location
        ? examples.find((candidate) => candidate.generatedFile === location[1])
        : undefined;
      if (!location || !example) {
        errors.push(`TypeScript documentation check: ${diagnostic}`);
        continue;
      }

      const generatedLine = Number(location[2]);
      const section = example.sections.find(
        (candidate) =>
          generatedLine >= candidate.generatedStart &&
          generatedLine <= candidate.generatedEnd,
      );
      if (!section) {
        errors.push(`${example.label}: ${location[4]}`);
        continue;
      }
      const sourceLine =
        section.sourceStart + (generatedLine - section.generatedStart);
      errors.push(`${section.sourceFile}:${sourceLine}: ${location[4]}`);
    }
  }
} finally {
  rmSync(temporaryDirectory, { force: true, recursive: true });
}

if (errors.length > 0) throw new Error(errors.join("\n"));
console.log(
  `Documentation checks passed (${files.length} files, ${typeScriptBlocks.length} TypeScript examples, ${shellBlocks.length} shell examples, ${jsonBlocks.length} JSON examples)`,
);
