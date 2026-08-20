import { readFileSync } from "node:fs";

const workflow = readFileSync(".github/workflows/release.yml", "utf8");
const ciWorkflow = readFileSync(".github/workflows/ci.yml", "utf8");
const manifest = JSON.parse(readFileSync("package.json", "utf8")) as {
  repository?: { url?: string };
  publishConfig?: { access?: string; provenance?: boolean };
  scripts?: Record<string, string>;
};

const errors: string[] = [];
const requireMatch = (pattern: RegExp, description: string) => {
  if (!pattern.test(workflow)) errors.push(`release.yml: ${description}`);
};

requireMatch(
  /workflow_run:/,
  "publication must be triggered by a completed CI run",
);
requireMatch(/workflows:\s*\[CI\]/, "the triggering workflow must be CI");
requireMatch(/branches:\s*\[main\]/, "publication must be limited to main");
requireMatch(
  /workflow_run\.conclusion\s*==\s*'success'/,
  "the triggering CI run must have succeeded",
);
requireMatch(
  /ref:\s*\$\{\{\s*github\.event\.workflow_run\.head_sha\s*\}\}/,
  "checkout must use the gated CI commit",
);
requireMatch(
  /GATED_SHA:\s*\$\{\{\s*github\.event\.workflow_run\.head_sha\s*\}\}/,
  "the gated commit must be verified before publication",
);
requireMatch(/id-token:\s*write/, "OIDC id-token permission must be enabled");
requireMatch(
  /environment:\s*npm-production/,
  "publication must use the protected npm-production environment",
);
requireMatch(
  /npm@11\.5\.1/,
  "npm must support trusted publishing (11.5.1 or newer)",
);
requireMatch(
  /NPM_CONFIG_PROVENANCE:\s*["']?true["']?/,
  "npm provenance must be enabled",
);

const qualityJob =
  /\n {2}quality-and-tests:\n[\s\S]*?(?=\n {2}[a-z][a-z-]+:\n)/.exec(
    ciWorkflow,
  )?.[0];
if (
  !qualityJob ||
  !/- uses: actions\/checkout@v4\n\s+with:\n\s+fetch-depth:\s*0/.test(
    qualityJob,
  )
) {
  errors.push(
    "ci.yml: quality checks must fetch full history before running changeset status",
  );
}
if (
  manifest.scripts?.["check:changesets"] !==
  "changeset status --since=origin/main"
) {
  errors.push(
    "package.json: changeset checks must compare against the fetched origin/main ref",
  );
}

if (/\b(?:NPM_TOKEN|NODE_AUTH_TOKEN)\b/.test(workflow)) {
  errors.push("release.yml: long-lived npm publication tokens are forbidden");
}
if (manifest.repository?.url !== "https://github.com/julia-script/effectmq") {
  errors.push(
    "package.json: repository.url must exactly identify julia-script/effectmq for npm OIDC",
  );
}
if (manifest.publishConfig?.access !== "public") {
  errors.push("package.json: publishConfig.access must be public");
}
if (manifest.publishConfig?.provenance !== true) {
  errors.push("package.json: publishConfig.provenance must remain enabled");
}

if (errors.length > 0) throw new Error(errors.join("\n"));
console.log("Release trust checks passed");
