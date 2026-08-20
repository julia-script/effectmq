# Contributing

Open an issue or design discussion before a large protocol or public API change.
Correctness changes should include the smallest failing test first and preserve
the v1 storage, generation identity, lease fencing, bounded-work, and
at-least-once contracts.

Install the pinned toolchain and run the release gates:

```sh
pnpm install --frozen-lockfile
pnpm check
pnpm test
pnpm build
pnpm verify:package
```

Integration tests use Docker by default. The restart and Sentinel fault tests
use local `redis-server` processes and are enabled with:

```sh
EFFECTMQ_TEST_REDIS=local EFFECTMQ_TEST_SENTINEL=local pnpm test
```

Edit `src/lua/taskEngine.lua`, then run `pnpm gen:lua`; never hand-edit the
generated TypeScript module. CI rejects generated drift. Add committed golden
fixtures for any declared storage compatibility pair and property/fault tests
for state-machine changes.

Document user-visible behavior in the appropriate guide and update the support
matrix for dependency/platform changes. Add a Changeset for publishable
changes. Keep commits focused; do not include local databases, benchmark
scratch files, test output, editor metadata, secrets, certificates, or packed
tarballs.
