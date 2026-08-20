import type { ReactNode } from "react";

function K({ children }: { children: ReactNode }) {
  return <span className="tk-k">{children}</span>;
}

function T({ children }: { children: ReactNode }) {
  return <span className="tk-t">{children}</span>;
}

function S({ children }: { children: ReactNode }) {
  return <span className="tk-s">{children}</span>;
}

function C({ children }: { children: ReactNode }) {
  return <span className="tk-c">{children}</span>;
}

function N({ children }: { children: ReactNode }) {
  return <span className="tk-n">{children}</span>;
}

export const apiTabLabels = [
  "01 define",
  "02 queue",
  "03 worker",
  "04 concurrency",
  "05 schedule",
];

export const apiSnippets: ReadonlyArray<ReactNode> = [
  // 01 define
  <pre key="define">
    <K>import</K> {"{ Cron, Effect, Schedule, Schema }"} <K>from</K>{" "}
    <S>"effect"</S>
    {";\n"}
    <K>import</K> {"{ NodeRuntime }"} <K>from</K> <S>"@effect/platform-node"</S>
    {";\n"}
    <K>import</K>{" "}
    {"{ Scheduler, Task, TaskEngine, TaskQueue, type TaskHandler, Worker }"}{" "}
    <K>from</K> <S>"@effectmq/core"</S>
    {";\n\n"}
    <K>class</K> <T>EmailRejected</T> <K>extends</K> <T>Schema</T>
    {".TaggedError<"}
    <T>EmailRejected</T>
    {">()(\n  "}
    <S>"EmailRejected"</S>
    {",\n  { reason: "}
    <T>Schema</T>
    {".String },\n) {}\n\n"}
    <K>const</K> SendEmail = <T>Task</T>
    {".make({\n  name: "}
    <S>"send-email"</S>
    {",\n  payload: { to: "}
    <T>Schema</T>
    {".String, subject: "}
    <T>Schema</T>
    {".String },\n  success: "}
    <T>Schema</T>
    {".String,\n  error: "}
    <T>EmailRejected</T>
    {",                        "}
    <C>{"// typed, pattern-matchable failure\n"}</C>
    {"  idempotencyKey: (p) => "}
    {/* biome-ignore lint/suspicious/noTemplateCurlyInString: displayed as literal code */}
    <S>{"`email:${p.to}:${p.subject}`"}</S>
    {",\n  retry: "}
    <T>Schedule</T>
    {".exponential("}
    <S>"1 second"</S>
    {"),   "}
    <C>{"// retries are just Schedules\n"}</C>
    {"});"}
  </pre>,
  // 02 queue
  <pre key="queue">
    <C>{"// A queue binds a name to a task definition.\n"}</C>
    <K>const</K> emails = <T>TaskQueue</T>
    {".make("}
    <S>"emails"</S>
    {", SendEmail);\n\n"}
    <C>{"// Enqueue a payload and keep its durable handle.\n"}</C>
    <K>const</K> offered = <K>yield</K>
    {"* "}
    <T>TaskQueue</T>
    {".offer(emails, {\n  to: "}
    <S>"ada@example.com"</S>
    {",\n  subject: "}
    <S>"Welcome"</S>
    {",\n});\n\n"}
    <C>{"// Offer + await the outcome in one call.\n"}</C>
    <K>const</K> result = <K>yield</K>
    {"* "}
    <T>TaskQueue</T>
    {".execute(emails, {\n  to: "}
    <S>"grace@example.com"</S>
    {",\n  subject: "}
    <S>"Welcome"</S>
    {",\n}); "}
    <C>{"// succeeds with your value — or fails with EmailRejected\n"}</C>
    {"\n"}
    <C>{"// Or await a task you already offered.\n"}</C>
    <K>const</K> outcome = <K>yield</K>
    {"* "}
    <T>TaskQueue</T>
    {".wait(emails, offered.handle);"}
  </pre>,
  // 03 worker
  <pre key="worker">
    <C>{"// Define the handler once, against the task schema.\n"}</C>
    <K>const</K> handleSendEmail: <T>TaskHandler</T>
    {
      "<\n  typeof SendEmail.payloadSchema,\n  typeof SendEmail.successSchema,\n  typeof SendEmail.errorSchema\n> = (task) =>\n  "
    }
    <T>Effect</T>
    {".succeed("}
    {/* biome-ignore lint/suspicious/noTemplateCurlyInString: displayed as literal code */}
    <S>{"`provider:${task.payload.to}`"}</S>
    {");\n\n"}
    <C>
      {"// A managed worker polls, supervises leases and drains cleanly.\n"}
    </C>
    <K>const</K> worker = <T>Worker</T>
    {".make(emails, handleSendEmail, { concurrency: "}
    <N>5</N>
    {" });\n"}
    <K>const</K> program = <T>Worker</T>
    {".run(worker);\n\n"}
    <C>{"// Provide the complete Redis-backed runtime once.\n"}</C>
    <K>const</K> AppLayer = <T>TaskEngine</T>
    {".layer({\n  redis: { url: "}
    <S>"redis://localhost:6379"</S>
    {" },\n});\n\nprogram.pipe("}
    <T>Effect</T>
    {".provide(AppLayer), "}
    <T>NodeRuntime</T>
    {".runMain);"}
  </pre>,
  // 04 concurrency
  <pre key="concurrency">
    <C>{"// Local concurrency is a Worker option, not a fiber recipe.\n"}</C>
    <K>const</K> worker = <T>Worker</T>
    {".make(emails, handleSendEmail, {\n  concurrency: "}
    <N>5</N>
    {",\n  pollInterval: "}
    <S>"250 millis"</S>
    {",\n  drainTimeout: "}
    <S>"30 seconds"</S>
    {",\n});\n\n"}
    <C>
      {
        "// The limit is per process. Add shared coordination for a global cap.\n"
      }
    </C>
    <K>const</K> program = <T>Worker</T>
    {".run(worker);"}
  </pre>,
  // 05 schedule
  <pre key="schedule">
    <C>{"// A durable task, materialized for every cron tick.\n"}</C>
    <K>const</K> schedule = <T>Scheduler</T>
    {".make({\n  name: "}
    <S>"nightly-report"</S>
    {",\n  cron: "}
    <T>Cron</T>
    {".parseUnsafe("}
    <S>"0 2 * * *"</S>
    {", "}
    <S>"UTC"</S>
    {
      "),\n  queue: reportQueue,\n  payload: (tick) => ({ scheduledAt: tick.scheduledAt.toISOString() }),\n  missed: { _tag: "
    }
    <S>"coalesce"</S>
    {" },      "}
    <C>{"// skip | coalesce | bounded backfill\n"}</C>
    {"});\n\n"}
    <K>const</K> worker = <T>Worker</T>
    {".make(reportQueue, ({ payload }) =>\n  "}
    <T>Effect</T>
    {".log("}
    {/* biome-ignore lint/suspicious/noTemplateCurlyInString: displayed as literal code */}
    <S>{"`Building report for ${payload.scheduledAt}`"}</S>
    {"),\n);"}
  </pre>,
];

export const semTabLabels = ["worker pool", "local pacing", "fan-out"];

export const semSnippets: ReadonlyArray<ReactNode> = [
  // bounded
  <pre key="bounded">
    <C>{"// Five acquire/process loops in this worker process.\n"}</C>
    <K>const</K> worker = <T>Worker</T>
    {".make(emails, handle, { concurrency: "}
    <N>5</N>
    {" });\n\n"}
    <K>yield</K>
    {"* "}
    <T>Worker</T>
    {".run(worker);"}
  </pre>,
  // rate limit
  <pre key="rate-limit">
    <C>
      {"// Pace one local loop. Use shared coordination for a global limit.\n"}
    </C>
    <K>const</K> runOne = <T>TaskQueue</T>
    {".complete(emails, handle);\n\n"}
    <K>yield</K>
    {"* runOne.pipe(\n  "}
    <T>Effect</T>
    {".repeat("}
    <T>Schedule</T>
    {".spaced("}
    <S>"100 millis"</S>
    {")), "}
    <C>{"// ≤ 10 tasks/s"}</C>
    {"\n);"}
  </pre>,
  // fan-out
  <pre key="fan-out">
    <C>
      {"// Run the same worker program in more OS processes or containers.\n"}
    </C>
    <K>const</K> worker = <T>Worker</T>
    {".make(emails, handle, { concurrency: "}
    <N>5</N>
    {" });\n\n"}
    <K>const</K> program = <T>Worker</T>
    {".run(worker).pipe(\n  "}
    <T>Effect</T>
    {".provide(AppLayer),\n);\n\n"}
    <T>NodeRuntime</T>
    {".runMain(program); "}
    <C>{"// deploy N replicas"}</C>
  </pre>,
];
