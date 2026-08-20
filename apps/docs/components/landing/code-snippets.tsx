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
    <K>import</K> {"{ Schedule, Schema }"} <K>from</K> <S>"effect"</S>
    {";\n"}
    <K>import</K> {"{ Task, TaskQueue }"} <K>from</K> <S>"@effectmq/core"</S>
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
    <C>{"// Enqueue a payload — idempotent, durable.\n"}</C>
    <K>yield</K>
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
    <K>const</K> task = <K>yield</K>
    {"* "}
    <T>TaskQueue</T>
    {".offer(emails, payload);\n"}
    <K>const</K> outcome = <K>yield</K>
    {"* "}
    <T>TaskQueue</T>
    {".wait(emails, task.handle);"}
  </pre>,
  // 03 worker
  <pre key="worker">
    <C>{"// Take one task, run it, report the outcome.\n"}</C>
    <K>yield</K>
    {"* "}
    <T>TaskQueue</T>
    {".complete(emails, (task) =>\n  "}
    <T>Effect</T>
    {".succeed("}
    {/* biome-ignore lint/suspicious/noTemplateCurlyInString: displayed as literal code */}
    <S>{"`provider:${task.payload.to}`"}</S>
    {"),\n);\n\n"}
    <C>{"// Or name the handler once, and loop forever.\n"}</C>
    <K>const</K>
    {" sendEmailWorker = "}
    <T>TaskQueue</T>
    {".complete(emails, handleSendEmail);\n"}
    <K>yield</K>
    {"* sendEmailWorker.pipe("}
    <T>Effect</T>
    {".repeat("}
    <T>Schedule</T>
    {".forever));\n\n"}
    <C>{"// The engine + its Redis layer: the only wiring you need.\n"}</C>
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
    <C>{"// No bespoke concurrency options. Just Effect.\n"}</C>
    <K>const</K> worker = <T>Effect</T>
    {".gen("}
    <K>function</K>
    {"* () {\n  "}
    <C>{"// At most 5 tasks in flight at any moment.\n"}</C>
    {"  "}
    <K>const</K> semaphore = <K>yield</K>
    {"* "}
    <T>Semaphore</T>
    {".make("}
    <N>5</N>
    {");\n\n  "}
    <K>yield</K>
    {"* "}
    <T>Semaphore</T>
    {".withPermit(\n    semaphore,\n    "}
    <T>TaskQueue</T>
    {".complete(emails, handleSendEmail),\n  ).pipe(\n    "}
    <T>Effect</T>
    {".forkScoped,               "}
    <C>{"// each worker is its own fiber\n"}</C>
    {"    "}
    <T>Effect</T>
    {".repeat("}
    <T>Schedule</T>
    {".forever), "}
    <C>{"// ...that keeps pulling work\n"}</C>
    {"  );\n});"}
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

export const semTabLabels = ["bounded", "rate limit", "fan-out"];

export const semSnippets: ReadonlyArray<ReactNode> = [
  // bounded
  <pre key="bounded">
    <C>{"// At most 5 tasks in flight, across any number of runners.\n"}</C>
    <K>const</K> semaphore = <K>yield</K>
    {"* "}
    <T>Semaphore</T>
    {".make("}
    <N>5</N>
    {");\n\n"}
    <K>yield</K>
    {"* "}
    <T>Semaphore</T>
    {".withPermit(\n  semaphore,\n  "}
    <T>TaskQueue</T>
    {".complete(emails, handle),\n).pipe("}
    <T>Effect</T>
    {".forkScoped, "}
    <T>Effect</T>
    {".repeat("}
    <T>Schedule</T>
    {".forever));"}
  </pre>,
  // rate limit
  <pre key="rate-limit">
    <C>{"// A rate limit is just a Semaphore + a Schedule.\n"}</C>
    <K>const</K> permits = <K>yield</K>
    {"* "}
    <T>Semaphore</T>
    {".make("}
    <N>1</N>
    {");\n\n"}
    <K>yield</K>
    {"* "}
    <T>Semaphore</T>
    {".withPermit(\n  permits,\n  "}
    <T>TaskQueue</T>
    {".complete(emails, handle),\n).pipe(\n  "}
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
    <C>{"// Fan out: more fibers — or more processes. Same worker.\n"}</C>
    <K>const</K> worker = <T>TaskQueue</T>
    {".complete(emails, handle).pipe(\n  "}
    <T>Effect</T>
    {".repeat("}
    <T>Schedule</T>
    {".forever),\n);\n\n"}
    <K>yield</K>
    {"* "}
    <T>Effect</T>
    {".all(\n  "}
    <T>Array</T>
    {".from({ length: "}
    <N>6</N>
    {" }, () => worker),\n  { concurrency: "}
    <S>"unbounded"</S>
    {" },\n);"}
  </pre>,
];
