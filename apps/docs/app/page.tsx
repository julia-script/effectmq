import type { Metadata } from "next";
import Link from "next/link";
import { ApiTabs } from "@/components/landing/api-tabs";
import { BenchChart } from "@/components/landing/bench-chart";
import { ConcurrencyTabs } from "@/components/landing/concurrency-tabs";
import { InstallCommand } from "@/components/landing/install-command";
import { LifecycleCanvas } from "@/components/landing/lifecycle-canvas";
import "./landing.css";

export const metadata: Metadata = {
  title: "effectmq — typed task queue built on Effect",
  description:
    "A task queue built on Effect. Describe work as a schema, process it with a handler that is just an Effect. Retries, delays, idempotency, cron schedules: handled. Redis keeps unfinished work recoverable.",
};

const GITHUB_URL = "https://github.com/julia-script/effectmq";

const FEATURES = [
  {
    title: "Retries are Schedules",
    body: "Declare retry on the task as an Effect Schedule — exponential, jittered, whatever composes. Exhausted schedule → your failure policy.",
    code: 'retry: Schedule.exponential("1 second")',
  },
  {
    title: "Idempotency is the id",
    body: "The idempotency key is the task id. Offer the same key twice and you get one task, not two — replacement is an explicit new generation.",
    // biome-ignore lint/suspicious/noTemplateCurlyInString: displayed as literal code
    code: "idempotencyKey: (p) => `email:${p.to}`",
  },
  {
    title: "Durable cron",
    body: "Scheduler.make materializes a real queue task per tick — competing schedulers and crash recovery can safely re-offer it.",
    code: 'missed: { _tag: "coalesce" }',
  },
  {
    title: "Typed lifecycle events",
    body: "task.completed carries your success type; task.failed carries your typed error. Streams, wait and execute all decode against your schemas.",
    code: "TaskQueue.stream(emails)",
  },
  {
    title: "At-least-once, fenced",
    body: "Every attempt is fenced with a unique lease token; stalled workers are separated from failed handlers. Unfinished work stays recoverable.",
    code: "lease: unique token per attempt",
  },
  {
    title: "Swappable engine",
    body: "The Redis engine is a Layer built on atomic Lua scripts. Provide it and forget it — or swap it for a different implementation, like anything in Effect.",
    code: "TaskEngine.layer({ redis })",
  },
];

export default function HomePage() {
  return (
    <div className="lp">
      <nav className="lp-container lp-nav">
        <span className="lp-wordmark">
          effect
          <span className="lp-accent">mq</span>
          <span className="lp-cursor">▮</span>
        </span>
        <div className="lp-nav-links">
          <Link href="/docs" className="lp-nav-docs">
            Docs
          </Link>
          <a href={GITHUB_URL} className="lp-nav-github">
            GitHub ↗
          </a>
        </div>
      </nav>

      <section className="lp-hero">
        <div className="lp-hero-grid" />
        <div className="lp-hero-glow" />
        <div className="lp-container lp-hero-inner">
          <div className="lp-chips">
            <span className="lp-chip lp-chip-accent">BUILT ON EFFECT 4</span>
            <span className="lp-chip">REDIS-BACKED</span>
            <span className="lp-chip">MIT</span>
          </div>
          <h1>
            Typed payloads<span className="lp-accent">.</span>
            <br />
            Typed results<span className="lp-accent">.</span>
            <br />
            Typed errors<span className="lp-accent">.</span> All the way down.
          </h1>
          <p className="lp-hero-sub">
            A task queue built on <a href="https://effect.website">Effect</a>.
            Describe work as a schema, process it with a handler that is just an
            Effect. Retries, delays, idempotency, cron schedules: handled. Redis
            keeps unfinished work recoverable.
          </p>
          <div className="lp-cta-row">
            <InstallCommand />
            <Link href="/docs" className="lp-ghost-btn">
              Read the docs →
            </Link>
          </div>
        </div>
      </section>

      <section
        className="lp-container"
        style={{ paddingTop: 96, paddingBottom: 84 }}
      >
        <p className="lp-eyebrow">01 — Lifecycle</p>
        <h2 className="lp-h2">How a task lives.</h2>
        <p className="lp-lede">
          The state machine every task moves through: offered, queued, leased
          under a unique fence token, and acknowledged — or routed back through
          its retry Schedule. Unfinished work survives worker loss. (An
          illustration of the engine's internals, not a product dashboard.)
        </p>
        <div className="lp-panel lp-canvas-panel">
          <LifecycleCanvas />
        </div>
        <div className="lp-legend">
          <span>
            <span className="lp-dot" style={{ background: "#8A8A96" }} />
            waiting / scheduled
          </span>
          <span>
            <span className="lp-dot" style={{ background: "#EDEDF2" }} />
            active (fenced attempt)
          </span>
          <span>
            <span className="lp-dot" style={{ background: "#FF8A5C" }} />
            retry / failed
          </span>
          <span>
            <span className="lp-dot" style={{ background: "var(--accent)" }} />
            success
          </span>
        </div>
      </section>

      <section
        className="lp-container"
        style={{ paddingTop: 12, paddingBottom: 96 }}
      >
        <p className="lp-eyebrow">02 — The API</p>
        <h2 className="lp-h2">The whole loop, in thirty seconds.</h2>
        <p className="lp-lede" style={{ marginBottom: 30 }}>
          A task is a schema, not a function. Your handler receives a fully
          decoded payload — the real object, not a JSON string — and its
          failures are pattern-matchable typed errors.
        </p>
        <ApiTabs />
      </section>

      <section className="lp-band">
        <div className="lp-container lp-conc-grid">
          <ConcurrencyTabs />
        </div>
      </section>

      <section
        className="lp-container"
        style={{ paddingTop: 96, paddingBottom: 96 }}
      >
        <p className="lp-eyebrow">04 — Receipts</p>
        <h2 className="lp-h2">Numbers, not vibes.</h2>
        <p className="lp-lede" style={{ marginBottom: 40, maxWidth: 640 }}>
          Every release ships reproducible performance evidence: full end-to-end
          task lifecycles — atomic create, fenced acquire, acknowledge —
          measured per payload size and concurrency.
        </p>
        <div className="lp-stats">
          <div className="lp-stat-card">
            <p className="lp-stat-value">
              2,968<span className="lp-stat-unit"> tasks/s</span>
            </p>
            <p className="lp-stat-caption">
              peak lifecycle throughput · 1 KiB @ c=32
            </p>
          </div>
          <div className="lp-stat-card">
            <p className="lp-stat-value">
              1.44<span className="lp-stat-unit"> ms p50</span>
            </p>
            <p className="lp-stat-caption">
              single-task lifecycle latency · 1 KiB @ c=1
            </p>
          </div>
          <div className="lp-stat-card">
            <p className="lp-stat-value">
              39,097<span className="lp-stat-unit"> items/s</span>
            </p>
            <p className="lp-stat-caption">
              due-backlog sweep, atomic Lua batches
            </p>
          </div>
        </div>
        <BenchChart />
        <p className="lp-footnote">
          baseline: Node 22 · Redis 8.0.6 (loopback) · Effect 4.0.0-beta.107 ·
          macOS arm64 — committed as{" "}
          <a href={`${GITHUB_URL}/blob/main/docs/performance.md`}>
            regression evidence
          </a>
          , not a capacity claim.
        </p>
      </section>

      <section className="lp-band">
        <div
          className="lp-container"
          style={{ paddingTop: 96, paddingBottom: 96 }}
        >
          <p className="lp-eyebrow">05 — Batteries</p>
          <h2 className="lp-h2" style={{ marginBottom: 44 }}>
            Handled, so you don't.
          </h2>
          <div className="lp-cards">
            {FEATURES.map((f) => (
              <div key={f.title} className="lp-card">
                <h3>{f.title}</h3>
                <p>{f.body}</p>
                <code>{f.code}</code>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="lp-cta">
        <div className="lp-cta-glow" />
        <div className="lp-container lp-cta-inner">
          <h2>
            Put work in. Take attempts.
            <br />
            Redis keeps the rest recoverable<span className="lp-accent">.</span>
          </h2>
          <InstallCommand />
          <a href={GITHUB_URL} className="lp-star-link">
            Star on GitHub ↗
          </a>
        </div>
      </section>

      <footer className="lp-footer">
        <div className="lp-container lp-footer-row">
          <span className="lp-footer-mark">
            effect<span className="lp-accent">mq</span>
          </span>
          <div className="lp-footer-links">
            <Link href="/docs" className="lp-footer-link">
              Docs
            </Link>
            <a href={GITHUB_URL} className="lp-footer-link">
              GitHub
            </a>
            <a
              href="https://www.npmjs.com/package/@effectmq/core"
              className="lp-footer-link"
            >
              npm
            </a>
          </div>
        </div>
        <div className="lp-container lp-footer-fine">
          <p>
            MIT · built on <a href="https://effect.website">Effect</a> ·
            requires the Effect 4 beta · © 2026
          </p>
        </div>
      </footer>
    </div>
  );
}
