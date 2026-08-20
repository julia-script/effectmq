"use client";

import { useState } from "react";
import { semSnippets, semTabLabels } from "./code-snippets";
import { SemCanvas } from "./sem-canvas";

export function ConcurrencyTabs() {
  const [tab, setTab] = useState(0);

  return (
    <>
      <div>
        <p className="lp-eyebrow">03 — Concurrency</p>
        <h2
          className="lp-h2"
          style={{
            font: "700 clamp(30px,3.4vw,42px)/1.1 'Space Grotesk', sans-serif",
          }}
        >
          Bring your own concurrency.
        </h2>
        <p className="lp-conc-body">
          No builtin concurrency knobs, rate limiting or backpressure — it
          doesn't need them. <span className="lp-inline-code">complete</span>{" "}
          does exactly one task; Semaphore, Schedule and fibers decide how many
          run at once. None of it is our invention. All of it composes.
        </p>
        <div className="lp-tabs lp-conc-tabs">
          {semTabLabels.map((label, i) => (
            <button
              key={label}
              type="button"
              className="lp-tab lp-tab-sm"
              data-active={tab === i}
              onClick={() => setTab(i)}
            >
              {label}
            </button>
          ))}
        </div>
        <div className="lp-panel lp-code-panel lp-code-sm">
          {semSnippets[tab]}
        </div>
      </div>
      <div className="lp-panel lp-sem-panel">
        <SemCanvas mode={tab} />
      </div>
    </>
  );
}
