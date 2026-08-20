"use client";

import { useState } from "react";
import { semSnippets, semTabLabels } from "./code-snippets";
import { SemCanvas } from "./sem-canvas";

export function ConcurrencyTabs() {
  const [tab, setTab] = useState(0);

  return (
    <>
      <div>
        <h2
          className="lp-h2"
          style={{
            font: "700 clamp(30px,3.4vw,42px)/1.1 'Space Grotesk', sans-serif",
          }}
        >
          Built in locally. Explicit globally.
        </h2>
        <p className="lp-conc-body">
          <span className="lp-inline-code">Worker</span> runs a bounded pool of
          local task slots with lease supervision, maintenance and graceful
          draining. Run more processes to fan out. Cross-process concurrency and
          rate limits need shared coordination; a local semaphore cannot enforce
          them.
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
