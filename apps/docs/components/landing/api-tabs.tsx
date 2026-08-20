"use client";

import { useState } from "react";
import { apiSnippets, apiTabLabels } from "./code-snippets";

export function ApiTabs() {
  const [tab, setTab] = useState(0);

  return (
    <>
      <div className="lp-tabs">
        {apiTabLabels.map((label, i) => (
          <button
            key={label}
            type="button"
            className="lp-tab"
            data-active={tab === i}
            onClick={() => setTab(i)}
          >
            {label}
          </button>
        ))}
      </div>
      <div className="lp-panel lp-code-panel">{apiSnippets[tab]}</div>
    </>
  );
}
