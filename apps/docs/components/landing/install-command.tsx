"use client";

import { useEffect, useRef, useState } from "react";

const FULL_COMMAND =
  "npm install @effectmq/core effect@4.0.0-beta.107 @effect/platform-node@4.0.0-beta.107";

export function InstallCommand() {
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(() => () => clearTimeout(timer.current), []);

  return (
    <div className="lp-install">
      <span className="lp-prompt">$</span>
      <span className="lp-pkg">
        npm install <span className="lp-accent">@effectmq/core</span>
      </span>
      <button
        type="button"
        className="lp-copy-btn"
        onClick={() => {
          navigator.clipboard.writeText(FULL_COMMAND);
          setCopied(true);
          clearTimeout(timer.current);
          timer.current = setTimeout(() => setCopied(false), 1600);
        }}
      >
        {copied ? "copied!" : "copy"}
      </button>
    </div>
  );
}
