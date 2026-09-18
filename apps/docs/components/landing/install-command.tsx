"use client";

import { useEffect, useRef, useState } from "react";

const FULL_COMMAND =
  "pnpm add @effectmq/core@0.3.0-rc.1 effect@4.0.0-rc.115 @effect/platform-node@4.0.0-rc.115";

export function InstallCommand() {
  const [status, setStatus] = useState<"idle" | "copied" | "error">("idle");
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  useEffect(() => () => clearTimeout(timer.current), []);

  return (
    <div className="lp-install">
      <span className="lp-prompt">$</span>
      <span className="lp-pkg">
        pnpm add <span className="lp-accent">@effectmq/core@0.3.0-rc.1</span>
      </span>
      <button
        type="button"
        className="lp-copy-btn"
        aria-live="polite"
        onClick={async () => {
          clearTimeout(timer.current);
          try {
            await navigator.clipboard.writeText(FULL_COMMAND);
            setStatus("copied");
          } catch {
            setStatus("error");
          }
          timer.current = setTimeout(() => setStatus("idle"), 2000);
        }}
      >
        {status === "copied"
          ? "copied"
          : status === "error"
            ? "copy failed"
            : "copy with peers"}
      </button>
    </div>
  );
}
