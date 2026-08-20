"use client";

import { useEffect, useRef, useState } from "react";

const GROUPS: ReadonlyArray<readonly [string, ReadonlyArray<number>]> = [
  ["64 B", [592.3, 2179.9, 2922.4]],
  ["1 KiB", [640.8, 2346.9, 2968.0]],
  ["16 KiB", [483.4, 1623.9, 2035.6]],
];

export function BenchChart() {
  const [chartsIn, setChartsIn] = useState(false);
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = panelRef.current;
    if (!el) return;
    const io = new IntersectionObserver(
      (entries) => {
        if (entries.some((e) => e.isIntersecting)) {
          setChartsIn(true);
          io.disconnect();
        }
      },
      { threshold: 0.25 },
    );
    io.observe(el);
    return () => io.disconnect();
  }, []);

  return (
    <div ref={panelRef} className="lp-panel lp-chart-panel">
      <p className="lp-chart-title">
        Completed lifecycles per second — by payload × concurrency
      </p>
      <div className="lp-chart-groups">
        {GROUPS.map(([label, values]) => (
          <div key={label}>
            <div className="lp-bars">
              {values.map((v, i) => (
                <div key={`c${i}`} className="lp-bar-col">
                  <p className="lp-bar-value">
                    {Math.round(v).toLocaleString("en-US")}
                  </p>
                  <div
                    className="lp-bar"
                    style={{
                      height: chartsIn
                        ? `${((v / 3000) * 100).toFixed(1)}%`
                        : "2%",
                      transitionDelay: `${i * 130}ms`,
                    }}
                  />
                </div>
              ))}
            </div>
            <div className="lp-bar-labels">
              <p>c=1</p>
              <p>c=8</p>
              <p>c=32</p>
            </div>
            <p className="lp-group-label">{label} payload</p>
          </div>
        ))}
      </div>
    </div>
  );
}
