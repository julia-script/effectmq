"use client";

import { useEffect, useRef } from "react";

interface Dot {
  node: string;
  edge: Edge | null;
  t: number;
  hold: number;
  attempt: number;
  alpha: number;
  delayed: boolean;
  x?: number;
  y?: number;
}

interface Edge {
  a: string;
  b: string;
  bow: number;
  label: string;
  warn?: boolean;
}

const accentColor = () =>
  getComputedStyle(document.documentElement)
    .getPropertyValue("--accent")
    .trim() || "#C6F94F";

/**
 * Constellation state-machine animation: tasks travel the real
 * storage-protocol lists (offer → scheduled/queue → worker → outcome).
 */
export function LifecycleCanvas() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const cv = canvasRef.current;
    if (!cv) return;
    const ctx = cv.getContext("2d");
    if (!ctx) return;

    const WORKERS = ["worker1", "worker2", "worker3"];
    const NODES: Record<
      string,
      { x: number; y: number; label: string; flash: number }
    > = {
      offer: { x: 0.06, y: 0.5, label: "offer()", flash: 0 },
      scheduled: { x: 0.36, y: 0.12, label: "scheduled", flash: 0 },
      waiting: { x: 0.34, y: 0.64, label: "queue", flash: 0 },
      worker1: { x: 0.62, y: 0.24, label: "worker · 1", flash: 0 },
      worker2: { x: 0.66, y: 0.52, label: "worker · 2", flash: 0 },
      worker3: { x: 0.6, y: 0.8, label: "worker · 3", flash: 0 },
      success: { x: 0.9, y: 0.3, label: "success", flash: 0 },
      failed: { x: 0.9, y: 0.74, label: "failed", flash: 0 },
    };
    const EDGES: Array<Edge> = [
      { a: "offer", b: "waiting", bow: 0, label: "offer" },
      { a: "offer", b: "scheduled", bow: 0, label: "delay" },
      { a: "scheduled", b: "waiting", bow: 0, label: "due" },
      { a: "waiting", b: "worker1", bow: 0, label: "lease" },
      { a: "waiting", b: "worker2", bow: 0, label: "" },
      { a: "waiting", b: "worker3", bow: 0, label: "" },
      { a: "worker1", b: "success", bow: 0, label: "" },
      { a: "worker2", b: "success", bow: 0, label: "succeed" },
      { a: "worker3", b: "success", bow: 0, label: "" },
      { a: "worker1", b: "failed", bow: 0, label: "" },
      { a: "worker2", b: "failed", bow: 0, label: "exhausted" },
      { a: "worker3", b: "failed", bow: 0, label: "" },
      { a: "worker1", b: "scheduled", bow: 0, label: "retry", warn: true },
      { a: "worker2", b: "scheduled", bow: 0, label: "", warn: true },
      { a: "worker3", b: "scheduled", bow: 0, label: "", warn: true },
    ];
    const edgeOf = (a: string, b: string) =>
      EDGES.find((e) => e.a === a && e.b === b) ?? null;
    const dots: Array<Dot> = [];
    const waitQ: Array<Dot> = [];
    const stars = Array.from({ length: 80 }, () => ({
      x: Math.random(),
      y: Math.random(),
      r: Math.random() * 1.1 + 0.3,
      ph: Math.random() * 6.28,
      tw: 0.0006 + Math.random() * 0.0012,
    }));
    let last = performance.now();
    let acc = 900;
    let nSuccess = 0;
    let nFailed = 0;
    let nRetries = 0;
    let raf = 0;

    const step = (now: number) => {
      raf = requestAnimationFrame(step);
      const dt = Math.min(64, now - last);
      last = now;
      const W = cv.clientWidth;
      const H = cv.clientHeight;
      if (!W) return;
      const dpr = window.devicePixelRatio || 1;
      if (cv.width !== Math.round(W * dpr)) {
        cv.width = Math.round(W * dpr);
        cv.height = Math.round(H * dpr);
      }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, W, H);
      const AC = accentColor();
      const P = (n: string): [number, number] => [
        NODES[n].x * W,
        NODES[n].y * H,
      ];
      const bez = (e: Edge, t: number): [number, number] => {
        const [x0, y0] = P(e.a);
        const [x1, y1] = P(e.b);
        const dx = x1 - x0;
        const dy = y1 - y0;
        const len = Math.hypot(dx, dy) || 1;
        const cx = (x0 + x1) / 2 + (-dy / len) * e.bow * W;
        const cyy = (y0 + y1) / 2 + (dx / len) * e.bow * W;
        const v = 1 - t;
        return [
          v * v * x0 + 2 * v * t * cx + t * t * x1,
          v * v * y0 + 2 * v * t * cyy + t * t * y1,
        ];
      };
      ctx.font = "10px 'JetBrains Mono', monospace";
      ctx.textAlign = "center";
      ctx.lineWidth = 1;
      // background starfield (twinkling)
      for (const s of stars) {
        ctx.globalAlpha =
          0.14 + 0.16 * (0.5 + 0.5 * Math.sin(now * s.tw + s.ph));
        ctx.fillStyle = "#EDEDF2";
        ctx.beginPath();
        ctx.arc(s.x * W, s.y * H, s.r, 0, Math.PI * 2);
        ctx.fill();
      }
      ctx.globalAlpha = 1;
      // constellation lines: straight, dashed, stopping short of each star
      for (const e of EDGES) {
        const col = e.warn ? "rgba(255,138,92,0.32)" : "rgba(237,237,242,0.18)";
        ctx.strokeStyle = col;
        ctx.setLineDash([4, 5]);
        ctx.beginPath();
        const [sx, sy] = bez(e, 0.06);
        ctx.moveTo(sx, sy);
        const [ex, ey] = bez(e, 0.94);
        ctx.lineTo(ex, ey);
        ctx.stroke();
        ctx.setLineDash([]);
        // small directional tick at 0.85
        const [ax, ay] = bez(e, 0.82);
        const [bx2, by2] = bez(e, 0.87);
        const ang = Math.atan2(by2 - ay, bx2 - ax);
        ctx.fillStyle = col;
        ctx.beginPath();
        ctx.moveTo(bx2, by2);
        ctx.lineTo(
          bx2 - 6 * Math.cos(ang - 0.5),
          by2 - 6 * Math.sin(ang - 0.5),
        );
        ctx.lineTo(
          bx2 - 6 * Math.cos(ang + 0.5),
          by2 - 6 * Math.sin(ang + 0.5),
        );
        ctx.fill();
        // label
        const [lx, ly] = bez(e, 0.5);
        ctx.fillStyle = e.warn
          ? "rgba(255,138,92,0.75)"
          : "rgba(139,139,152,0.7)";
        ctx.fillText(e.label, lx, ly - 7);
      }
      // spawn
      acc += dt;
      if (acc > 900 && dots.length < 24) {
        acc = 0;
        NODES.offer.flash = 200;
        dots.push({
          node: "offer",
          edge: null,
          t: 0,
          hold: 220,
          attempt: 0,
          alpha: 1,
          delayed: Math.random() < 0.2,
        });
      }
      // waiting -> workers dispatch (one fenced attempt per worker)
      for (const w of WORKERS) {
        if (!waitQ.length) break;
        const busy = dots.some(
          (d) => (d.node === w && !d.edge) || (d.edge && d.edge.b === w),
        );
        if (!busy) {
          const d = waitQ.shift();
          if (d) {
            d.edge = edgeOf("waiting", w);
            d.t = 0;
          }
        }
      }
      // step dots
      for (let i = dots.length - 1; i >= 0; i--) {
        const d = dots[i];
        if (d.edge) {
          d.t += dt / (d.edge.warn ? 950 : 780);
          const [x, y] = bez(d.edge, Math.min(1, d.t));
          d.x = x;
          d.y = y;
          if (d.t >= 1) {
            d.node = d.edge.b;
            d.edge = null;
            NODES[d.node].flash = 220;
            if (d.node === "waiting") {
              waitQ.push(d);
              d.hold = -1;
            } else if (d.node === "scheduled")
              d.hold = 900 + Math.random() * 900;
            else if (d.node.indexOf("worker") === 0)
              d.hold = 650 + Math.random() * 950;
            else {
              d.hold = 300;
              if (d.node === "success") nSuccess++;
              else nFailed++;
            }
          }
        } else if (d.hold >= 0) {
          d.hold -= dt;
          if (d.hold < 0) {
            if (d.node === "offer") {
              d.edge = edgeOf("offer", d.delayed ? "scheduled" : "waiting");
              d.t = 0;
            } else if (d.node === "scheduled") {
              d.edge = edgeOf("scheduled", "waiting");
              d.t = 0;
            } else if (d.node.indexOf("worker") === 0) {
              const r = Math.random();
              if (d.attempt === 0 && r < 0.3) {
                d.attempt = 1;
                d.edge = edgeOf(d.node, "scheduled");
                nRetries++;
              } else if (r < (d.attempt === 0 ? 0.36 : 0.15))
                d.edge = edgeOf(d.node, "failed");
              else d.edge = edgeOf(d.node, "success");
              d.t = 0;
            } else if (d.node === "success" || d.node === "failed") {
              d.alpha -= dt * 0.002;
              d.hold = 0;
              if (d.alpha <= 0) {
                dots.splice(i, 1);
              }
            }
          }
        }
      }
      // resting dots orbit their star like satellites
      const rest: Record<string, number> = {};
      for (const d of dots) {
        if (d.edge) continue;
        const n = d.node;
        rest[n] = rest[n] || 0;
        const idx = rest[n]++;
        const [nx, ny] = P(n);
        const ring = 17 + 7 * Math.floor(idx / 7);
        const ang = idx * 2.4 + now * 0.00035;
        d.x = nx + ring * Math.cos(ang);
        d.y = ny + ring * Math.sin(ang);
      }
      // stars (nodes)
      for (const k in NODES) {
        const n = NODES[k];
        n.flash = Math.max(0, n.flash - dt);
        const [x, y] = P(k);
        const isWorker = k.indexOf("worker") === 0;
        const base =
          k === "success"
            ? AC
            : k === "failed"
              ? "#FF8A5C"
              : k === "scheduled"
                ? "#B8A7FF"
                : "#EDEDF2";
        const col = n.flash > 0 ? AC : base;
        // sparkle cross
        ctx.strokeStyle = col;
        ctx.globalAlpha = 0.35;
        ctx.beginPath();
        ctx.moveTo(x - 9, y);
        ctx.lineTo(x + 9, y);
        ctx.moveTo(x, y - 9);
        ctx.lineTo(x, y + 9);
        ctx.stroke();
        ctx.globalAlpha = 1;
        // core
        ctx.shadowColor = col;
        ctx.shadowBlur = 14;
        ctx.fillStyle = col;
        ctx.beginPath();
        ctx.arc(x, y, isWorker ? 4.5 : 3.5, 0, Math.PI * 2);
        ctx.fill();
        ctx.shadowBlur = 0;
        // arrival pulse ring
        if (n.flash > 0) {
          const u = 1 - n.flash / 220;
          ctx.strokeStyle = AC;
          ctx.globalAlpha = 0.5 * (1 - u);
          ctx.beginPath();
          ctx.arc(x, y, 8 + u * 16, 0, Math.PI * 2);
          ctx.stroke();
          ctx.globalAlpha = 1;
        }
        // label
        ctx.font = "11px 'JetBrains Mono', monospace";
        ctx.fillStyle = n.flash > 0 ? AC : "rgba(185,185,196,0.85)";
        const count =
          k === "success"
            ? ` · ${nSuccess}`
            : k === "failed"
              ? ` · ${nFailed}`
              : "";
        const ly =
          k === "scheduled" || k === "success" || k === "worker1"
            ? y - 22
            : y + 32;
        ctx.fillText(n.label + count, x, ly);
        ctx.font = "10px 'JetBrains Mono', monospace";
      }
      // dots on top
      for (const d of dots) {
        const inActive = d.node.indexOf("worker") === 0 && !d.edge;
        const col =
          d.edge?.warn || (d.attempt > 0 && d.node === "scheduled" && !d.edge)
            ? "#FF8A5C"
            : d.node === "success" || d.edge?.b === "success"
              ? AC
              : d.node === "failed" || d.edge?.b === "failed"
                ? "#FF8A5C"
                : inActive
                  ? "#EDEDF2"
                  : "#8A8A96";
        ctx.globalAlpha = Math.max(0, d.alpha);
        ctx.shadowColor = col;
        ctx.shadowBlur = inActive || d.node === "success" ? 9 : 0;
        ctx.fillStyle = col;
        ctx.beginPath();
        ctx.arc(d.x ?? 0, d.y ?? 0, 4.5, 0, Math.PI * 2);
        ctx.fill();
        ctx.shadowBlur = 0;
        if (inActive) {
          ctx.strokeStyle = "rgba(237,237,242,0.35)";
          ctx.beginPath();
          ctx.arc(d.x ?? 0, d.y ?? 0, 8, 0, Math.PI * 2);
          ctx.stroke();
        }
        ctx.globalAlpha = 1;
      }
      // counters
      ctx.textAlign = "right";
      ctx.fillStyle = "rgba(139,139,152,0.9)";
      ctx.fillText(`retries · ${nRetries}`, W - 16, 22);
      ctx.textAlign = "center";
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, []);

  return <canvas ref={canvasRef} style={{ height: 420 }} />;
}
