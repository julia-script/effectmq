"use client";

import { useEffect, useRef } from "react";

interface Dot {
  node: string;
  edge: Edge | null;
  t: number;
  hold: number;
  alpha: number;
  x?: number;
  y?: number;
}

interface Edge {
  a: string;
  b: string;
  label: string;
}

const accentColor = () =>
  getComputedStyle(document.documentElement)
    .getPropertyValue("--accent")
    .trim() || "#C6F94F";

/**
 * Worker constellation: producers offer into a queue whose head is pulled
 * through local worker slots. Three modes — a five-slot local pool, a paced
 * local loop, and process-level fan-out.
 */
export function SemCanvas({ mode }: { readonly mode: number }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const modeRef = useRef(mode);
  modeRef.current = mode;

  useEffect(() => {
    const cv = canvasRef.current;
    if (!cv) return;
    const ctx = cv.getContext("2d");
    if (!ctx) return;

    let NODES: Record<
      string,
      { x: number; y: number; label: string; flash: number }
    > = {};
    let EDGES: Array<Edge> = [];
    let PERMITS: Array<string> = [];
    let currentMode = -1;
    let cooldown = 0;
    let rateMs = 0;
    let gateLabel = "";
    let dots: Array<Dot> = [];
    let waitQ: Array<Dot> = [];
    let doneCount = 0;
    const edgeOf = (a: string, b: string) =>
      EDGES.find((e) => e.a === a && e.b === b) ?? null;
    const build = (m: number) => {
      currentMode = m;
      dots = [];
      waitQ = [];
      doneCount = 0;
      cooldown = 0;
      const n = m === 0 ? 5 : m === 1 ? 1 : 6;
      rateMs = m === 1 ? 620 : 0;
      gateLabel =
        m === 0
          ? "Worker concurrency: 5"
          : m === 1
            ? 'Schedule.spaced("100 millis")'
            : "6 worker processes";
      NODES = {
        runnerA: { x: 0.09, y: 0.28, label: "producer A", flash: 0 },
        runnerB: { x: 0.09, y: 0.72, label: "producer B", flash: 0 },
        queue: { x: 0.38, y: 0.5, label: "queue", flash: 0 },
        done: { x: 0.92, y: 0.5, label: "done", flash: 0 },
      };
      PERMITS = [];
      for (let j = 0; j < n; j++) {
        const k = `permit${j}`;
        PERMITS.push(k);
        NODES[k] = {
          x: 0.68,
          y: n === 1 ? 0.5 : 0.14 + (0.72 * j) / (n - 1),
          label: "",
          flash: 0,
        };
      }
      EDGES = [
        { a: "runnerA", b: "queue", label: "offer()" },
        { a: "runnerB", b: "queue", label: "" },
      ];
      const midIdx = Math.floor(n / 2);
      PERMITS.forEach((k, j) => {
        EDGES.push({
          a: "queue",
          b: k,
          label: j === midIdx ? (m === 1 ? "complete()" : "completeOne()") : "",
        });
        EDGES.push({ a: k, b: "done", label: "" });
      });
    };
    build(modeRef.current);
    const stars = Array.from({ length: 70 }, () => ({
      x: Math.random(),
      y: Math.random(),
      r: Math.random() * 1.1 + 0.3,
      ph: Math.random() * 6.28,
      tw: 0.0006 + Math.random() * 0.0012,
    }));
    const runners = [
      { node: "runnerA", acc: 0, next: 700 },
      { node: "runnerB", acc: 420, next: 1100 },
    ];
    let last = performance.now();
    let raf = 0;

    const step = (now: number) => {
      raf = requestAnimationFrame(step);
      if (modeRef.current !== currentMode) build(modeRef.current);
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
      const lin = (e: Edge, t: number): [number, number] => {
        const [x0, y0] = P(e.a);
        const [x1, y1] = P(e.b);
        return [x0 + (x1 - x0) * t, y0 + (y1 - y0) * t];
      };
      ctx.font = "10px 'JetBrains Mono', monospace";
      ctx.textAlign = "center";
      ctx.lineWidth = 1;
      // starfield
      for (const s of stars) {
        ctx.globalAlpha =
          0.14 + 0.16 * (0.5 + 0.5 * Math.sin(now * s.tw + s.ph));
        ctx.fillStyle = "#EDEDF2";
        ctx.beginPath();
        ctx.arc(s.x * W, s.y * H, s.r, 0, Math.PI * 2);
        ctx.fill();
      }
      ctx.globalAlpha = 1;
      // dashed constellation lines
      for (const e of EDGES) {
        const col = "rgba(237,237,242,0.18)";
        ctx.strokeStyle = col;
        ctx.setLineDash([4, 5]);
        ctx.beginPath();
        const [sx, sy] = lin(e, 0.08);
        ctx.moveTo(sx, sy);
        const [ex, ey] = lin(e, 0.92);
        ctx.lineTo(ex, ey);
        ctx.stroke();
        ctx.setLineDash([]);
        const [ax, ay] = lin(e, 0.8);
        const [bx2, by2] = lin(e, 0.86);
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
        if (e.label) {
          const [lx, ly] = lin(e, 0.5);
          ctx.fillStyle = "rgba(139,139,152,0.7)";
          ctx.fillText(e.label, lx, ly - 7);
        }
      }
      // runners emit
      const backlog =
        waitQ.length +
        dots.filter((d) => d.edge && d.edge.b === "queue").length;
      for (const r of runners) {
        r.acc += dt;
        if (r.acc > r.next && backlog < 9) {
          r.acc = 0;
          r.next = 600 + Math.random() * 900;
          NODES[r.node].flash = 220;
          dots.push({
            node: r.node,
            edge: edgeOf(r.node, "queue"),
            t: 0,
            hold: 0,
            alpha: 1,
          });
        }
      }
      // free permits pull from queue head (rate-limited mode releases on a clock)
      const isBusy = (pk: string) =>
        dots.some(
          (d) => (d.node === pk && !d.edge) || (d.edge && d.edge.b === pk),
        );
      if (rateMs) {
        cooldown -= dt;
        if (cooldown <= 0 && waitQ.length) {
          const pk = PERMITS.find((k) => !isBusy(k));
          if (pk) {
            const d = waitQ.shift();
            if (d) {
              d.edge = edgeOf("queue", pk);
              d.t = 0;
              cooldown = rateMs;
              NODES[pk].flash = 220;
            }
          }
        }
      } else {
        for (const pk of PERMITS) {
          if (!waitQ.length) break;
          if (!isBusy(pk)) {
            const d = waitQ.shift();
            if (d) {
              d.edge = edgeOf("queue", pk);
              d.t = 0;
            }
          }
        }
      }
      // step dots
      for (let i = dots.length - 1; i >= 0; i--) {
        const d = dots[i];
        if (d.edge) {
          d.t += dt / 720;
          const [x, y] = lin(d.edge, Math.min(1, d.t));
          d.x = x;
          d.y = y;
          if (d.t >= 1) {
            d.node = d.edge.b;
            d.edge = null;
            NODES[d.node].flash = 220;
            if (d.node === "queue") {
              waitQ.push(d);
              d.hold = -1;
            } else if (d.node === "done") {
              doneCount++;
              d.hold = 0;
            } else
              d.hold =
                currentMode === 1
                  ? 320
                  : currentMode === 2
                    ? 400 + Math.random() * 800
                    : 600 + Math.random() * 1400;
          }
        } else if (d.node === "done") {
          d.alpha -= dt * 0.002;
          if (d.alpha <= 0) {
            dots.splice(i, 1);
          }
        } else if (d.hold >= 0) {
          d.hold -= dt;
          if (d.hold < 0 && d.node.indexOf("permit") === 0) {
            d.edge = edgeOf(d.node, "done");
            d.t = 0;
          }
        }
      }
      // resting dots orbit their star
      const rest: Record<string, number> = {};
      for (const d of dots) {
        if (d.edge) continue;
        rest[d.node] = rest[d.node] || 0;
        const idx = rest[d.node]++;
        const [nx, ny] = P(d.node);
        const ring = 15 + 7 * Math.floor(idx / 7);
        const ang = idx * 2.4 + now * 0.00035;
        d.x = nx + ring * Math.cos(ang);
        d.y = ny + ring * Math.sin(ang);
      }
      // stars
      for (const k in NODES) {
        const n = NODES[k];
        n.flash = Math.max(0, n.flash - dt);
        const [x, y] = P(k);
        const isPermit = k.indexOf("permit") === 0;
        const busy = isPermit && dots.some((d) => d.node === k && !d.edge);
        const base =
          k === "done"
            ? AC
            : busy
              ? "#EDEDF2"
              : isPermit
                ? "rgba(184,167,255,0.9)"
                : "#EDEDF2";
        const col = n.flash > 0 ? AC : base;
        ctx.strokeStyle = col;
        ctx.globalAlpha = 0.35;
        ctx.beginPath();
        ctx.moveTo(x - 8, y);
        ctx.lineTo(x + 8, y);
        ctx.moveTo(x, y - 8);
        ctx.lineTo(x, y + 8);
        ctx.stroke();
        ctx.globalAlpha = 1;
        ctx.shadowColor = col;
        ctx.shadowBlur = 14;
        ctx.fillStyle = col;
        ctx.beginPath();
        ctx.arc(x, y, isPermit ? 3 : 3.5, 0, Math.PI * 2);
        ctx.fill();
        ctx.shadowBlur = 0;
        if (busy) {
          ctx.strokeStyle = AC;
          ctx.globalAlpha = 0.6;
          ctx.beginPath();
          ctx.arc(x, y, 8, 0, Math.PI * 2);
          ctx.stroke();
          ctx.globalAlpha = 1;
        }
        if (n.flash > 0) {
          const u = 1 - n.flash / 220;
          ctx.strokeStyle = AC;
          ctx.globalAlpha = 0.5 * (1 - u);
          ctx.beginPath();
          ctx.arc(x, y, 8 + u * 16, 0, Math.PI * 2);
          ctx.stroke();
          ctx.globalAlpha = 1;
        }
        if (n.label) {
          ctx.font = "11px 'JetBrains Mono', monospace";
          ctx.fillStyle = n.flash > 0 ? AC : "rgba(185,185,196,0.85)";
          const count = k === "done" ? ` · ${doneCount}` : "";
          ctx.fillText(n.label + count, x, y + 30);
          ctx.font = "10px 'JetBrains Mono', monospace";
        }
      }
      // group label for the permit column
      ctx.font = "11px 'JetBrains Mono', monospace";
      ctx.fillStyle = "rgba(185,185,196,0.85)";
      const p0 = NODES[PERMITS[0]];
      ctx.fillText(
        gateLabel,
        p0.x * W,
        p0.y * H - (PERMITS.length === 1 ? 28 : 22),
      );
      ctx.font = "10px 'JetBrains Mono', monospace";
      // traveling + resting dots
      for (const d of dots) {
        const atPermit = d.node.indexOf("permit") === 0 && !d.edge;
        const col =
          d.node === "done" || d.edge?.b === "done"
            ? AC
            : atPermit
              ? "#EDEDF2"
              : "#8A8A96";
        ctx.globalAlpha = Math.max(0, d.alpha);
        ctx.shadowColor = col;
        ctx.shadowBlur = atPermit || d.node === "done" ? 9 : 0;
        ctx.fillStyle = col;
        ctx.beginPath();
        ctx.arc(d.x ?? 0, d.y ?? 0, 4.5, 0, Math.PI * 2);
        ctx.fill();
        ctx.shadowBlur = 0;
        ctx.globalAlpha = 1;
      }
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, []);

  return <canvas ref={canvasRef} style={{ height: 360 }} />;
}
