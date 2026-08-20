import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span className="docs-wordmark">
          effect
          <span className="docs-wordmark-accent">mq</span>
        </span>
      ),
      url: "/",
    },
    githubUrl: "https://github.com/julia-script/effectmq",
  };
}
