import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // `next dev` otherwise auto-generates AGENTS.md/CLAUDE.md on every start;
  // this repo intentionally has neither (see repo root's own conventions).
  agentRules: false,
};

export default nextConfig;
