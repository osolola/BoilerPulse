// Thin client for the BoilerPulse gateway's HTTP API. Defaults to the
// gateway (:8090), which routes writes to the current Raft leader and
// distributes reads — talking to a single node directly (:8080/8081/8082)
// still works for anything that doesn't need that, but the gateway is the
// intended entry point as of Milestone 4.
const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8090";

export type ClusterNode = {
  id: string;
  addr?: string;
  role: string;
  status?: string; // single-node/Raft shape
  reachable?: boolean; // gateway shape
  term?: number;
};

export type ClusterStatus = {
  mode: string;
  leader_id?: string;
  term?: number;
  nodes: ClusterNode[];
};

export type HealthStatus = {
  status: string;
};

export type HotKey = {
  key: string;
  count: number;
};

export type CacheStats = {
  hits: number;
  misses: number;
  evictions: number;
  hit_rate: number;
};

export type WorkloadStatus = {
  mode: string;
  rps: number;
  hot_keys?: HotKey[];
  cache: CacheStats;
};

export type CampusLocation = {
  name: string;
  latitude?: number;
  longitude?: number;
};

export type CampusEvent = {
  id: string;
  type: string;
  title: string;
  description?: string;
  location: CampusLocation;
  start_time: string;
  end_time?: string;
  expected_attendance?: number;
  expected_traffic_multiplier?: number;
  urgency: string;
  audience?: string[];
  source?: string;
  created_at?: string;
  confidence?: number;
};

export type EventsList = {
  events: CampusEvent[];
};

export type PredictionInput = {
  type: string;
  title: string;
  start_time: string;
  end_time?: string;
  expected_attendance?: number;
};

export type PredictionOutput = {
  predicted_rps: number;
  confidence: number;
  peak_time: string;
  recommended_nodes: number;
};

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`${path} responded with ${res.status}`);
  }
  return res.json() as Promise<T>;
}

async function postJSON<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`${path} responded with ${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

export function getHealth(): Promise<HealthStatus> {
  return getJSON<HealthStatus>("/healthz");
}

export function getClusterStatus(): Promise<ClusterStatus> {
  return getJSON<ClusterStatus>("/v1/cluster");
}

export function getWorkloadStatus(): Promise<WorkloadStatus> {
  return getJSON<WorkloadStatus>("/v1/workload");
}

export function getEvents(): Promise<EventsList> {
  return getJSON<EventsList>("/v1/events");
}

export function predictTraffic(input: PredictionInput): Promise<PredictionOutput> {
  return postJSON<PredictionOutput>("/v1/predict", input);
}

// --- Chaos/failure-injection admin proxy (spec §23/§34; internal/admin +
// internal/gateway/admin_proxy.go). Every call here goes through the
// gateway's /v1/admin/* routes, never a node's admin port directly, so the
// browser only ever needs to know the gateway's origin. All of it is
// gated by the shared admin token -- there is no default, and an empty or
// wrong token gets a 401/503 from the gateway, surfaced as a thrown Error.

export type AdminFaultStatus = {
  partitioned: boolean;
  latency_ms: number;
  drop_rate: number;
};

export type AdminRaftStatus = {
  state: string;
  term?: number;
  leader_id?: string;
};

export type AdminNodeStatus = {
  faults: AdminFaultStatus;
  raft: AdminRaftStatus;
};

export type AdminStatusAll = Record<string, AdminNodeStatus | { error: string }>;

async function adminRequest<T>(path: string, token: string, method: string, body?: unknown): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`${path} responded with ${res.status}: ${text}`);
  }
  return res.json() as Promise<T>;
}

export function getAdminStatusAll(token: string): Promise<AdminStatusAll> {
  return adminRequest<AdminStatusAll>("/v1/admin/status", token, "GET");
}

export function adminKill(nodeID: string, token: string): Promise<unknown> {
  return adminRequest(`/v1/admin/${nodeID}/kill`, token, "POST");
}

export function adminFault(
  nodeID: string,
  token: string,
  fault: { partitioned?: boolean; latency_ms?: number; drop_rate?: number },
): Promise<AdminNodeStatus> {
  return adminRequest<AdminNodeStatus>(`/v1/admin/${nodeID}/fault`, token, "POST", fault);
}

export function adminRestore(nodeID: string, token: string): Promise<AdminNodeStatus> {
  return adminRequest<AdminNodeStatus>(`/v1/admin/${nodeID}/restore`, token, "POST");
}
