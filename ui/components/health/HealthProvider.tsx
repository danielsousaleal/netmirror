"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

export type ClusterNodeStats = {
  id: string;
  status: "online" | "offline";
  disk_total: number;
  disk_free: number;
  objects: number;
};

export type ClusterStats = {
  nodes: ClusterNodeStats[];
  total_objects: number;
  total_chunks: number;
};

export type ActivityEvent = {
  id: number;
  type: string;
  message: string;
  timestamp: number;
};

type ActivityResponse = {
  events?: ActivityEvent[];
};

type ClusterState = "checking" | "online" | "warning";

type HealthContextValue = {
  state: ClusterState;
  stats: ClusterStats | null;
  activity: ActivityEvent[];
  refresh: () => void;
};

const HealthContext = createContext<HealthContextValue | null>(null);

export function useClusterHealth(): HealthContextValue {
  const context = useContext(HealthContext);
  if (context === null) {
    throw new Error("useClusterHealth must be used inside HealthProvider");
  }
  return context;
}

export default function HealthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<ClusterState>("checking");
  const [stats, setStats] = useState<ClusterStats | null>(null);
  const [activity, setActivity] = useState<ActivityEvent[]>([]);
  const [refreshKey, setRefreshKey] = useState(0);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();

    const check = async () => {
      try {
        const response = await fetch("/api/cluster/stats", {
          cache: "no-store",
          signal: controller.signal,
        });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const payload = (await response.json()) as ClusterStats;
        if (!Array.isArray(payload.nodes)) {
          throw new Error("Malformed cluster stats");
        }

        let events: ActivityEvent[] = [];
        try {
          const activityResponse = await fetch("/api/cluster/activity", {
            cache: "no-store",
            signal: controller.signal,
          });
          if (activityResponse.ok) {
            const activityPayload =
              (await activityResponse.json()) as ActivityResponse;
            if (Array.isArray(activityPayload.events)) {
              events = activityPayload.events;
            }
          }
        } catch {
          // Activity is supplemental; keep the cluster stats usable.
        }

        if (cancelled) {
          return;
        }
        const allOnline =
          payload.nodes.length > 0 &&
          payload.nodes.every((node) => node.status === "online");
        setStats(payload);
        setActivity(events);
        setState(allOnline ? "online" : "warning");
      } catch {
        if (!cancelled) {
          setStats(null);
          setActivity([]);
          setState("warning");
        }
      }
    };

    void check();
    const timer = window.setInterval(() => void check(), 5000);
    return () => {
      cancelled = true;
      controller.abort();
      window.clearInterval(timer);
    };
  }, [refreshKey]);

  const refresh = useCallback(() => {
    setState("checking");
    setRefreshKey((value) => value + 1);
  }, []);

  const value = useMemo(
    () => ({ state, stats, activity, refresh }),
    [state, stats, activity, refresh],
  );

  return (
    <HealthContext.Provider value={value}>{children}</HealthContext.Provider>
  );
}
