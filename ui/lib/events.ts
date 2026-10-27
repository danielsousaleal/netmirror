export const REFRESH_EVENT = "netmirror:refresh";

export function requestPageRefresh(): void {
  if (typeof window !== "undefined") {
    window.dispatchEvent(new Event(REFRESH_EVENT));
  }
}
