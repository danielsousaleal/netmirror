function defaultGatewayUrl(): string {
  if (typeof window !== "undefined") {
    return `${window.location.protocol}//${window.location.hostname}:8080`;
  }
  return "http://localhost:8080";
}

export function gatewayBaseUrl(): string {
  const configured = process.env.NEXT_PUBLIC_GATEWAY_URL?.trim();
  if (configured) {
    return configured.replace(/\/+$/, "");
  }
  return defaultGatewayUrl();
}

export function objectDownloadUrl(key: string): string {
  return `${gatewayBaseUrl()}/obj/${encodeURIComponent(key)}`;
}

export function objectUploadUrl(key: string): string {
  return `${gatewayBaseUrl()}/obj/${encodeURIComponent(key)}`;
}
