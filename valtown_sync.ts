import { email } from "https://esm.town/v/std/email";

const GOOGLE_CALENDAR_SCOPE = "https://www.googleapis.com/auth/calendar.events";

const envAccessor =
  (globalThis as { Deno?: { env?: { get(key: string): string | undefined } } }).Deno?.env;

function getEnv(key: string): string | undefined {
  if (envAccessor && typeof envAccessor.get === "function") {
    return envAccessor.get(key);
  }
  if (typeof process !== "undefined" && process.env) {
    return process.env[key];
  }
  return undefined;
}

type LogLevel = "info" | "warn" | "error";

function log(level: LogLevel, message: string, data: Record<string, unknown> = {}) {
  const entry = { level, message, ...data, timestamp: new Date().toISOString() };
  const payload = JSON.stringify(entry);
  if (level === "error") {
    console.error(payload);
  } else if (level === "warn") {
    console.warn(payload);
  } else {
    console.log(payload);
  }
}

function maskIdentifier(value?: string): string | undefined {
  if (!value) return undefined;
  if (value.length <= 8) return value;
  return `${value.slice(0, 4)}***${value.slice(-4)}`;
}

async function fetchICSContent(url: string): Promise<string> {
  const response = await fetch(url);
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(`Failed to fetch ICS feed: ${response.status} ${response.statusText} ${body}`.trim());
  }
  const text = await response.text();
  log("info", "Fetched ICS feed", {
    url,
    status: response.status,
    bytes: text.length,
  });
  return text;
}

type ParsedDate =
  | { date: string }
  | { dateTime: string; timeZone?: string };

interface ParsedEvent {
  uid: string;
  summary: string;
  description?: string;
  location?: string;
  url?: string;
  start: ParsedDate;
  end: ParsedDate;
}

let tokenCache:
  | { accessToken: string; expiresAt: number }
  | null = null;
let cryptoKeyPromise: Promise<CryptoKey> | null = null;

export default async function syncICSToGoogleCalendar(_: Request) {
  const icsUrl =
    getEnv("ICS_FEED_URL") ??
    "https://github.com/themorgantown/woodstock-filmfestival-calendar-generator/raw/main/wff_2025_complete.ics";
  const calendarId = getEnv("GOOGLE_CALENDAR_ID");
  const serviceAccountEmail = getEnv("GOOGLE_SERVICE_ACCOUNT_EMAIL");
  const privateKey = getEnv("GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY");
  const delegatedUser =
    getEnv("GOOGLE_DELEGATED_USER_EMAIL") ?? serviceAccountEmail ?? undefined;
  const defaultTimeZone =
    getEnv("DEFAULT_EVENT_TIME_ZONE") ?? "America/New_York";

  try {
    log("info", "Starting ICS to Google Calendar sync", {
      icsUrl,
      calendarId: maskIdentifier(calendarId ?? undefined),
      delegatedUser: maskIdentifier(delegatedUser ?? undefined),
      defaultTimeZone,
    });
    if (!calendarId) {
      throw new Error("Missing GOOGLE_CALENDAR_ID environment variable");
    }
    if (!serviceAccountEmail) {
      throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_EMAIL environment variable");
    }
    if (!privateKey) {
      throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY environment variable");
    }
    if (!delegatedUser) {
      throw new Error(
        "Missing GOOGLE_DELEGATED_USER_EMAIL; set it or give the service account direct access to the calendar.",
      );
    }

    const icsContent = await fetchICSContent(icsUrl);
    if (!icsContent) {
      throw new Error("Fetched ICS feed was empty");
    }

    const events = parseICS(icsContent, defaultTimeZone);
    log("info", "Parsed events from ICS feed", { count: events.length });

    const syncResults = { created: 0, updated: 0 };
    for (const event of events) {
      const outcome = await syncEventToGoogle(
        event,
        calendarId,
        serviceAccountEmail,
        privateKey,
        delegatedUser,
      );
      if (outcome === "created") syncResults.created += 1;
      if (outcome === "updated") syncResults.updated += 1;
      log("info", "Event synchronized", { uid: event.uid, outcome });
    }

    log("info", "Sync completed", {
      status: 200,
      processed: events.length,
      ...syncResults,
    });

    return new Response(
      JSON.stringify({
        success: true,
        totals: syncResults,
        processed: events.length,
        status: 200,
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  } catch (error) {
    log("error", "Sync failed", {
      status: 500,
      error: error instanceof Error ? error.message : String(error),
    });
    await email({
      subject: "ICS Sync Failed",
      text: `Error syncing ICS: ${error instanceof Error ? error.message : String(error)}`,
    });

    return new Response(
      JSON.stringify({
        success: false,
        error: error instanceof Error ? error.message : String(error),
        status: 500,
      }),
      {
        status: 500,
        headers: { "Content-Type": "application/json" },
      },
    );
  }
}

function parseICS(icsContent: string, defaultTimeZone: string): ParsedEvent[] {
  const unfoldedLines = unfoldICS(icsContent);
  const events: ParsedEvent[] = [];
  let current: Partial<ParsedEvent> | null = null;

  for (const rawLine of unfoldedLines) {
    if (!rawLine) continue;
    if (rawLine === "BEGIN:VEVENT") {
      current = {};
      continue;
    }
    if (rawLine === "END:VEVENT") {
      if (current?.uid && current?.summary && current?.start && current?.end) {
        events.push(current as ParsedEvent);
      }
      current = null;
      continue;
    }
    if (!current) continue;

    const [keyPart, valuePart = ""] = rawLine.split(":", 2);
    const [name, ...paramSegments] = keyPart.split(";");
    const params = Object.fromEntries(
      paramSegments.map((segment) => {
        const [paramKey, paramValue] = segment.split("=", 2);
        return [paramKey.toUpperCase(), paramValue];
      }),
    );
    const value = valuePart.trim();

    switch (name.toUpperCase()) {
      case "UID":
        current.uid = value;
        break;
      case "SUMMARY":
        current.summary = decodeICSText(value);
        break;
      case "DESCRIPTION":
        current.description = mergeMultilineText(current.description, value);
        break;
      case "LOCATION":
        current.location = decodeICSText(value);
        break;
      case "URL":
        current.url = value;
        break;
      case "DTSTART":
        current.start = parseICSDate(value, params, defaultTimeZone);
        break;
      case "DTEND":
        current.end = parseICSDate(value, params, defaultTimeZone);
        break;
      default:
        break;
    }
  }

  for (const event of events) {
    if (!("date" in event.end) && !("dateTime" in event.end)) {
      event.end = event.start;
    }
  }

  return events;
}

function unfoldICS(icsContent: string): string[] {
  const normalized = icsContent.replace(/\r\n/g, "\n");
  const lines = normalized.split("\n");
  const unfolded: string[] = [];

  for (const line of lines) {
    if (!line) {
      unfolded.push("");
      continue;
    }
    if (line.startsWith(" ") || line.startsWith("\t")) {
      const previous = unfolded.pop() ?? "";
      unfolded.push(previous + line.slice(1));
    } else {
      unfolded.push(line);
    }
  }

  return unfolded;
}

function mergeMultilineText(existing: string | undefined, incoming: string): string {
  const decoded = decodeICSText(incoming);
  if (!existing) return decoded;
  return `${existing}\n${decoded}`;
}

function decodeICSText(value: string): string {
  return value
    .replace(/\\n/g, "\n")
    .replace(/\\,/g, ",")
    .replace(/\\;/g, ";")
    .replace(/\\\\/g, "\\");
}

function parseICSDate(
  rawValue: string,
  params: Record<string, string>,
  defaultTimeZone: string,
): ParsedDate {
  if (params.VALUE === "DATE") {
    return { date: formatICSSimpleDate(rawValue) };
  }

  const tzId = params.TZID ?? (rawValue.endsWith("Z") ? "UTC" : defaultTimeZone);
  const { dateTime, isUTC } = formatICSTimestamp(rawValue);

  if (isUTC) {
    return { dateTime: `${dateTime}Z`, timeZone: "UTC" };
  }

  return { dateTime, timeZone: tzId };
}

function formatICSSimpleDate(raw: string): string {
  const match = raw.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (!match) {
    throw new Error(`Unrecognized all-day date format: ${raw}`);
  }
  const [, year, month, day] = match;
  return `${year}-${month}-${day}`;
}

function formatICSTimestamp(raw: string): { dateTime: string; isUTC: boolean } {
  const isUTC = raw.endsWith("Z");
  const trimmed = isUTC ? raw.slice(0, -1) : raw;
  const match = trimmed.match(/^([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})?$/);
  if (!match) {
    throw new Error(`Unrecognized date-time format: ${raw}`);
  }
  const [, year, month, day, hour, minute, second = "00"] = match;
  return { dateTime: `${year}-${month}-${day}T${hour}:${minute}:${second.padEnd(2, "0")}`, isUTC };
}

async function syncEventToGoogle(
  event: ParsedEvent,
  calendarId: string,
  serviceAccountEmail: string,
  privateKey: string,
  delegatedUser: string,
): Promise<"created" | "updated"> {
  const accessToken = await getAccessToken(
    serviceAccountEmail,
    privateKey,
    delegatedUser,
  );
  const baseUrl = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}`;

  const existing = await fetch(
    `${baseUrl}/events?maxResults=1&singleEvents=true&iCalUID=${encodeURIComponent(event.uid)}`,
    {
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    },
  );

  if (!existing.ok) {
    const errorText = await existing.text();
    throw new Error(`Failed to check existing events: ${existing.status} ${errorText}`);
  }

  const existingJson: { items?: Array<{ id: string }> } = await existing.json();
  const existingId = existingJson.items?.[0]?.id;

  const eventResource = buildGoogleEventResource(event);

  if (existingId) {
    const updateResponse = await fetch(`${baseUrl}/events/${existingId}`, {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(eventResource),
    });

    if (!updateResponse.ok) {
      const body = await updateResponse.text();
      throw new Error(`Failed to update event ${existingId}: ${updateResponse.status} ${body}`);
    }

    return "updated";
  }

  const importResponse = await fetch(`${baseUrl}/events/import`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ ...eventResource, iCalUID: event.uid }),
  });

  if (!importResponse.ok) {
    const body = await importResponse.text();
    throw new Error(`Failed to create event ${event.uid}: ${importResponse.status} ${body}`);
  }

  return "created";
}

function buildGoogleEventResource(event: ParsedEvent) {
  const resource: Record<string, unknown> = {
    summary: event.summary,
    start: event.start,
    end: event.end,
  };

  if (event.description) resource.description = event.description;
  if (event.location) resource.location = event.location;
  if (event.url) resource.source = { url: event.url };

  return resource;
}

async function getAccessToken(
  serviceAccountEmail: string,
  privateKey: string,
  delegatedUser: string,
): Promise<string> {
  const now = Date.now();
  if (tokenCache && tokenCache.expiresAt > now + 60_000) {
    return tokenCache.accessToken;
  }

  const assertion = await createJwtAssertion(
    serviceAccountEmail,
    privateKey,
    delegatedUser,
  );

  const response = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to obtain access token: ${response.status} ${errorText}`);
  }

  const { access_token, expires_in } = await response.json();
  const expiresAt = now + Math.max(0, (expires_in ?? 3600) * 1000);
  tokenCache = { accessToken: access_token, expiresAt };

  return access_token;
}

async function createJwtAssertion(
  serviceAccountEmail: string,
  privateKey: string,
  delegatedUser: string,
): Promise<string> {
  const issuedAt = Math.floor(Date.now() / 1000);
  const payload = {
    iss: serviceAccountEmail,
    scope: GOOGLE_CALENDAR_SCOPE,
    aud: "https://oauth2.googleapis.com/token",
    sub: delegatedUser,
    iat: issuedAt,
    exp: issuedAt + 3600,
  };

  const encoder = new TextEncoder();
  const headerBytes = encoder.encode(JSON.stringify({ alg: "RS256", typ: "JWT" }));
  const payloadBytes = encoder.encode(JSON.stringify(payload));
  const header = base64UrlEncode(headerBytes);
  const body = base64UrlEncode(payloadBytes);
  const toSign = encoder.encode(`${header}.${body}`);

  const key = await loadPrivateKey(privateKey);
  const signature = await crypto.subtle.sign(
    { name: "RSASSA-PKCS1-v1_5" },
    key,
    toSign,
  );

  const signatureEncoded = base64UrlEncode(new Uint8Array(signature));
  return `${header}.${body}.${signatureEncoded}`;
}

function base64UrlEncode(data: Uint8Array): string {
  let binary = "";
  for (const byte of data) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

async function loadPrivateKey(privateKey: string): Promise<CryptoKey> {
  if (!cryptoKeyPromise) {
    const cleaned = privateKey
      .replace(/-----BEGIN PRIVATE KEY-----/, "")
      .replace(/-----END PRIVATE KEY-----/, "")
      .replace(/\r?\n/g, "")
      .replace(/\s+/g, "");
    const binary = Uint8Array.from(atob(cleaned), (char) => char.charCodeAt(0));
    cryptoKeyPromise = crypto.subtle.importKey(
      "pkcs8",
      binary.buffer,
      {
        name: "RSASSA-PKCS1-v1_5",
        hash: "SHA-256",
      },
      false,
      ["sign"],
    );
  }

  return cryptoKeyPromise;
}