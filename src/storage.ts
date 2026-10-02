// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

// Large HTTP requests and results through S3-compatible object storage
// (VGI-RPC external locations), matching grainlift-server's
// `[external_storage]`. A client whose request exceeds the request limit asks
// for an upload URL (`POST /__upload_url__/init`), PUTs the request to the
// bucket and sends only a pointer; a result batch over the threshold is stored
// in the bucket and the client is sent a URL to fetch it. URLs are presigned
// here (AWS Signature Version 4, query string) with WebCrypto, so this runs on
// Node.js, Bun and Cloudflare Workers without an AWS SDK.
import type { HttpHandlerOptions } from "@query-farm/vgi-rpc";

/** Settings for {@link ExternalStorageConfig}. */
export interface ExternalStorageOptions {
  /** The S3 API endpoint, e.g. `https://<account>.r2.cloudflarestorage.com`. */
  endpoint: string;
  bucket: string;
  /** The signing region (default `auto`, as R2 uses). */
  region?: string;
  /** Key prefix for the service's objects (default none). */
  prefix?: string;
  /** Default: the `AWS_ACCESS_KEY_ID` environment variable, where there is one. */
  accessKeyId?: string;
  /** Default: the `AWS_SECRET_ACCESS_KEY` environment variable, where there is one. */
  secretAccessKey?: string;
  /** `https://<bucket>.<endpoint host>/` instead of `<endpoint>/<bucket>/`. */
  virtualHostedStyle?: boolean;
  /** How long presigned URLs stay valid: 1 second to 7 days (default 900). */
  urlTtlSeconds?: number;
  /** Result batches at least this large go to the bucket (default 1 MiB). */
  thresholdBytes?: number;
  /** The largest request a client may upload (default 256 MiB). */
  maxUploadBytes?: number;
  /** Request implementation for uploads and downloads (default global `fetch`). */
  fetch?: typeof globalThis.fetch;
}

/** The VGI-RPC HTTP options a bucket provides. */
export type ExternalStorageHttpOptions = Required<
  Pick<HttpHandlerOptions, "uploadUrlProvider" | "maxUploadBytes" | "externalLocation">
>;

const encoder = new TextEncoder();

function environment(name: string): string | undefined {
  const value = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.[
    name
  ];
  return value ? value : undefined;
}

function hex(bytes: ArrayBuffer): string {
  return [...new Uint8Array(bytes)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

/** SigV4 URI encoding: everything but unreserved characters (and `/` in paths). */
function uriEncode(value: string, encodeSlash: boolean): string {
  let out = "";
  for (const byte of encoder.encode(value)) {
    const char = String.fromCharCode(byte);
    if (/[A-Za-z0-9\-_.~]/.test(char) || (char === "/" && !encodeSlash)) out += char;
    else out += `%${byte.toString(16).toUpperCase().padStart(2, "0")}`;
  }
  return out;
}

async function hmac(key: Uint8Array<ArrayBuffer> | ArrayBuffer, data: string): Promise<ArrayBuffer> {
  const imported = await crypto.subtle.importKey("raw", key, { name: "HMAC", hash: "SHA-256" }, false, [
    "sign",
  ]);
  return crypto.subtle.sign("HMAC", imported, encoder.encode(data));
}

/** `YYYYMMDD` and `YYYYMMDDTHHMMSSZ`. */
function amzDates(now: Date): [string, string] {
  const timestamp = now
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d{3}/, "");
  return [timestamp.slice(0, 8), timestamp];
}

function positive(name: string, value: number, max = Number.MAX_SAFE_INTEGER): number {
  if (!Number.isSafeInteger(value) || value < 1 || value > max)
    throw new TypeError(`${name} must be an integer between 1 and ${max}`);
  return value;
}

/**
 * An S3-compatible bucket (AWS S3, Cloudflare R2, MinIO, ...) for large
 * requests and results. Pass it as `externalStorage` to
 * `GrainliftService.httpHandler` (or `serveHttp`). The gateway never deletes
 * objects: give the bucket a lifecycle rule that expires them, and for browser
 * clients a CORS rule allowing PUT and GET from their origin.
 *
 * The secret key is held in a private field: it is not enumerable and never
 * appears in `JSON.stringify`, `String()` or `util.inspect`.
 */
export class ExternalStorageConfig {
  readonly endpoint: string;
  readonly bucket: string;
  readonly region: string;
  readonly prefix: string;
  readonly accessKeyId: string;
  readonly virtualHostedStyle: boolean;
  readonly urlTtlSeconds: number;
  readonly thresholdBytes: number;
  readonly maxUploadBytes: number;
  readonly #secretAccessKey: string;
  readonly #fetch: typeof globalThis.fetch;
  /** Objects live under this URL; ends in `/`. */
  readonly #base: URL;

  constructor(options: ExternalStorageOptions) {
    let endpoint: URL;
    try {
      endpoint = new URL(options.endpoint);
    } catch {
      throw new TypeError("endpoint must be an absolute http(s) URL");
    }
    if ((endpoint.protocol !== "https:" && endpoint.protocol !== "http:") || !endpoint.hostname)
      throw new TypeError("endpoint must be an absolute http(s) URL");
    if (endpoint.search || endpoint.hash) throw new TypeError("endpoint must not have a query or fragment");
    if (!options.bucket?.trim()) throw new TypeError("bucket must not be empty");
    const region = options.region ?? "auto";
    if (!region.trim()) throw new TypeError("region must not be empty");
    const accessKeyId = options.accessKeyId ?? environment("AWS_ACCESS_KEY_ID");
    const secretAccessKey = options.secretAccessKey ?? environment("AWS_SECRET_ACCESS_KEY");
    if (!accessKeyId || !secretAccessKey)
      throw new TypeError(
        "external storage needs credentials: pass accessKeyId and secretAccessKey, " +
          "or set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY",
      );
    this.endpoint = options.endpoint;
    this.bucket = options.bucket;
    this.region = region;
    this.prefix = options.prefix ?? "";
    this.accessKeyId = accessKeyId;
    this.virtualHostedStyle = options.virtualHostedStyle ?? false;
    this.urlTtlSeconds = positive("urlTtlSeconds", options.urlTtlSeconds ?? 900, 604_800);
    this.thresholdBytes = positive("thresholdBytes", options.thresholdBytes ?? 1024 * 1024);
    this.maxUploadBytes = positive("maxUploadBytes", options.maxUploadBytes ?? 256 * 1024 * 1024);
    this.#secretAccessKey = secretAccessKey;
    this.#fetch = options.fetch ?? ((input, init) => globalThis.fetch(input, init));
    const base = new URL(endpoint.href);
    const path = base.pathname.replace(/\/+$/, "");
    if (this.virtualHostedStyle) {
      base.hostname = `${this.bucket}.${base.hostname}`;
      base.pathname = `${path}/`;
    } else {
      base.pathname = `${path}/${uriEncode(this.bucket, true)}/`;
    }
    this.#base = base;
  }

  /** A URL valid for `method` on `key` for `expiresSeconds` from `now`. */
  async presign(
    method: "GET" | "PUT",
    key: string,
    now: Date = new Date(),
    expiresSeconds: number = this.urlTtlSeconds,
  ): Promise<string> {
    const path = `${this.#base.pathname}${uriEncode(key, false)}`;
    const host = this.#base.host;
    const [date, timestamp] = amzDates(now);
    const scope = `${date}/${this.region}/s3/aws4_request`;
    const query = [
      ["X-Amz-Algorithm", "AWS4-HMAC-SHA256"],
      ["X-Amz-Credential", `${this.accessKeyId}/${scope}`],
      ["X-Amz-Date", timestamp],
      ["X-Amz-Expires", String(expiresSeconds)],
      ["X-Amz-SignedHeaders", "host"],
    ]
      .map(([name, value]) => `${uriEncode(name!, true)}=${uriEncode(value!, true)}`)
      .sort()
      .join("&");
    const canonical = `${method}\n${path}\n${query}\nhost:${host}\n\nhost\nUNSIGNED-PAYLOAD`;
    const digest = hex(await crypto.subtle.digest("SHA-256", encoder.encode(canonical)));
    const toSign = `AWS4-HMAC-SHA256\n${timestamp}\n${scope}\n${digest}`;
    let signingKey = await hmac(encoder.encode(`AWS4${this.#secretAccessKey}`), date);
    for (const part of [this.region, "s3", "aws4_request"]) signingKey = await hmac(signingKey, part);
    const signature = hex(await hmac(signingKey, toSign));
    return `${this.#base.protocol}//${host}${path}?${query}&X-Amz-Signature=${signature}`;
  }

  /** Accepts only this bucket's objects, so a client cannot point the gateway elsewhere. */
  validate(raw: string): void {
    let url: URL;
    try {
      url = new URL(raw);
    } catch {
      throw new Error("invalid external location URL");
    }
    if (url.origin !== this.#base.origin || !url.pathname.startsWith(this.#base.pathname))
      throw new Error("external location URL is not in this gateway's storage bucket");
  }

  /** The VGI-RPC HTTP options: the upload URL provider and result storage. */
  httpOptions(): ExternalStorageHttpOptions {
    const pair = async () => {
      const key = `${this.prefix}${crypto.randomUUID()}.arrow`;
      const now = new Date();
      return {
        uploadUrl: await this.presign("PUT", key, now),
        downloadUrl: await this.presign("GET", key, now),
        expiresAt: new Date(now.getTime() + this.urlTtlSeconds * 1000),
      };
    };
    return {
      uploadUrlProvider: { generateUploadUrl: pair },
      maxUploadBytes: this.maxUploadBytes,
      externalLocation: {
        storage: {
          upload: async (data, contentEncoding) => {
            const urls = await pair();
            const headers: Record<string, string> = { "content-type": "application/vnd.apache.arrow.stream" };
            if (contentEncoding) headers["content-encoding"] = contentEncoding;
            const response = await this.#fetch(urls.uploadUrl, {
              method: "PUT",
              headers,
              body: data as Uint8Array<ArrayBuffer>,
            });
            // Drain the body so the connection can be reused.
            await response.arrayBuffer().catch(() => undefined);
            if (!response.ok) throw new Error(`object storage PUT returned ${response.status}`);
            return urls.downloadUrl;
          },
        },
        externalizeThresholdBytes: this.thresholdBytes,
        urlValidator: (url) => this.validate(url),
        maxFetchBytes: this.maxUploadBytes,
        maxDecompressedBytes: this.maxUploadBytes,
        fetch: this.#fetch,
      },
    };
  }

  toJSON(): Record<string, unknown> {
    return {
      endpoint: this.endpoint,
      bucket: this.bucket,
      region: this.region,
      prefix: this.prefix,
      accessKeyId: this.accessKeyId,
      virtualHostedStyle: this.virtualHostedStyle,
      urlTtlSeconds: this.urlTtlSeconds,
      thresholdBytes: this.thresholdBytes,
      maxUploadBytes: this.maxUploadBytes,
    };
  }

  toString(): string {
    return `ExternalStorageConfig(${this.endpoint}, bucket ${this.bucket})`;
  }
}
