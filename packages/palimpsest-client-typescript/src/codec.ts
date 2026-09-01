// Row→object conversion. The wasm-side encoder hands us untyped
// `unknown[][]` aligned with `schema.columns`; we project that into the
// caller-supplied row type, with optional per-column decoder hooks for
// callers who need to massage values (e.g. parse JSON columns, convert
// `bigint` to `number` for safe integers).

import type { Schema } from "./types.js";

export type ColumnDecoder = (
  value: unknown,
  column: Schema["columns"][number],
) => unknown;

export interface RowDecoderOptions {
  /**
   * Optional per-column override. Receives the raw value (which can be
   * `bigint`/`string`/`boolean`/`null` per the wasm-side codec) and the
   * column metadata; returns whatever shape the row type expects.
   */
  decoders?: Record<string, ColumnDecoder>;
  /**
   * If `true`, narrow `bigint` values to `number` when they fit in
   * `Number.MAX_SAFE_INTEGER` (the wasm side widens `i64` to `bigint`
   * to avoid silent precision loss; `i16`/`i32` already arrive as
   * `number`). Values outside the safe range stay `bigint`. Defaults
   * to `false`.
   */
  coerceSafeIntegersToNumber?: boolean;
}

const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

/**
 * Project a single raw row into the caller's row type.
 *
 * `T` is assumed to be a plain object whose keys are a subset of
 * `schema.columns.name`. Extra keys are filled in from the row in
 * declaration order.
 */
export function decodeRow<T>(
  row: unknown[],
  schema: Schema,
  options: RowDecoderOptions = {},
): T {
  const obj: Record<string, unknown> = {};
  const decoders = options.decoders ?? {};
  for (let i = 0; i < schema.columns.length; i++) {
    const col = schema.columns[i]!;
    let value = row[i];
    if (col.name in decoders) {
      const decode = decoders[col.name]!;
      value = decode(value, col);
    } else if (
      options.coerceSafeIntegersToNumber &&
      typeof value === "bigint" &&
      value <= MAX_SAFE &&
      value >= -MAX_SAFE
    ) {
      value = Number(value);
    }
    obj[col.name] = value;
  }
  return obj as T;
}

export function decodeRows<T>(
  rows: unknown[][],
  schema: Schema,
  options?: RowDecoderOptions,
): T[] {
  return rows.map((row) => decodeRow<T>(row, schema, options));
}

/** Stringify the primary-key portion of a row for use as a Map key. */
export function primaryKey(row: unknown[], schema: Schema): string {
  return schema.primaryKeyColumns
    .map((i) => {
      const v = row[i];
      return typeof v === "bigint" ? v.toString() : String(v);
    })
    .join("|");
}
