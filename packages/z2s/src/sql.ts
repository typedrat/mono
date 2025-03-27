import type {SQLQuery, FormatConfig, SQLItem} from '@databases/sql';
import baseSql, {SQLItemType} from '@databases/sql';
import {
  escapePostgresIdentifier,
  escapeSQLiteIdentifier,
} from '@databases/escape-identifier';
import type {ValueType} from '../../zero-protocol/src/client-schema.ts';

export function formatPg(sql: SQLQuery) {
  const format = new ReusingFormat(escapePostgresIdentifier);
  return sql.format((items: readonly SQLItem[]) => formatFn(items, format));
}

export function formatSqlite(sql: SQLQuery) {
  const format = new ReusingFormat(escapeSQLiteIdentifier);
  return sql.format((items: readonly SQLItem[]) => formatFn(items, format));
}

export const extractFromJson = Symbol('fromJson');
type ExtractFromJsonArg = {
  [extractFromJson]: true;
  type: ValueType;
  value: unknown;
};

class ReusingFormat implements FormatConfig {
  readonly #seen: Map<unknown, readonly [argIndex: number, packIndex: number]> =
    new Map();
  readonly escapeIdentifier: (str: string) => string;

  constructor(escapeIdentifier: (str: string) => string) {
    this.escapeIdentifier = escapeIdentifier;
  }

  formatValue = (value: unknown) => {
    const mapKey = isJsonExtract(value) ? value.value : value;
    if (this.#seen.has(mapKey)) {
      return {
        placeholder: this.#createPlaceholder(this.#seen.get(mapKey)!, value),
        value: PREVIOUSLY_SEEN_VALUE,
      };
    }
    const indices = [this.#seen.size + 1, 0] as const;
    this.#seen.set(mapKey, indices);
    return {placeholder: this.#createPlaceholder(indices, value), value};
  };

  #createPlaceholder(
    indices: readonly [argIndex: number, packIndex: number],
    value: unknown,
  ) {
    if (isJsonExtract(value)) {
      return this.#createJsonExtractPlaceholder(indices, value);
    }
    return `$${indices[0]}`;
  }

  #createJsonExtractPlaceholder(
    indices: readonly [argIndex: number, packIndex: number],
    value: ExtractFromJsonArg,
  ) {
    switch (value.type) {
      case 'json':
        return `$${indices[0]}->${indices[1]}`;
      case 'boolean':
        return `($${indices[0]}->>${indices[1]})::boolean`;
      case 'number':
        return `($${indices[0]}->>${indices[1]})::numeric`;
      case 'string':
        return `$${indices[0]}->>${indices[1]}`;
      case 'null':
        throw new Error('unsupported type');
    }
  }
}

function isJsonExtract(value: unknown): value is ExtractFromJsonArg {
  return (
    value !== null && typeof value === 'object' && extractFromJson in value
  );
}

export const sql = baseSql.default;

const PREVIOUSLY_SEEN_VALUE = Symbol('PREVIOUSLY_SEEN_VALUE');
function formatFn(
  items: readonly SQLItem[],
  {escapeIdentifier, formatValue}: FormatConfig,
): {
  text: string;
  values: unknown[];
} {
  // Create an empty query object.
  let text = '';
  const values = [];

  const localIdentifiers = new Map<unknown, string>();

  for (const item of items) {
    switch (item.type) {
      // If this is just raw text, we add it directly to the query text.
      case SQLItemType.RAW: {
        text += item.text;
        break;
      }

      // If we got a value SQL item, add a placeholder and add the value to our
      // placeholder values array.
      case SQLItemType.VALUE: {
        const {placeholder, value} = formatValue(item.value, values.length);
        text += placeholder;
        if (value !== PREVIOUSLY_SEEN_VALUE) {
          values.push(value);
        }

        break;
      }

      // If we got an identifier type, escape the strings and get a local
      // identifier for non-string identifiers.
      case SQLItemType.IDENTIFIER: {
        text += item.names
          .map((name): string => {
            if (typeof name === 'string') return escapeIdentifier(name);

            if (!localIdentifiers.has(name))
              localIdentifiers.set(name, `__local_${localIdentifiers.size}__`);

            return escapeIdentifier(localIdentifiers.get(name)!);
          })
          .join('.');
        break;
      }
    }
  }

  if (text.trim()) {
    const lines = text.split('\n');
    const min = Math.min(
      ...lines.filter(l => l.trim() !== '').map(l => /^\s*/.exec(l)![0].length),
    );
    if (min) {
      text = lines.map(line => line.substr(min)).join('\n');
    }
  }
  return {text: text.trim(), values};
}
