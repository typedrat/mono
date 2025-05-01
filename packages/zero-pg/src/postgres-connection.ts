import type {JSONValue} from '../../shared/src/json.ts';
import type {Row} from '../../zql/src/mutate/custom.ts';
import type {Connection} from './zql-pg-database.ts';

/**
 * Subset of the postgres lib's `Transaction` interface that we use.
 */
export type PostgresLibTransaction = {
  unsafe(sql: string, params: JSONValue[]): Promise<Row[]>;
};

/**
 * Subset of the postgres lib's `SQL` interface that we use.
 */
export type PostgresLibSQL = {
  unsafe(sql: string, params: JSONValue[]): Promise<Row[]>;
  begin<R>(fn: (tx: PostgresLibTransaction) => Promise<R>): Promise<R>;
};

class PostgresTransaction {
  readonly #lib: PostgresLibTransaction;

  constructor(lib: PostgresLibTransaction) {
    this.#lib = lib;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.#lib.unsafe(sql, params as JSONValue[]);
  }
}

export class PostgresConnection implements Connection {
  readonly #lib: PostgresLibSQL;
  constructor(lib: PostgresLibSQL) {
    this.#lib = lib;
  }

  /*
  TODO: needed?
  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.#lib.unsafe(sql, params as JSONValue[]);
  }
  */

  transact<T>(fn: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
    return this.#lib.begin(libTx =>
      fn(new PostgresTransaction(libTx)),
    ) as Promise<T>;
  }
}
