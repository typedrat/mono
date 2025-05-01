import {compile, extractZqlResult} from '../../z2s/src/compiler.ts';
import type {ServerSchema} from '../../z2s/src/schema.ts';
import {formatPgInternalConvert} from '../../z2s/src/sql.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {Format} from '../../zql/src/ivm/view.ts';
import type {SchemaQuery} from '../../zql/src/mutate/custom.ts';
import {AbstractQuery, defaultFormat} from '../../zql/src/query/query-impl.ts';
import type {HumanReadable, PullRow, Query} from '../../zql/src/query/query.ts';
import type {TypedView} from '../../zql/src/query/typed-view.ts';
import type {ConnectionTransaction} from './zql-pg-database.ts';

export function makeSchemaQuery<S extends Schema>(
  schema: S,
): (
  connectionTx: ConnectionTransaction,
  serverSchema: ServerSchema,
) => SchemaQuery<S> {
  class SchemaQueryHandler {
    readonly #connectionTx: ConnectionTransaction;
    readonly #serverSchema: ServerSchema;
    constructor(
      connectionTx: ConnectionTransaction,
      serverSchema: ServerSchema,
    ) {
      this.#connectionTx = connectionTx;
      this.#serverSchema = serverSchema;
    }

    get(
      target: Record<
        string,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        Omit<Query<S, string, any>, 'materialize' | 'preload'>
      >,
      prop: string,
    ) {
      if (prop in target) {
        return target[prop];
      }

      const q = new ZPGQuery(
        schema,
        this.#serverSchema,
        prop,
        this.#connectionTx,
        {table: prop},
        defaultFormat,
      );
      target[prop] = q;
      return q;
    }
  }

  return (connectionTx: ConnectionTransaction, serverSchema: ServerSchema) =>
    new Proxy(
      {},
      new SchemaQueryHandler(connectionTx, serverSchema),
    ) as SchemaQuery<S>;
}

export class ZPGQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> extends AbstractQuery<TSchema, TTable, TReturn> {
  readonly #connectionTx: ConnectionTransaction;
  readonly #schema: TSchema;
  readonly #serverSchema: ServerSchema;

  #query:
    | {
        text: string;
        values: unknown[];
      }
    | undefined;

  constructor(
    schema: TSchema,
    serverSchema: ServerSchema,
    tableName: TTable,
    connectionTx: ConnectionTransaction,
    ast: AST,
    format: Format,
  ) {
    super(schema, tableName, ast, format);
    this.#connectionTx = connectionTx;
    this.#schema = schema;
    this.#serverSchema = serverSchema;
  }

  protected readonly _system = 'permissions';

  protected _newQuery<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
  ): ZPGQuery<TSchema, TTable, TReturn> {
    return new ZPGQuery(
      schema,
      this.#serverSchema,
      tableName,
      this.#connectionTx,
      ast,
      format,
    );
  }

  async run(): Promise<HumanReadable<TReturn>> {
    const sqlQuery =
      this.#query ??
      formatPgInternalConvert(
        compile(
          this.#serverSchema,
          this.#schema,
          this._completeAst(),
          this.format,
        ),
      );
    this.#query = sqlQuery;
    const pgIterableResult = await this.#connectionTx.query(
      sqlQuery.text,
      sqlQuery.values,
    );

    const pgArrayResult = Array.isArray(pgIterableResult)
      ? pgIterableResult
      : [...pgIterableResult];
    if (pgArrayResult.length === 0 && this.format.singular) {
      return undefined as unknown as HumanReadable<TReturn>;
    }

    return extractZqlResult(pgArrayResult) as HumanReadable<TReturn>;
  }

  preload(): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    throw new Error('Z2SQuery cannot be preloaded');
  }

  materialize(): TypedView<HumanReadable<TReturn>> {
    throw new Error('Z2SQuery cannot be materialized');
  }
}
