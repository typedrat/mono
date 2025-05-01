import type {
  Database,
  TransactHooks,
  TransactParams,
} from './push-processor.ts';
import type {SchemaCRUD, SchemaQuery} from '../../zql/src/mutate/custom.ts';
import {
  formatPg,
  sql,
  formatPgInternalConvert,
  sqlConvertColumnArg,
} from '../../z2s/src/sql.ts';
import {
  type ServerSchema,
  type ServerColumnSchema,
  type ServerTableSchema,
} from '../../z2s/src/schema.ts';
import {makeSchemaQuery} from './query.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {MutationResponse} from '../../zero-protocol/src/push.ts';
import type {Connection, ConnectionTransaction} from './connection.ts';
import type {TableSchema} from '../../zero-schema/src/table-schema.ts';
import type {TableCRUD} from '../../zql/src/mutate/custom.ts';
import {getServerSchema} from './schema.ts';

/**
 * Implements the `Database` interface needed by PushProcessor for Postgres.
 *
 * This database can execute the same mutators that run on the client-side,
 * because the Transaction it passes to those mutators exposes the same ZQL
 * API as the client-side Transaction.
 */
export class ZQLPGDatabase<S extends Schema>
  implements Database<ZQLPGTransaction<S>>
{
  readonly #connection: Connection;
  readonly #schema: S;

  readonly #mutate: (
    connectionTx: ConnectionTransaction,
    serverSchema: ServerSchema,
  ) => SchemaCRUD<S>;
  readonly #query: (
    connectionTx: ConnectionTransaction,
    serverSchema: ServerSchema,
  ) => SchemaQuery<S>;

  constructor(connection: Connection, schema: S) {
    this.#connection = connection;
    this.#schema = schema;
    this.#mutate = makeSchemaCRUD(schema);
    this.#query = makeSchemaQuery(schema);
  }

  transact(
    args: TransactParams,
    callback: (
      tx: ZQLPGTransaction<S>,
      hooks: TransactHooks,
    ) => Promise<MutationResponse>,
  ): Promise<MutationResponse> {
    return this.#connection.transact(async connectTx => {
      const zeroTx = await makeServerTransaction(
        connectTx,
        args.clientID,
        args.mutationID,
        this.#schema,
        this.#mutate,
        this.#query,
      );

      return callback(zeroTx, {
        async incrementLMID() {
          const formatted = formatPg(
            sql`INSERT INTO ${sql.ident(args.upstreamSchema)}.clients 
                    as current ("clientGroupID", "clientID", "lastMutationID")
                        VALUES (${args.clientGroupID}, ${args.clientID}, ${1})
                    ON CONFLICT ("clientGroupID", "clientID")
                    DO UPDATE SET "lastMutationID" = current."lastMutationID" + 1
                    RETURNING "lastMutationID"`,
          );

          const [{lastMutationID}] = (await connectTx.query(
            formatted.text,
            formatted.values,
          )) as {lastMutationID: bigint}[];

          return {lmid: lastMutationID};
        },
      });
    });
  }
}

export class ZQLPGTransaction<S extends Schema> {
  readonly #connectionTx: ConnectionTransaction;

  readonly location = 'server';
  readonly reason = 'authoritative';
  readonly clientID: string;
  readonly mutationID: number;
  readonly mutate: SchemaCRUD<S>;
  readonly query: SchemaQuery<S>;

  constructor(
    connectionTx: ConnectionTransaction,
    clientID: string,
    mutationID: number,
    mutate: SchemaCRUD<S>,
    query: SchemaQuery<S>,
  ) {
    this.#connectionTx = connectionTx;
    this.clientID = clientID;
    this.mutationID = mutationID;
    this.mutate = mutate;
    this.query = query;
  }

  querySQL(sql: string, params: unknown[]): Promise<unknown[]> {
    return this.#connectionTx.query(sql, params);
  }
}

const dbTxSymbol = Symbol();
const serverSchemaSymbol = Symbol();
type WithHiddenTxAndSchema = {
  [dbTxSymbol]: ConnectionTransaction;
  [serverSchemaSymbol]: ServerSchema;
};

export async function makeServerTransaction<S extends Schema>(
  connectionTx: ConnectionTransaction,
  clientID: string,
  mutationID: number,
  schema: S,
  mutate: (
    connectionTx: ConnectionTransaction,
    serverSchema: ServerSchema,
  ) => SchemaCRUD<S>,
  query: (
    connectionTx: ConnectionTransaction,
    serverSchema: ServerSchema,
  ) => SchemaQuery<S>,
) {
  const serverSchema = await getServerSchema(connectionTx, schema);
  return new ZQLPGTransaction(
    connectionTx,
    clientID,
    mutationID,
    mutate(connectionTx, serverSchema),
    query(connectionTx, serverSchema),
  );
}

export function makeSchemaCRUD<S extends Schema>(
  schema: S,
): (
  connectionTx: ConnectionTransaction,
  serverSchema: ServerSchema,
) => SchemaCRUD<S> {
  const schemaCRUDs: Record<string, TableCRUD<TableSchema>> = {};
  for (const tableSchema of Object.values(schema.tables)) {
    schemaCRUDs[tableSchema.name] = makeTableCRUD(tableSchema);
  }

  /**
   * For users with very large schemas it is expensive to re-create
   * all the CRUD mutators for each transaction. Instead, we create
   * them all once up-front and then bind them to the transaction
   * as requested.
   */
  class CRUDHandler {
    readonly connectionTx: ConnectionTransaction;
    readonly #serverSchema: ServerSchema;
    constructor(
      dbTransaction: ConnectionTransaction,
      serverSchema: ServerSchema,
    ) {
      this.connectionTx = dbTransaction;
      this.#serverSchema = serverSchema;
    }

    get(target: Record<string, TableCRUD<TableSchema>>, prop: string) {
      if (prop in target) {
        return target[prop];
      }

      const txHolder: WithHiddenTxAndSchema = {
        [dbTxSymbol]: this.connectionTx,
        [serverSchemaSymbol]: this.#serverSchema,
      };
      target[prop] = Object.fromEntries(
        Object.entries(schemaCRUDs[prop]).map(([name, method]) => [
          name,
          method.bind(txHolder),
        ]),
      ) as TableCRUD<TableSchema>;

      return target[prop];
    }
  }

  return (connectionTx: ConnectionTransaction, serverSchema: ServerSchema) =>
    new Proxy({}, new CRUDHandler(connectionTx, serverSchema)) as SchemaCRUD<S>;
}

function removeUndefined<T extends Record<string, unknown>>(value: T): T {
  const valueWithoutUndefined: Record<string, unknown> = {};
  for (const [key, val] of Object.entries(value)) {
    if (val !== undefined) {
      valueWithoutUndefined[key] = val;
    }
  }
  return valueWithoutUndefined as T;
}

function makeTableCRUD(schema: TableSchema): TableCRUD<TableSchema> {
  return {
    async insert(this: WithHiddenTxAndSchema, value) {
      value = removeUndefined(value);
      const serverTableSchema = this[serverSchemaSymbol][serverName(schema)];

      const targetedColumns = origAndServerNamesFor(Object.keys(value), schema);
      const stmt = formatPgInternalConvert(
        sql`INSERT INTO ${sql.ident(serverName(schema))} (${sql.join(
          targetedColumns.map(([, serverName]) => sql.ident(serverName)),
          ',',
        )}) VALUES (${sql.join(
          Object.entries(value).map(([col, v]) =>
            sqlInsertValue(v, serverTableSchema[serverNameFor(col, schema)]),
          ),
          ', ',
        )})`,
      );
      const tx = this[dbTxSymbol];
      await tx.query(stmt.text, stmt.values);
    },
    async upsert(this: WithHiddenTxAndSchema, value) {
      value = removeUndefined(value);
      const serverTableSchema = this[serverSchemaSymbol][serverName(schema)];
      const targetedColumns = origAndServerNamesFor(Object.keys(value), schema);
      const primaryKeyColumns = origAndServerNamesFor(
        schema.primaryKey,
        schema,
      );
      const stmt = formatPgInternalConvert(
        sql`INSERT INTO ${sql.ident(serverName(schema))} (${sql.join(
          targetedColumns.map(([, serverName]) => sql.ident(serverName)),
          ',',
        )}) VALUES (${sql.join(
          Object.entries(value).map(([col, val]) =>
            sqlInsertValue(val, serverTableSchema[serverNameFor(col, schema)]),
          ),
          ', ',
        )}) ON CONFLICT (${sql.join(
          primaryKeyColumns.map(([, serverName]) => sql.ident(serverName)),
          ', ',
        )}) DO UPDATE SET ${sql.join(
          Object.entries(value).map(
            ([col, val]) =>
              sql`${sql.ident(
                schema.columns[col].serverName ?? col,
              )} = ${sqlInsertValue(val, serverTableSchema[serverNameFor(col, schema)])}`,
          ),
          ', ',
        )}`,
      );
      const tx = this[dbTxSymbol];
      await tx.query(stmt.text, stmt.values);
    },
    async update(this: WithHiddenTxAndSchema, value) {
      value = removeUndefined(value);
      const serverTableSchema = this[serverSchemaSymbol][serverName(schema)];
      const targetedColumns = origAndServerNamesFor(Object.keys(value), schema);
      const stmt = formatPgInternalConvert(
        sql`UPDATE ${sql.ident(serverName(schema))} SET ${sql.join(
          targetedColumns.map(
            ([origName, serverName]) =>
              sql`${sql.ident(serverName)} = ${sqlInsertValue(value[origName], serverTableSchema[serverName])}`,
          ),
          ', ',
        )} WHERE ${primaryKeyClause(schema, serverTableSchema, value)}`,
      );
      const tx = this[dbTxSymbol];
      await tx.query(stmt.text, stmt.values);
    },
    async delete(this: WithHiddenTxAndSchema, value) {
      value = removeUndefined(value);
      const serverTableSchema = this[serverSchemaSymbol][serverName(schema)];
      const stmt = formatPgInternalConvert(
        sql`DELETE FROM ${sql.ident(
          serverName(schema),
        )} WHERE ${primaryKeyClause(schema, serverTableSchema, value)}`,
      );
      const tx = this[dbTxSymbol];
      await tx.query(stmt.text, stmt.values);
    },
  };
}

function serverName(x: {name: string; serverName?: string | undefined}) {
  return x.serverName ?? x.name;
}

function primaryKeyClause(
  schema: TableSchema,
  serverTableSchema: ServerTableSchema,
  row: Record<string, unknown>,
) {
  const primaryKey = origAndServerNamesFor(schema.primaryKey, schema);
  return sql`${sql.join(
    primaryKey.map(
      ([origName, serverName]) =>
        sql`${sql.ident(serverName)}${maybeCastColumn(serverTableSchema[serverName])} = ${sqlValue(row[origName], serverTableSchema[serverName])}`,
    ),
    ' AND ',
  )}`;
}

function maybeCastColumn(col: ServerColumnSchema) {
  if (col.type === 'uuid' || col.isEnum) {
    return sql`::text`;
  }
  return sql``;
}

function origAndServerNamesFor(
  originalNames: readonly string[],
  schema: TableSchema,
): [origName: string, serverName: string][] {
  return originalNames.map(
    name => [name, serverNameFor(name, schema)] as const,
  );
}

function serverNameFor(originalName: string, schema: TableSchema): string {
  const col = schema.columns[originalName];
  return col.serverName ?? originalName;
}

function sqlValue(value: unknown, serverColumnSchema: ServerColumnSchema) {
  return sqlConvertColumnArg(serverColumnSchema, value, false, true);
}

function sqlInsertValue(
  value: unknown,
  serverColumnSchema: ServerColumnSchema,
) {
  return sqlConvertColumnArg(serverColumnSchema, value, false, false);
}
