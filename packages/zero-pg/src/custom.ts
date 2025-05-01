import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {TableSchema} from '../../zero-schema/src/table-schema.ts';
import type {
  SchemaCRUD,
  SchemaQuery,
  TableCRUD,
} from '../../zql/src/mutate/custom.ts';
import {
  formatPgInternalConvert,
  sql,
  sqlConvertColumnArg,
} from '../../z2s/src/sql.ts';
import type {
  ServerColumnSchema,
  ServerSchema,
  ServerTableSchema,
} from '../../z2s/src/schema.ts';
import {getServerSchema} from './schema.ts';
import type {ConnectionTransaction} from './zql-pg-database.ts';

export type CustomMutatorDefs<TDBTransaction> = {
  [namespaceOrKey: string]:
    | {
        [key: string]: CustomMutatorImpl<TDBTransaction>;
      }
    | CustomMutatorImpl<TDBTransaction>;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type CustomMutatorImpl<TDBTransaction, TArgs = any> = (
  tx: TDBTransaction,
  args: TArgs,
) => Promise<void>;

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
