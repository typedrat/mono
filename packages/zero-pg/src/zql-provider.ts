import type {DatabaseProvider} from './push-processor.ts';
import type {JSONValue} from '../../shared/src/json.ts';
import type {
  DBTransaction,
  Row,
  SchemaCRUD,
  SchemaQuery,
} from '../../zql/src/mutate/custom.ts';
import {formatPg, sql} from '../../z2s/src/sql.ts';
import type {ServerSchema} from '../../z2s/src/schema.ts';
import {makeSchemaQuery} from './query.ts';
import {makeSchemaCRUD, TransactionImpl} from './custom.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import {getServerSchema} from './schema.ts';
/**
 * Subset of the postgres lib's `Transaction` interface that we use.
 */
export type PostgresTransaction = {
  unsafe(sql: string, params: JSONValue[]): Promise<Row[]>;
};

/**
 * Subset of the postgres lib's `SQL` interface that we use.
 */
export type PostgresSQL<Transaction extends PostgresTransaction> = {
  unsafe(sql: string, params: JSONValue[]): Promise<Row[]>;
  begin<T>(fn: (tx: Transaction) => Promise<T>): Promise<T>;
};

class Transaction<WrappedTransaction extends PostgresTransaction>
  implements DBTransaction<WrappedTransaction>
{
  readonly wrappedTransaction: WrappedTransaction;
  constructor(pgTx: WrappedTransaction) {
    this.wrappedTransaction = pgTx;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.wrappedTransaction.unsafe(sql, params as JSONValue[]);
  }
}

async function updateClientMutationID(
  input: {
    schema: string;
    clientGroupID: string;
    clientID: string;
    mutationID: number;
  },
  dbTx: Transaction<PostgresTransaction>,
) {
  const formatted = formatPg(
    sql`INSERT INTO ${sql.ident(input.schema)}.clients 
            as current ("clientGroupID", "clientID", "lastMutationID")
                VALUES (${input.clientGroupID}, ${input.clientID}, ${1})
            ON CONFLICT ("clientGroupID", "clientID")
            DO UPDATE SET "lastMutationID" = current."lastMutationID" + 1
            RETURNING "lastMutationID"`,
  );

  const [{lastMutationID}] = (await dbTx.query(
    formatted.text,
    formatted.values,
  )) as {lastMutationID: bigint}[];

  return {lastMutationID};
}

export class ZQLPGDatabaseProvider<S extends Schema>
  implements DatabaseProvider<TransactionImpl<S, PostgresTransaction>>
{
  readonly #pg: PostgresSQL<PostgresTransaction>;
  readonly #mutate: (
    dbTransaction: DBTransaction<unknown>,
    serverSchema: ServerSchema,
  ) => SchemaCRUD<S>;
  readonly #query: (
    dbTransaction: DBTransaction<unknown>,
    serverSchema: ServerSchema,
  ) => SchemaQuery<S>;
  readonly #schema: S;
  constructor(pg: PostgresSQL<PostgresTransaction>, schema: S) {
    this.#pg = pg;
    this.#mutate = makeSchemaCRUD(schema);
    this.#query = makeSchemaQuery(schema);
    this.#schema = schema;
  }

  transaction<R>(
    cb: (tx: TransactionImpl<S, PostgresTransaction>) => Promise<R>,
    transactionInput: {
      clientGroupID: string;
      clientID: string;
      mutationID: number;
    },
  ): Promise<R> {
    return this.#pg.begin(async pgTx => {
      const dbTx = new Transaction(pgTx);
      const serverSchema = await getServerSchema(dbTx, this.#schema);
      const zeroTx = new TransactionImpl(
        dbTx,
        transactionInput.clientID,
        transactionInput.mutationID,
        this.#mutate(dbTx, serverSchema),
        this.#query(dbTx, serverSchema),
        input => updateClientMutationID(input, dbTx),
      );

      return cb(zeroTx);
    });
  }
}
