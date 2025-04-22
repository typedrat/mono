import type {
  DatabaseProvider,
  TransactionProviderHooks,
  TransactionProviderInput,
} from './push-processor.ts';
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
import {makeServerTransaction} from './custom.ts';

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
    callback: (
      tx: TransactionImpl<S, PostgresTransaction>,
      transactionHooks: TransactionProviderHooks,
    ) => Promise<R>,
    transactionInput: TransactionProviderInput,
  ): Promise<R> {
    return this.#pg.begin(async pgTx => {
      const dbTx = new Transaction(pgTx);

      const zeroTx = await makeServerTransaction(
        dbTx,
        transactionInput.clientID,
        transactionInput.mutationID,
        this.#schema,
        this.#mutate,
        this.#query,
      );

      return callback(zeroTx, {
        async updateClientMutationID() {
          const formatted = formatPg(
            sql`INSERT INTO ${sql.ident(transactionInput.upstreamSchema)}.clients 
                    as current ("clientGroupID", "clientID", "lastMutationID")
                        VALUES (${transactionInput.clientGroupID}, ${transactionInput.clientID}, ${1})
                    ON CONFLICT ("clientGroupID", "clientID")
                    DO UPDATE SET "lastMutationID" = current."lastMutationID" + 1
                    RETURNING "lastMutationID"`,
          );

          const [{lastMutationID}] = (await dbTx.query(
            formatted.text,
            formatted.values,
          )) as {lastMutationID: bigint}[];

          return {lastMutationID};
        },
      });
    });
  }
}
