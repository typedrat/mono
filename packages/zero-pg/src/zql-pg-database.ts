import type {
  Database,
  TransactionHooks,
  TransactParams,
} from './push-processor.ts';
import type {SchemaCRUD, SchemaQuery} from '../../zql/src/mutate/custom.ts';
import {formatPg, sql} from '../../z2s/src/sql.ts';
import type {ServerSchema} from '../../z2s/src/schema.ts';
import {makeSchemaQuery} from './query.ts';
import {makeSchemaCRUD, ZQLPGTransaction} from './custom.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import {makeServerTransaction} from './custom.ts';
import type {MutationResponse} from '../../zero-protocol/src/push.ts';

export interface Connection {
  transact<R>(callback: (tx: ConnectionTransaction) => Promise<R>): Promise<R>;
}

export interface ConnectionTransaction {
  query(sql: string, params: unknown[]): Promise<unknown[]>;
}

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
      hooks: TransactionHooks,
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
