import type {TableSchema} from '../../../zero-schema/src/table-schema.js';
import type {WriteTransaction} from './replicache-types.js';
import type {Schema} from '../../../zero-schema/src/mod.js';
import type {ClientID, ReadonlyJSONValue} from '../mod.js';
import type {NormalizedTableSchema} from '../../../zero-schema/src/normalize-table-schema.js';
import type {NormalizedSchema} from '../../../zero-schema/src/normalized-schema.js';
import {must} from '../../../shared/src/must.js';
import {
  deleteImpl,
  insertImpl,
  updateImpl,
  upsertImpl,
  type DeleteID,
  type InsertValue,
  type UpdateValue,
  type UpsertValue,
} from './crud.js';

export type CustomMutatorDefs<S extends Schema> = {
  [key: string]: CustomMutatorImpl<S>;
};

export type CustomMutatorImpl<S extends Schema> = (
  tx: Transaction<S>,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  args?: any | undefined,
) => void;

// eslint-disable-next-line arrow-body-style
export function makeReplicacheMutator(
  mutator: CustomMutatorImpl<Schema>,
  schema: NormalizedSchema,
) {
  return (repTx: WriteTransaction, args: ReadonlyJSONValue) => {
    const tx = new TransactionImpl(repTx, schema);
    mutator(tx, args);
  };
}

export type MakeCustomMutatorInterfaces<
  S extends Schema,
  MD extends CustomMutatorDefs<S>,
> = {
  readonly [P in keyof MD]: MakeCustomMutatorInterface<S, MD[P]>;
};

export type MakeCustomMutatorInterface<
  S extends Schema,
  F extends (tx: Transaction<S>, ...args: [] | [ReadonlyJSONValue]) => void,
> = F extends (tx: Transaction<S>, ...args: infer Args) => void
  ? (...args: Args) => void
  : never;

export type TransactionReason = 'optimistic' | 'rebase';

/**
 * WriteTransactions are used with *mutators* which are registered using
 * {@link ReplicacheOptions.mutators} and allows read and write operations on the
 * database.
 */
export interface Transaction<S extends Schema> {
  readonly clientID: ClientID;
  /**
   * The ID of the mutation that is being applied.
   */
  readonly mutationID: number;

  /**
   * The reason for the transaction.
   */
  readonly reason: TransactionReason;

  readonly mutate: SchemaCRUD<S>;
}

export class TransactionImpl implements Transaction<Schema> {
  constructor(repTx: WriteTransaction, schema: NormalizedSchema) {
    must(repTx.reason === 'initial' || repTx.reason === 'rebase');
    this.clientID = repTx.clientID;
    this.mutationID = repTx.mutationID;
    this.reason = repTx.reason === 'initial' ? 'optimistic' : 'rebase';
    this.mutate = makeSchemaCRUD(schema, repTx);
  }

  readonly clientID: ClientID;
  readonly mutationID: number;
  readonly reason: TransactionReason;
  readonly mutate: SchemaCRUD<Schema>;
}

type SchemaCRUD<S extends Schema> = {
  [P in keyof S['tables']]: TableCRUD<S['tables'][P]>;
};

function makeSchemaCRUD(schema: NormalizedSchema, tx: WriteTransaction) {
  const mutate: Record<string, TableCRUD<TableSchema>> = {};
  for (const [name] of Object.entries(schema.tables)) {
    mutate[name] = makeTableCRUD(schema, name, tx);
  }
  return mutate;
}

export type TableCRUD<S extends TableSchema> = {
  /**
   * Writes a row if a row with the same primary key doesn't already exists.
   * Non-primary-key fields that are 'optional' can be omitted or set to
   * `undefined`. Such fields will be assigned the value `null` optimistically
   * and then the default value as defined by the server.
   */
  insert: (value: InsertValue<S>) => void;

  /**
   * Writes a row unconditionally, overwriting any existing row with the same
   * primary key. Non-primary-key fields that are 'optional' can be omitted or
   * set to `undefined`. Such fields will be assigned the value `null`
   * optimistically and then the default value as defined by the server.
   */
  upsert: (value: UpsertValue<S>) => void;

  /**
   * Updates a row with the same primary key. If no such row exists, this
   * function does nothing. All non-primary-key fields can be omitted or set to
   * `undefined`. Such fields will be left unchanged from previous value.
   */
  update: (value: UpdateValue<S>) => void;

  /**
   * Deletes the row with the specified primary key. If no such row exists, this
   * function does nothing.
   */
  delete: (id: DeleteID<S>) => void;
};

function makeTableCRUD(
  schema: NormalizedSchema,
  tableName: string,
  tx: WriteTransaction,
) {
  const table = must(schema.tables[tableName]);
  const {primaryKey} = table;
  return {
    insert: (value: InsertValue<NormalizedTableSchema>) =>
      void insertImpl(tx, {op: 'insert', tableName, primaryKey, value}, schema),
    upsert: (value: UpsertValue<NormalizedTableSchema>) =>
      void upsertImpl(tx, {op: 'upsert', tableName, primaryKey, value}, schema),
    update: (value: UpdateValue<NormalizedTableSchema>) =>
      void updateImpl(tx, {op: 'update', tableName, primaryKey, value}, schema),
    delete: (id: DeleteID<NormalizedTableSchema>) =>
      void deleteImpl(
        tx,
        {op: 'delete', tableName, primaryKey, value: id},
        schema,
      ),
  };
}
