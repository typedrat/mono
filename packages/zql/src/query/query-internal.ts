import type {
  Schema,
  TableNames,
} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {PullRow, Query} from './query.ts';

/** @deprecated Use Query instead */
export interface AdvancedQuery<
  TSchema extends Schema,
  TTable extends TableNames<TSchema>,
  TReturn = PullRow<TSchema, TTable>,
> extends Query<TSchema, TTable, TReturn> {}
