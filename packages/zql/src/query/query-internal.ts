import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {PullRow, Query} from './query.ts';

/** @deprecated Use Query instead */
export interface AdvancedQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TSchema, TTable>,
> extends Query<TSchema, TTable, TReturn> {}
