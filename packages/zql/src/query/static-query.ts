import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {
  Schema,
  TableNames,
} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {Format} from '../ivm/view.ts';
import {ExpressionBuilder} from './expression.ts';
import {AbstractQuery, defaultFormat} from './query-impl.ts';
import type {HumanReadable, PullRow, Query} from './query.ts';
import type {TypedView} from './typed-view.ts';

export function staticQuery<
  TSchema extends Schema,
  TTable extends TableNames<TSchema>,
>(schema: TSchema, tableName: TTable): Query<TSchema, TTable> {
  return new StaticQuery<TSchema, TTable>(
    schema,
    tableName,
    {table: tableName},
    defaultFormat,
  );
}

/**
 * A query that cannot be run.
 * Only serves to generate ASTs.
 */
export class StaticQuery<
  TSchema extends Schema,
  TTable extends TableNames<TSchema>,
  TReturn = PullRow<TSchema, TTable>,
> extends AbstractQuery<TSchema, TTable, TReturn> {
  expressionBuilder() {
    return new ExpressionBuilder(this._exists);
  }

  protected readonly _system = 'permissions';

  protected _newQuery<
    TSchema extends Schema,
    TTable extends TableNames<TSchema>,
    TReturn,
  >(
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
  ): StaticQuery<TSchema, TTable, TReturn> {
    return new StaticQuery(schema, tableName, ast, format);
  }

  get ast() {
    return this._completeAst();
  }

  materialize(): TypedView<HumanReadable<TReturn>> {
    throw new Error('StaticQuery cannot be materialized');
  }

  run(): Promise<HumanReadable<TReturn>> {
    return Promise.reject(new Error('StaticQuery cannot be run'));
  }

  preload(): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    throw new Error('StaticQuery cannot be preloaded');
  }
}
