import {
  createStore,
  reconcile,
  type SetStoreFunction,
  type Store,
} from 'solid-js/store';
import {
  applyChange,
  type Change,
  type Entry,
  type Format,
  type HumanReadable,
  type Input,
  type Output,
  type Query,
  type ViewFactory,
} from '../../zero/src/advanced.ts';
import type {ResultType, Schema} from '../../zero/src/zero.ts';

export type QueryResultDetails = {
  readonly type: ResultType;
};

type State = [Entry, QueryResultDetails];

const complete = {type: 'complete'} as const;
const unknown = {type: 'unknown'} as const;

export class SolidView<V> implements Output {
  readonly #input: Input;
  readonly #format: Format;
  readonly #onDestroy: () => void;

  #draftState: State;
  #state: Store<State>;
  #setState: SetStoreFunction<State>;

  constructor(
    input: Input,
    onTransactionCommit: (cb: () => void) => void,
    format: Format,
    onDestroy: () => void,
    queryComplete: true | Promise<true>,
  ) {
    this.#input = input;
    onTransactionCommit(this.#onTransactionCommit);
    this.#format = format;
    this.#onDestroy = onDestroy;
    this.#draftState = [
      {'': format.singular ? undefined : []},
      queryComplete === true ? complete : unknown,
    ];
    [this.#state, this.#setState] = createStore<State>([
      {'': format.singular ? undefined : []},
      queryComplete === true ? complete : unknown,
    ]);
    input.setOutput(this);

    for (const node of input.fetch({})) {
      applyChange(
        this.#draftState[0],
        {
          type: 'add',
          node,
        },
        this.#input.getSchema(),
        '',
        this.#format,
      );
    }
    this.#setState(reconcile(this.#draftState));

    if (queryComplete !== true) {
      void queryComplete.then(() => {
        this.#setState(oldState => [oldState[0], complete]);
      });
    }
  }

  get data(): V {
    return this.#state[0][''] as V;
  }

  get resultDetails(): QueryResultDetails {
    return this.#state[1];
  }

  destroy(): void {
    this.#onDestroy();
  }

  #onTransactionCommit = () => {
    this.#setState(reconcile(this.#draftState));
  };

  push(change: Change): void {
    applyChange(
      this.#draftState[0],
      change,
      this.#input.getSchema(),
      '',
      this.#format,
    );
  }
}

export function solidViewFactory<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  _query: Query<TSchema, TTable, TReturn>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  onTransactionCommit: (cb: () => void) => void,
  queryComplete: true | Promise<true>,
) {
  return new SolidView<HumanReadable<TReturn>>(
    input,
    onTransactionCommit,
    format,
    onDestroy,
    queryComplete,
  );
}

solidViewFactory satisfies ViewFactory<Schema, string, unknown, unknown>;
