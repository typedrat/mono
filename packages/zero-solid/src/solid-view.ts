import {createStore, type SetStoreFunction, type Store} from 'solid-js/store';
import {
  applyChange,
  type Change,
  type Entry,
  type Format,
  type Input,
  type Output,
  type Query,
  type QueryType,
  type Smash,
  type TableSchema,
  type View,
  type ViewFactory,
} from '../../zero-advanced/src/mod.js';
import type {ResultType} from '../../zql/src/query/typed-view.js';

export type QueryResultDetails = {
  readonly type: ResultType;
};

type State = [Entry, QueryResultDetails];

const complete = {type: 'complete'} as const;
const unknown = {type: 'unknown'} as const;

const delegate = {
  setProperty(entry: Entry, key: string, value: Entry[string]): Entry {
    entry[key] = value;
    return entry; //{...entry, [key]: value};
  },
  toSpliced<T>(
    list: readonly T[],
    start: number,
    deleteCount: number,
    ...items: T[]
  ): readonly T[] {
    (list as T[]).splice(start, deleteCount, ...items);
    return list;
    // return [
    //   ...list.slice(0, start),
    //   ...items,
    //   ...list.slice(start + deleteCount),
    // ];
  },
};

export class SolidView<V extends View> implements Output {
  readonly #input: Input;
  readonly #format: Format;
  readonly #onDestroy: () => void;

  #state: Store<State>;
  #setState: SetStoreFunction<State>;

  constructor(
    input: Input,
    format: Format = {singular: false, relationships: {}},
    onDestroy: () => void = () => {},
    queryComplete: true | Promise<true> = true,
  ) {
    this.#input = input;
    this.#format = format;
    this.#onDestroy = onDestroy;
    [this.#state, this.#setState] = createStore<State>([
      {'': format.singular ? undefined : []},
      queryComplete === true ? complete : unknown,
    ]);
    input.setOutput(this);

    this.#setState(state => {
      for (const node of input.fetch({})) {
        state[0] = applyChange(
          state[0],
          {type: 'add', node},
          input.getSchema(),
          '',
          this.#format,
          delegate,
        );
      }
      return [state[0], state[1]];
    });
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

  push(change: Change): void {
    this.#setState((state: State) => [
      applyChange(
        state[0],
        change,
        this.#input.getSchema(),
        '',
        this.#format,
        delegate,
      ),
      state[1],
    ]);
  }
}

export function solidViewFactory<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(
  _query: Query<TSchema, TReturn>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  _onTransactionCommit: (cb: () => void) => void,
  queryComplete: true | Promise<true>,
): SolidView<Smash<TReturn>> {
  return new SolidView<Smash<TReturn>>(input, format, onDestroy, queryComplete);
}

solidViewFactory satisfies ViewFactory<TableSchema, QueryType, unknown>;
