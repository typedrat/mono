import {useMemo, useSyncExternalStore} from 'react';
import {deepClone} from '../../shared/src/deep-clone.ts';
import type {Immutable} from '../../shared/src/immutable.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {AdvancedQuery} from '../../zql/src/query/query-internal.ts';
import type {HumanReadable, Query} from '../../zql/src/query/query.ts';
import type {ResultType, TypedView} from '../../zql/src/query/typed-view.ts';
import {useZero} from './use-zero.tsx';

export type QueryResultDetails = Readonly<{
  type: ResultType;
}>;

export type QueryResult<TReturn> = readonly [
  HumanReadable<TReturn>,
  QueryResultDetails,
];

export function useQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  q: Query<TSchema, TTable, TReturn>,
  enableOrOptions:
    | boolean
    | {
        enable?: boolean | undefined;
        serverSnapshot: TReturn | undefined;
      }
    | undefined = true,
): QueryResult<TReturn> {
  const z = useZero();
  const enable =
    typeof enableOrOptions === 'boolean'
      ? enableOrOptions
      : enableOrOptions.enable ?? false;
  const options =
    typeof enableOrOptions === 'boolean' ? undefined : enableOrOptions;
  const serverSnapshot = options?.serverSnapshot;
  const view = viewStore.getView(
    z.clientID,
    q as AdvancedQuery<TSchema, TTable, TReturn>,
    enable && z.server !== null,
    serverSnapshot !== undefined,
  );
  const ss = useMemo(
    () =>
      [
        serverSnapshot as unknown as HumanReadable<TReturn>,
        {type: 'complete'},
      ] as const,
    [serverSnapshot],
  );
  // https://react.dev/reference/react/useSyncExternalStore
  return useSyncExternalStore(
    view.subscribeReactInternals,
    view.getSnapshot,
    serverSnapshot ? () => ss : undefined,
  );
}

const emptyArray: unknown[] = [];
const disabledSubscriber = () => () => {};

const resultTypeUnknown = {type: 'unknown'} as const;
const resultTypeComplete = {type: 'complete'} as const;

const emptySnapshotSingularUnknown = [undefined, resultTypeUnknown] as const;
const emptySnapshotSingularComplete = [undefined, resultTypeComplete] as const;
const emptySnapshotPluralUnknown = [emptyArray, resultTypeUnknown] as const;
const emptySnapshotPluralComplete = [emptyArray, resultTypeComplete] as const;

function getDefaultSnapshot<TReturn>(singular: boolean): QueryResult<TReturn> {
  return (
    singular ? emptySnapshotSingularUnknown : emptySnapshotPluralUnknown
  ) as QueryResult<TReturn>;
}

/**
 * Returns a new snapshot or one of the empty predefined ones. Returning the
 * predefined ones is important to prevent unnecessary re-renders in React.
 */
function getSnapshot<TReturn>(
  singular: boolean,
  data: HumanReadable<TReturn>,
  resultType: string,
): QueryResult<TReturn> {
  if (singular && data === undefined) {
    return (resultType === 'complete'
      ? emptySnapshotSingularComplete
      : emptySnapshotSingularUnknown) as unknown as QueryResult<TReturn>;
  }

  if (!singular && (data as unknown[]).length === 0) {
    return (
      resultType === 'complete'
        ? emptySnapshotPluralComplete
        : emptySnapshotPluralUnknown
    ) as QueryResult<TReturn>;
  }

  return [
    data,
    resultType === 'complete' ? resultTypeComplete : resultTypeUnknown,
  ];
}

declare const TESTING: boolean;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ViewWrapperAny = ViewWrapper<any, any, any>;

const allViews = new WeakMap<ViewStore, Map<string, ViewWrapperAny>>();

export function getAllViewsSizeForTesting(store: ViewStore): number {
  if (TESTING) {
    return allViews.get(store)?.size ?? 0;
  }
  return 0;
}

/**
 * A global store of all active views.
 *
 * React subscribes and unsubscribes to these views
 * via `useSyncExternalStore`.
 *
 * Managing views through `useEffect` or `useLayoutEffect` causes
 * inconsistencies because effects run after render.
 *
 * For example, if useQuery used use*Effect in the component below:
 * ```ts
 * function Foo({issueID}) {
 *   const issue = useQuery(z.query.issue.where('id', issueID).one());
 *   if (issue?.id !== undefined && issue.id !== issueID) {
 *     console.log('MISMATCH!', issue.id, issueID);
 *   }
 * }
 * ```
 *
 * `MISMATCH` will be printed whenever the `issueID` prop changes.
 *
 * This is because the component will render once with
 * the old state returned from `useQuery`. Then the effect inside
 * `useQuery` will run. The component will render again with the new
 * state. This inconsistent transition can cause unexpected results.
 *
 * Emulating `useEffect` via `useState` and `if` causes resource leaks.
 * That is:
 *
 * ```ts
 * function useQuery(q) {
 *   const [oldHash, setOldHash] = useState();
 *   if (hash(q) !== oldHash) {
 *      // make new view
 *   }
 *
 *   useEffect(() => {
 *     return () => view.destroy();
 *   }, []);
 * }
 * ```
 *
 * I'm not sure why but in strict mode the cleanup function
 * fails to be called for the first instance of the view and only
 * cleans up later instances.
 *
 * Swapping `useState` to `useRef` has similar problems.
 */
export class ViewStore {
  #views = new Map<string, ViewWrapperAny>();

  constructor() {
    if (TESTING) {
      allViews.set(this, this.#views);
    }
  }

  getView<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    clientID: string,
    query: AdvancedQuery<TSchema, TTable, TReturn>,
    enabled: boolean,
    requireComplete: boolean,
  ): {
    getSnapshot: () => QueryResult<TReturn>;
    subscribeReactInternals: (internals: () => void) => () => void;
  } {
    if (!enabled) {
      return {
        getSnapshot: () => getDefaultSnapshot(query.format.singular),
        subscribeReactInternals: disabledSubscriber,
      };
    }

    const hash = query.hash() + clientID;
    let existing = this.#views.get(hash);
    if (!existing) {
      existing = new ViewWrapper(
        query,
        view => {
          const lastView = this.#views.get(hash);
          // I don't think this can happen
          // but lets guard against it so we don't
          // leak resources.
          if (lastView && lastView !== view) {
            throw new Error('View already exists');
          }
          this.#views.set(hash, view);
        },
        () => {
          this.#views.delete(hash);
        },
        requireComplete,
      ) as ViewWrapper<TSchema, TTable, TReturn>;
      this.#views.set(hash, existing);
    }
    return existing as ViewWrapper<TSchema, TTable, TReturn>;
  }
}

const viewStore = new ViewStore();

/**
 * This wraps and ref counts a view.
 *
 * The only signal we have from React as to whether or not it is
 * done with a view is when it calls `unsubscribe`.
 *
 * In non-strict-mode we can clean up the view as soon
 * as the listener count goes to 0.
 *
 * In strict-mode, the listener count will go to 0 then a
 * new listener for the same view is immediately added back.
 *
 * This is why the `onMaterialized` and `onDematerialized` callbacks exist --
 * they allow a view which React is still referencing to be added
 * back into the store when React re-subscribes to it.
 *
 * This wrapper also exists to deal with the various
 * `useSyncExternalStore` caveats that cause excessive
 * re-renders and materializations.
 *
 * See: https://react.dev/reference/react/useSyncExternalStore#caveats
 * Especially:
 * 1. The store snapshot returned by getSnapshot must be immutable. If the underlying store has mutable data, return a new immutable snapshot if the data has changed. Otherwise, return a cached last snapshot.
 * 2. If a different subscribe function is passed during a re-render, React will re-subscribe to the store using the newly passed subscribe function. You can prevent this by declaring subscribe outside the component.
 */
class ViewWrapper<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
> {
  #view: TypedView<HumanReadable<TReturn>> | undefined;
  readonly #onDematerialized;
  readonly #onMaterialized;
  readonly #query: AdvancedQuery<TSchema, TTable, TReturn>;
  #snapshot: QueryResult<TReturn>;
  #reactInternals: Set<() => void>;
  #requireComplete: boolean;

  constructor(
    query: AdvancedQuery<TSchema, TTable, TReturn>,
    onMaterialized: (view: ViewWrapper<TSchema, TTable, TReturn>) => void,
    onDematerialized: () => void,
    requireComplete: boolean,
  ) {
    this.#snapshot = getDefaultSnapshot(query.format.singular);
    this.#onMaterialized = onMaterialized;
    this.#onDematerialized = onDematerialized;
    this.#reactInternals = new Set();
    this.#query = query;
    this.#requireComplete = requireComplete;
    this.#materializeIfNeeded();
  }

  #onData = (
    snap: Immutable<HumanReadable<TReturn>>,
    resultType: ResultType,
  ) => {
    // We allow waiting for first compelete result in the case where there's a
    // server snapshot. We don't want to bounce back to an older result from
    // local store if there's a server snapshot.
    //
    // Note: when we have consistency, we can have a local complete result that
    // is still behind network. So in this case we'd have to add a new 'source'
    // or something that means whether it came from the server.
    if (this.#requireComplete && resultType !== 'complete') {
      return;
    }
    // Once the first complete result is received, we no longer want to ignore
    // other result types. Queries can bounce back to unknown for certain
    // changes and we want caller to receive those updates.
    this.#requireComplete = false;
    const data =
      snap === undefined
        ? snap
        : (deepClone(snap as ReadonlyJSONValue) as HumanReadable<TReturn>);
    this.#snapshot = getSnapshot(this.#query.format.singular, data, resultType);
    for (const internals of this.#reactInternals) {
      internals();
    }
  };

  #materializeIfNeeded = () => {
    if (this.#view) {
      return;
    }

    this.#view = this.#query.materialize();
    this.#view.addListener(this.#onData);

    this.#onMaterialized(this);
  };

  getSnapshot = () => this.#snapshot;

  subscribeReactInternals = (internals: () => void): (() => void) => {
    this.#reactInternals.add(internals);
    this.#materializeIfNeeded();
    return () => {
      this.#reactInternals.delete(internals);

      // only schedule a cleanup task if we have no listeners left
      if (this.#reactInternals.size === 0) {
        setTimeout(() => {
          // Someone re-registered a listener on this view before the timeout elapsed.
          // This happens often in strict-mode which forces a component
          // to mount, unmount, remount.
          if (this.#reactInternals.size > 0) {
            return;
          }
          // We already destroyed the view
          if (this.#view === undefined) {
            return;
          }
          this.#view?.destroy();
          this.#view = undefined;
          this.#onDematerialized();
        }, 10);
      }
    };
  };
}
