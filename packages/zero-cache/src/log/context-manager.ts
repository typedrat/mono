import {AsyncLocalStorage} from 'node:async_hooks';
import type {MaybePromise} from '../../../shared/src/types.ts';

type ContextValues = Record<string, unknown>;

/**
 * Simplify managing log context so it
 * is available without threading it through
 * a callstack.
 *
 * E.g.,
 *
 * ```ts
 * app.get('/foo', (req, res) => {
 *   withContext({userId: req.user.id}, () => {
 *      someFn();
 *   });
 * });
 *
 * // someFn can be arbitrarily nested
 * // and still access the context.
 * function someFn() {
 *  log(`User ID: ${getContext().userId}`);
 * }
 * ```
 */
export class ContextManager {
  #storage: AsyncLocalStorage<ContextValues>;

  constructor() {
    this.#storage = new AsyncLocalStorage();
  }

  /**
   * Run a callback with the provided context values.
   * If there's an existing context, merges with it.
   */
  #run<T>(values: ContextValues, callback: () => T): T {
    const currentStore = this.#storage.getStore();
    const mergedStore = currentStore ? {...currentStore, ...values} : values;

    return this.#storage.run(mergedStore, callback);
  }

  /**
   * Get the entire current context store.
   */
  getContext = (): ContextValues | undefined => this.#storage.getStore();

  /**
   * Run an async function with the provided context values.
   * Merges with existing context if present.
   */
  withContext = <T>(
    values: ContextValues,
    fn: () => MaybePromise<T>,
  ): Promise<T> =>
    new Promise<T>((resolve, reject) => {
      this.#run(values, () => {
        Promise.resolve(fn()).then(resolve).catch(reject);
      });
    });
}

const contextManager = new ContextManager();

export const {getContext, withContext} = contextManager;
