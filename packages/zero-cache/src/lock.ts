import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';

let id = 0;
export class Lock {
  #lockP: {promise: Promise<void>; name: string} | null = null;
  readonly #lc: LogContext | undefined;
  readonly #id: number;
  readonly #pollInterval: number;

  constructor(lc?: LogContext | undefined, pollInterval: number = 1000) {
    this.#id = id++;
    this.#pollInterval = pollInterval;
    this.#lc = lc
      ?.withContext('component', 'lock')
      ?.withContext('id', this.#id);
  }

  async lock(taskName: string): Promise<() => void> {
    const previous = this.#lockP;
    const {promise, resolve} = resolver();
    this.#lockP = {promise, name: taskName};

    let waiting = true;
    const acquisitionStack = new Error('Lock acquisition stack');

    const pollStatus = () => {
      setTimeout(() => {
        if (waiting) {
          this.#lc?.warn?.(
            'Lock is taking too long to resolve. It may be stuck. Waiting on:',
            previous?.name,
            acquisitionStack.stack,
          );
          pollStatus();
        }
      }, this.#pollInterval);
    };
    pollStatus();

    await previous?.promise;
    waiting = false;

    return resolve;
  }

  withLock<R>(f: () => R | Promise<R>, taskName: string = ''): Promise<R> {
    return run(this.lock(taskName), f);
  }
}

export class RWLock {
  private _lock = new Lock();
  private _writeP: Promise<void> | null = null;
  private _readP: Promise<void>[] = [];

  read(): Promise<() => void> {
    return this._lock.withLock(async () => {
      await this._writeP;
      const {promise, resolve} = resolver();
      this._readP.push(promise);
      return resolve;
    });
  }

  withRead<R>(f: () => R | Promise<R>): Promise<R> {
    return run(this.read(), f);
  }

  write(): Promise<() => void> {
    return this._lock.withLock(async () => {
      await this._writeP;
      await Promise.all(this._readP);
      const {promise, resolve} = resolver();
      this._writeP = promise;
      this._readP = [];
      return resolve;
    });
  }

  withWrite<R>(f: () => R | Promise<R>): Promise<R> {
    return run(this.write(), f);
  }
}

async function run<R>(
  p: Promise<() => void>,
  f: () => R | Promise<R>,
): Promise<R> {
  const release = await p;
  try {
    return await f();
  } finally {
    release();
  }
}
