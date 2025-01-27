import type {AST} from '../../../../zero-protocol/src/ast.js';
import {MemoryStorage} from '../../ivm/memory-storage.js';
import {createSource} from '../../ivm/test/source-factory.js';
import type {Source} from '../../ivm/source.js';
import type {
  CommitListener,
  GotCallback,
  QueryDelegate,
} from '../query-impl.js';
import {
  commentSchema,
  issueLabelSchema,
  issueSchema,
  labelSchema,
  revisionSchema,
  userSchema,
} from './test-schemas.js';

export class QueryDelegateImpl implements QueryDelegate {
  readonly #sources: Record<string, Source> = makeSources();
  readonly #commitListeners: Set<CommitListener> = new Set();

  readonly addedServerQueries: AST[] = [];
  readonly gotCallbacks: (GotCallback | undefined)[] = [];
  synchronouslyCallNextGotCallback = false;

  constructor(sources?: Record<string, Source>) {
    this.#sources = sources ?? makeSources();
  }

  batchViewUpdates<T>(applyViewUpdates: () => T): T {
    return applyViewUpdates();
  }

  onTransactionCommit(listener: CommitListener): () => void {
    this.#commitListeners.add(listener);
    return () => {
      this.#commitListeners.delete(listener);
    };
  }

  commit() {
    for (const listener of this.#commitListeners) {
      listener();
    }
  }
  addServerQuery(ast: AST, gotCallback?: GotCallback | undefined): () => void {
    this.addedServerQueries.push(ast);
    this.gotCallbacks.push(gotCallback);
    if (this.synchronouslyCallNextGotCallback) {
      this.synchronouslyCallNextGotCallback = false;
      gotCallback?.(true);
    }
    return () => {};
  }
  getSource(name: string): Source {
    return this.#sources[name];
  }
  createStorage() {
    return new MemoryStorage();
  }
}

const logConfig = {
  traceFetch: false,
  tracePush: false,
};

function makeSources() {
  const {user, issue, comment, revision, label, issueLabel} = {
    user: userSchema,
    issue: issueSchema,
    comment: commentSchema,
    revision: revisionSchema,
    label: labelSchema,
    issueLabel: issueLabelSchema,
  };

  return {
    user: createSource(logConfig, 'user', user.columns, user.primaryKey),
    issue: createSource(logConfig, 'issue', issue.columns, issue.primaryKey),
    comment: createSource(
      logConfig,
      'comment',
      comment.columns,
      comment.primaryKey,
    ),
    revision: createSource(
      logConfig,
      'revision',
      revision.columns,
      revision.primaryKey,
    ),
    label: createSource(logConfig, 'label', label.columns, label.primaryKey),
    issueLabel: createSource(
      logConfig,
      'issueLabel',
      issueLabel.columns,
      issueLabel.primaryKey,
    ),
  };
}
