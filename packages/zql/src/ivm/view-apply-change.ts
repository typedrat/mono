import {
  assert,
  assertArray,
  assertObject,
  assertUndefined,
  unreachable,
} from '../../../shared/src/asserts.js';
import {must} from '../../../shared/src/must.js';
import type {Row, Value} from '../../../zero-protocol/src/data.js';
import type {Change} from './change.js';
import type {Comparator, Node} from './data.js';
import type {SourceSchema} from './schema.js';
import type {Entry, EntryList, Format, View} from './view.js';

interface Delegate {
  setProperty(entry: Entry, key: string, value: Value | View): Entry;
  toSpliced<T>(
    list: readonly T[],
    start: number,
    deleteCount: number,
    ...items: T[]
  ): T[];
}

export function applyChange(
  parentEntry: Entry,
  change: Change,
  schema: SourceSchema,
  relationship: string,
  format: Format,
  delegate: Delegate,
): Entry {
  if (schema.isHidden) {
    switch (change.type) {
      case 'add':
      case 'remove':
        for (const [relationship, children] of Object.entries(
          change.node.relationships,
        )) {
          const childSchema = must(schema.relationships[relationship]);
          for (const node of children) {
            parentEntry = applyChange(
              parentEntry,
              {type: change.type, node},
              childSchema,
              relationship,
              format,
            );
          }
        }
        return parentEntry;
      case 'edit':
        // If hidden at this level it means that the hidden row was changed. If
        // the row was changed in such a way that it would change the
        // relationships then the edit would have been split into remove and
        // add.
        return parentEntry;
      case 'child': {
        const childSchema = must(
          schema.relationships[change.child.relationshipName],
        );
        return applyChange(
          parentEntry,
          change.child.change,
          childSchema,
          relationship,
          format,
        );
      }
      default:
        unreachable(change);
    }
  }

  const {singular, relationships: childFormats} = format;
  switch (change.type) {
    case 'add': {
      const entry = change.node.row;
      if (singular) {
        assertUndefined(
          parentEntry[relationship],
          'single output already exists',
        );

        let newEntry = entry;

        for (const [relationship, children] of Object.entries(
          change.node.relationships,
        )) {
          // TODO: Is there a flag to make TypeScript complain that dictionary access might be undefined?
          const childSchema = must(schema.relationships[relationship]);
          const childFormat = childFormats[relationship];
          if (childFormat === undefined) {
            continue;
          }

          const newView = childFormat.singular ? undefined : ([] as EntryList);
          newEntry = delegate.setProperty(newEntry, relationship, newView);
          // newEntry = {...newEntry, [relationship]: newView};

          for (const node of children) {
            newEntry = applyChange(
              newEntry,
              {type: 'add', node},
              childSchema,
              relationship,
              childFormat,
              delegate,
            );
          }
        }
        return delegate.setProperty(parentEntry, relationship, newEntry);
      }

      const view = getChildEntryList(parentEntry, relationship);
      const {pos, found} = binarySearch(view, entry, schema.compareRows);
      assert(!found, 'node already exists');

      let newEntry: Entry = entry;
      for (const [relationship, children] of Object.entries(
        change.node.relationships,
      )) {
        // TODO: Is there a flag to make TypeScript complain that dictionary access might be undefined?
        const childSchema = must(schema.relationships[relationship]);
        const childFormat = childFormats[relationship];
        if (childFormat === undefined) {
          continue;
        }

        const newView = childFormat.singular ? undefined : ([] as EntryList);
        newEntry = delegate.setProperty(newEntry, relationship, newView);
        for (const node of children) {
          newEntry = applyChange(
            newEntry,
            {type: 'add', node},
            childSchema,
            relationship,
            childFormat,
            delegate,
          );
        }
      }

      return delegate.setProperty(
        parentEntry,
        relationship,
        delegate.toSpliced(view, pos, 0, newEntry),
      );
    }
    case 'remove': {
      if (singular) {
        assertObject(parentEntry[relationship]);
        parentEntry = {
          ...parentEntry,
          [relationship]: undefined,
        };
      } else {
        const view = getChildEntryList(parentEntry, relationship);
        const {pos, found} = binarySearch(
          view,
          change.node.row,
          schema.compareRows,
        );
        assert(found, 'node does not exist');
        parentEntry = {
          ...parentEntry,
          [relationship]: view.toSpliced(pos, 1),
        };
      }
      // Needed to ensure cleanup of operator state is fully done.
      drainStreams(change.node);
      return parentEntry;
    }

    case 'child': {
      const childSchema = must(
        schema.relationships[change.child.relationshipName],
      );
      const childFormat = format.relationships[change.child.relationshipName];
      if (childFormat !== undefined) {
        if (singular) {
          assertObject(parentEntry[relationship]);
          const existingChild: Entry = parentEntry[relationship];

          const newChild = applyChange(
            existingChild,
            change.child.change,
            childSchema,
            change.child.relationshipName,
            childFormat,
            delegate,
          );
          return {
            ...parentEntry,
            [relationship]: newChild,
          };
        }

        const view = getChildEntryList(parentEntry, relationship);
        const {pos, found} = binarySearch(view, change.row, schema.compareRows);
        assert(found, 'node does not exist');
        const existingChild = view[pos];

        const newChild = applyChange(
          existingChild,
          change.child.change,
          childSchema,
          change.child.relationshipName,
          childFormat,
          delegate,
        );
        return delegate.setProperty(
          parentEntry,
          relationship,
          delegate.toSpliced(view, pos, 1, newChild),
        );
      }
      return parentEntry;
    }
    case 'edit': {
      if (singular) {
        assertObject(parentEntry[relationship]);
        return {
          ...parentEntry,
          [relationship]: {
            ...parentEntry[relationship],
            ...change.node.row,
          },
        };
      }

      const view = getChildEntryList(parentEntry, relationship);
      // If the order changed due to the edit, we need to remove and reinsert.
      if (schema.compareRows(change.oldNode.row, change.node.row) === 0) {
        const {pos, found} = binarySearch(
          view,
          change.oldNode.row,
          schema.compareRows,
        );
        assert(found, 'node does not exists');
        return {
          ...parentEntry,
          [relationship]: view.with(
            pos,
            makeNewEntryPreserveRelationships(
              change.node.row,
              view[pos],
              schema.relationships,
            ),
          ),
        };
      }

      // Remove
      const {pos, found} = binarySearch(
        view,
        change.oldNode.row,
        schema.compareRows,
      );
      assert(found, 'node does not exists');
      const oldEntry = view[pos];
      const newView = view.toSpliced(pos, 1);

      // Insert
      {
        const {pos, found} = binarySearch(
          newView,
          change.node.row,
          schema.compareRows,
        );
        assert(!found, 'node already exists');
        newView.splice(
          pos,
          0,
          makeNewEntryPreserveRelationships(
            change.node.row,
            oldEntry,
            schema.relationships,
          ),
        );
      }

      return delegate.setProperty(parentEntry, relationship, newView);
    }
    default:
      unreachable(change);
  }
}

// TODO: Do not return an object. It puts unnecessary pressure on the GC.
function binarySearch(view: EntryList, target: Entry, comparator: Comparator) {
  let low = 0;
  let high = view.length - 1;
  while (low <= high) {
    const mid = (low + high) >>> 1;
    const comparison = comparator(view[mid] as Row, target as Row);
    if (comparison < 0) {
      low = mid + 1;
    } else if (comparison > 0) {
      high = mid - 1;
    } else {
      return {pos: mid, found: true};
    }
  }
  return {pos: low, found: false};
}

function makeNewEntryPreserveRelationships(
  row: Row,
  entry: Entry,
  relationships: {[key: string]: SourceSchema},
): Entry {
  let result: Entry | undefined;
  for (const relationship in relationships) {
    if (!result) {
      result = {...row};
    }
    assert(!(relationship in row), 'Relationship already exists');
    result[relationship] = entry[relationship];
  }
  return result ?? row;
}

function drainStreams(node: Node) {
  for (const stream of Object.values(node.relationships)) {
    for (const node of stream) {
      drainStreams(node);
    }
  }
}

function getChildEntryList(
  parentEntry: Entry,
  relationship: string,
): EntryList {
  const view = parentEntry[relationship] as unknown;
  assertArray(view);
  return view as EntryList;
}
