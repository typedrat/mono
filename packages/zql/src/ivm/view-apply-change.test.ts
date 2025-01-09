import {expect, suite, test} from 'vitest';
import type {Change} from './change.js';
import type {SourceSchema} from './schema.js';
import {applyChange} from './view-apply-change.js';
import type {Format} from './view.js';

suite('applyChange', () => {
  type Row = {id: number; s: string};
  const parentEntry = {children: []};
  const schema: SourceSchema = {
    tableName: 'table',
    columns: {
      id: {type: 'number'},
      s: {type: 'string'},
    },
    primaryKey: ['id'],
    relationships: {},
    isHidden: false,
    system: 'client',
    compareRows: (a, b) => (a.id as number) - (b.id as number),
    sort: [['id', 'asc']],
  };
  const relationship = 'children';
  const format: Format = {singular: false, relationships: {}};

  test('add keeps object identity', () => {
    const row1 = {id: 1, s: 'a'};
    const change: Change = {
      type: 'add',
      node: {row: row1, relationships: {}},
    };
    const newEntry = applyChange(
      parentEntry,
      change,
      schema,
      relationship,
      format,
    );
    expect((newEntry['children'] as Row[])[0]).toBe(row1);

    const row2 = {id: 2, s: 'b'};
    const change2: Change = {
      type: 'add',
      node: {row: row2, relationships: {}},
    };
    const newEntry2 = applyChange(
      newEntry,
      change2,
      schema,
      relationship,
      format,
    );
    expect((newEntry2['children'] as Row[])[1]).toBe(row2);
    expect((newEntry2['children'] as Row[])[0]).toBe(row1);
    expect(newEntry2).not.toBe(newEntry);
  });

  test('remove keeps object identity', () => {
    const row1 = {id: 1, s: 'a'};
    const change1: Change = {
      type: 'add',
      node: {row: row1, relationships: {}},
    };
    const row2 = {id: 2, s: 'b'};
    const change2: Change = {
      type: 'add',
      node: {row: row2, relationships: {}},
    };
    const change3: Change = {
      type: 'remove',
      node: {row: row1, relationships: {}},
    };

    let newEntry = applyChange(
      parentEntry,
      change1,
      schema,
      relationship,
      format,
    );
    newEntry = applyChange(newEntry, change2, schema, relationship, format);
    newEntry = applyChange(newEntry, change3, schema, relationship, format);
    expect((newEntry['children'] as Row[])[0]).toBe(row2);
  });

  test('edit keeps object identity', () => {
    const row1 = {id: 1, s: 'a'};
    const change1: Change = {
      type: 'add',
      node: {row: row1, relationships: {}},
    };
    const row2 = {id: 2, s: 'b'};
    const change2: Change = {
      type: 'add',
      node: {row: row2, relationships: {}},
    };
    const change3: Change = {
      type: 'edit',
      node: {row: {id: 1, s: 'a2'}, relationships: {}},
      oldNode: {row: row1, relationships: {}},
    };
    let newEntry = applyChange(
      parentEntry,
      change1,
      schema,
      relationship,
      format,
    );
    newEntry = applyChange(newEntry, change2, schema, relationship, format);
    newEntry = applyChange(newEntry, change3, schema, relationship, format);
    expect(newEntry).toMatchInlineSnapshot(`
      {
        "children": [
          {
            "id": 1,
            "s": "a2",
          },
          {
            "id": 2,
            "s": "b",
          },
        ],
      }
    `);
    expect((newEntry['children'] as Row[])[0]).toBe(change3.node.row);
    expect((newEntry['children'] as Row[])[1]).toBe(change2.node.row);
  });
});
