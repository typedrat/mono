import {describe, expect, test} from 'vitest';
import type {ServerColumnSchema} from './schema.ts';
import {
  formatPg,
  formatPgInternalConvert,
  sql,
  sqlConvertColumnArg,
  sqlConvertSingularLiteralArg,
} from './sql.ts';

test('identical values result in a single placeholder', () => {
  const userId = 1;
  expect(
    formatPg(
      sql`SELECT * FROM "user" WHERE "id" = ${userId} AND "id" = ${userId}`,
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT * FROM "user" WHERE "id" = $1 AND "id" = $1",
      "values": [
        1,
      ],
    }
  `);

  const str = JSON.stringify({a: 1});
  expect(
    formatPg(
      sql`SELECT * FROM "user" WHERE "meta1" = ${str} AND "id" = ${userId} OR "meta2" = ${str} OR "otherId" = ${userId} AND foo = ${''}`,
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT * FROM "user" WHERE "meta1" = $1 AND "id" = $2 OR "meta2" = $1 OR "otherId" = $2 AND foo = $3",
      "values": [
        "{"a":1}",
        1,
        "",
      ],
    }
  `);
});

describe('string arg packing', () => {
  test('single arg', () => {
    expect(
      formatPgInternalConvert(
        sql`SELECT * FROM "user" WHERE "id" = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'numeric',
          },
          1,
          false,
          true,
        )} `,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "user" WHERE "id" = $1::text::double precision",
        "values": [
          "1",
        ],
      }
    `);
  });

  // identical values should only be encoded once
  test('many equivalent args', () => {
    expect(
      formatPgInternalConvert(
        sql`SELECT * FROM "user" WHERE "id" = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'numeric',
          },
          1,
          false,
          true,
        )} OR "other_id" = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'numeric',
          },
          1,
          false,
          true,
        )}`,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "user" WHERE "id" = $1::text::double precision OR "other_id" = $1::text::double precision",
        "values": [
          "1",
        ],
      }
    `);
  });

  test('many types', () => {
    expect(
      formatPgInternalConvert(
        sql`SELECT * FROM "foo" WHERE "jsonb" = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'jsonb',
          },
          {},
          false,
          true,
        )} OR "numeric" = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'numeric',
          },
          1,
          false,
          true,
        )}
        OR "str" = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'text',
          },
          'str',
          false,
          true,
        )} OR "boolean" = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'boolean',
          },
          true,
          false,
          true,
        )} OR "uuid"::text = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'uuid',
          },
          '8f1dceb2-b3dd-46cf-9deb-460e9d87541c',
          false,
          true,
        )} OR "enum"::text = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: true,
            type: 'some_enum',
          },
          'ENUM_KEY',
          false,
          true,
        )} OR "timestamp" = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'timestamp',
          },
          'abc',
          false,
          true,
        )} OR "timestampz" = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'timestamptz',
          },
          'abc',
          false,
          true,
        )}`,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "jsonb" = $1::text::jsonb OR "numeric" = $2::text::double precision
              OR "str" = $3::text COLLATE "ucs_basic" OR "boolean" = $4::text::boolean OR "uuid"::text = $5::text COLLATE "ucs_basic" OR "enum"::text = $6::text COLLATE "ucs_basic" OR "timestamp" = to_timestamp($7::text::bigint / 1000.0) AT TIME ZONE 'UTC' OR "timestampz" = to_timestamp($7::text::bigint / 1000.0)",
        "values": [
          "{}",
          "1",
          "str",
          "true",
          "8f1dceb2-b3dd-46cf-9deb-460e9d87541c",
          "ENUM_KEY",
          ""abc"",
        ],
      }
    `);
  });

  test('mapped and joined', () => {
    const values = [1, 1.1, 'two', true, null];
    expect(
      formatPgInternalConvert(
        sql`SELECT * FROM "foo" WHERE ${sql.join(
          values.map(v => sqlConvertSingularLiteralArg(v)),
          ' AND ',
        )} `,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE $1::text::double precision AND $2::text::double precision AND $3::text::text COLLATE "ucs_basic" AND $4::text::boolean AND $5",
        "values": [
          "1",
          "1.1",
          "two",
          "true",
          null,
        ],
      }
    `);
  });

  test('insert', () => {
    const values: [ServerColumnSchema, unknown][] = [
      [
        {
          isArray: false,
          isEnum: false,
          type: 'numeric',
        },
        1,
      ],
      [
        {
          isArray: false,
          isEnum: false,
          type: 'numeric',
        },
        1.1,
      ],
      // This MUST NOT insert with a COLLATION
      [
        {
          isArray: false,
          isEnum: false,
          type: 'text',
        },
        'two',
      ],
      [
        {
          isArray: false,
          isEnum: false,
          type: 'boolean',
        },
        true,
      ],
      [
        {
          isArray: true,
          isEnum: false,
          type: 'numeric',
        },
        [1, 2, 3],
      ],
    ];
    expect(
      formatPgInternalConvert(
        sql`INSERT INTO "foo" VALUES (${sql.join(
          values.map(([schema, v]) =>
            sqlConvertColumnArg(schema, v, false, false),
          ),
          ', ',
        )})`,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "INSERT INTO "foo" VALUES ($1::text::numeric, $2::text::numeric, $3::text::text, $4::text::boolean, ARRAY(
                SELECT value::text::numeric FROM jsonb_array_elements_text($5::text::jsonb)
              ))",
        "values": [
          "1",
          "1.1",
          "two",
          "true",
          "[1,2,3]",
        ],
      }
    `);
  });
});
