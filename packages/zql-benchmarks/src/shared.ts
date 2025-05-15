import type {trial} from 'mitata';
import {must} from '../../shared/src/must.ts';
import {expect, test} from 'vitest';
import type {Row} from '../../zero-protocol/src/data.ts';
import type {JSONValue} from '../../shared/src/json.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';

/* eslint-disable no-console */
export function frameStats(trial: trial) {
  const p99ms = p99(trial);
  const avgms = avg(trial);
  return `
  - Total time: ${stat(p99ms)}p99, ${stat(avgms)}avg
  - Frame budget: 16ms
  - FPS: ${stat(1 / (p99ms / 16))}p99, ${stat(1 / (avgms / 16))}avg`;
}

export function frameMaintStats(trial: trial) {
  const p99ms = p99(trial);
  const avgms = avg(trial);
  return `
  - Maintain queries over 1 write: ${stat(p99ms)}ms p99 ${stat(avgms)}ms avg.
  - Frame budget: 16ms
  - Writes per frame: ${stat(16 / p99ms, 10)}p99, ${stat(16 / avgms, 10)}avg`;
}

export function p99(trial: trial) {
  const stats = must(trial.runs[0].stats);
  return nanosToMs(stats.p99);
}

export function avg(trial: trial) {
  const stats = must(trial.runs[0].stats);
  return nanosToMs(stats.avg);
}

export function nanosToMs(nanos: number) {
  return nanos / 1_000_000;
}

test('noop', () => expect(true).toBe(true));

export function log(strings: TemplateStringsArray, ...values: unknown[]): void {
  let result = '';
  strings.forEach((string, i) => {
    result += string;
    if (i < values.length) {
      result += values[i];
    }
  });

  console.log(result);
}

export function stat(s: number, precision = 2) {
  return (
    '`' +
    s.toLocaleString('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: precision,
    }) +
    '`'
  );
}

export function makeEdit<TSchema extends Schema>(
  rawData: ReadonlyMap<keyof TSchema['tables'], readonly Row[]>,
) {
  const currentValues = new Map<string, Row>();
  return (table: string, index: number, column: string, value: JSONValue) => {
    const key = `${table}-${index}`;
    const dataset = must(rawData.get(table));
    const row = must(currentValues.get(key) ?? dataset[index]);
    const newRow = {
      ...row,
      [column]: value,
    };
    currentValues.set(key, newRow);
    return {
      type: 'edit',
      oldRow: row,
      row: newRow,
    } as const;
  };
}

export function makeMiddle<TSchema extends Schema>(
  raw: ReadonlyMap<keyof TSchema['tables'], readonly Row[]>,
) {
  return (table: keyof TSchema['tables']) => {
    const dataset = must(raw.get(table));
    return dataset[Math.floor(dataset.length / 2)];
  };
}
