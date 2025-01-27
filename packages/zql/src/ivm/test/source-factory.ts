import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.js';
import type {SchemaValue} from '../../../../zero-schema/src/table-schema.js';
import type {LogConfig} from '../../log.ts';
import {MemorySource} from '../memory-source.js';
import type {Source} from '../source.js';

export type SourceFactory = (
  logConfig: LogConfig,
  tableName: string,
  columns: Record<string, SchemaValue>,
  primaryKey: PrimaryKey,
) => Source;

export const createSource: SourceFactory = (
  logConfig: LogConfig,
  tableName: string,
  columns: Record<string, SchemaValue>,
  primaryKey: PrimaryKey,
): Source => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const {sourceFactory} = globalThis as any;
  if (sourceFactory) {
    return sourceFactory(logConfig, tableName, columns, primaryKey);
  }

  return new MemorySource(tableName, columns, primaryKey);
};
