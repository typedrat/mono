import {describe, test, expect} from 'vitest';
import {getContext, withContext} from './context-manager.ts';

describe('ContextManager', () => {
  describe('getContext', () => {
    test('returns undefined when no context is set', () => {
      expect(getContext()).toBeUndefined();
    });

    test('returns the current context when set', async () => {
      const context = {userId: '123'};
      await withContext(context, () => expect(getContext()).toEqual(context));
      expect(getContext()).toBeUndefined();
    });
  });

  describe('withContext', () => {
    test('sets context for sync functions', async () => {
      const context = {userId: '123'};
      await withContext(context, () => expect(getContext()).toEqual(context));
      expect(getContext()).toBeUndefined();
    });

    test('sets context for async functions', async () => {
      const context = {userId: '123'};
      await withContext(context, async () => {
        await Promise.resolve();
        expect(getContext()).toEqual(context);
      });
      expect(getContext()).toBeUndefined();
    });

    test('nested contexts inherit parent values', async () => {
      const parentContext = {userId: '123', role: 'admin'};
      const childContext = {role: 'user'};

      await withContext(parentContext, async () => {
        expect(getContext()).toEqual(parentContext);

        await withContext(childContext, () => {
          expect(getContext()).toEqual({
            userId: '123',
            role: 'user', // child context overrides parent
          });
        });

        // Parent context is preserved after child context
        expect(getContext()).toEqual(parentContext);
      });
    });

    test('context persists through async operations', async () => {
      const context = {userId: '123'};

      await withContext(context, async () => {
        // Simulate some async operations
        await new Promise(resolve => setTimeout(resolve, 10));
        expect(getContext()).toEqual(context);

        // Nested async operations
        await Promise.all([
          new Promise(resolve => setTimeout(resolve, 10)),
          new Promise(resolve => setTimeout(resolve, 10)),
        ]);
        expect(getContext()).toEqual(context);
      });
    });

    test('context is isolated between parallel operations', async () => {
      const context1 = {userId: '123'};
      const context2 = {userId: '456'};

      const results = await Promise.all([
        withContext(context1, async () => {
          await new Promise(resolve => setTimeout(resolve, 10));
          return getContext();
        }),
        withContext(context2, async () => {
          await new Promise(resolve => setTimeout(resolve, 10));
          return getContext();
        }),
      ]);

      expect(results[0]).toEqual(context1);
      expect(results[1]).toEqual(context2);
    });
  });
});
