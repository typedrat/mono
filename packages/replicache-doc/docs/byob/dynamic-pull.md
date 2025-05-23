---
title: Dynamic Pull
slug: /byob/dynamic-pull
---

Even though in the previous step we're making persistent changes in the database, we still aren't _serving_ that data in the pull endpoint – it's still static 🤣. Let's fix that now.

The implementation of pull will depend on the backend strategy you are using. For the [Global Version](/strategies/global-version) strategy we're using, the basics steps are:

- Open a transaction
- Read the latest global version from the database
- Build the response patch:
  - If the request cookie is null, this patch contains a `put` for each entity in the database that isn't deleted
  - Otherwise, this patch contains only entries that have been changed since the request cookie
- Build a map of changes to client `lastMutationID` values:
  - If the request cookie is null, this map contains an entry for every client in the requesting `clientGroup`
  - Otherwise, it contains only entries for clients that have changed since the request cookie
- Return the patch, the current global `version`, and the `lastMutationID` changes as a `PullResponse` struct

## Implement Pull

Replace the contents of `server/src/pull.ts` with this code:

```ts
import {serverID, tx, type Transaction} from './db';
import type {PatchOperation, PullResponse} from 'replicache';
import type {Request, Response, NextFunction} from 'express';

export async function handlePull(
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  try {
    const resp = await pull(req, res);
    res.json(resp);
  } catch (e) {
    next(e);
  }
}

async function pull(req: Request, res: Response) {
  const pull = req.body;
  console.log(`Processing pull`, JSON.stringify(pull));
  const {clientGroupID} = pull;
  const fromVersion = pull.cookie ?? 0;
  const t0 = Date.now();

  try {
    // Read all data in a single transaction so it's consistent.
    await tx(async t => {
      // Get current version.
      const {version: currentVersion} = await t.one<{version: number}>(
        'select version from replicache_server where id = $1',
        serverID,
      );

      if (fromVersion > currentVersion) {
        throw new Error(
          `fromVersion ${fromVersion} is from the future - aborting. This can happen in development if the server restarts. In that case, clear appliation data in browser and refresh.`,
        );
      }

      // Get lmids for requesting client groups.
      const lastMutationIDChanges = await getLastMutationIDChanges(
        t,
        clientGroupID,
        fromVersion,
      );

      // Get changed domain objects since requested version.
      const changed = await t.manyOrNone<{
        id: string;
        sender: string;
        content: string;
        ord: number;
        version: number;
        deleted: boolean;
      }>(
        'select id, sender, content, ord, version, deleted from message where version > $1',
        fromVersion,
      );

      // Build and return response.
      const patch: PatchOperation[] = [];
      for (const row of changed) {
        const {id, sender, content, ord, version: rowVersion, deleted} = row;
        if (deleted) {
          if (rowVersion > fromVersion) {
            patch.push({
              op: 'del',
              key: `message/${id}`,
            });
          }
        } else {
          patch.push({
            op: 'put',
            key: `message/${id}`,
            value: {
              from: sender,
              content,
              order: ord,
            },
          });
        }
      }

      const body: PullResponse = {
        lastMutationIDChanges: lastMutationIDChanges ?? {},
        cookie: currentVersion,
        patch,
      };
      res.json(body);
      res.end();
    });
  } catch (e) {
    console.error(e);
    res.status(500).send(e);
  } finally {
    console.log('Processed pull in', Date.now() - t0);
  }
}

async function getLastMutationIDChanges(
  t: Transaction,
  clientGroupID: string,
  fromVersion: number,
) {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const rows = await t.manyOrNone<{id: string; last_mutation_id: number}>(
    `select id, last_mutation_id
    from replicache_client
    where client_group_id = $1 and version > $2`,
    [clientGroupID, fromVersion],
  );
  return Object.fromEntries(rows.map(r => [r.id, r.last_mutation_id]));
}
```

Because the previous pull response was hard-coded and not really reading from the database, you'll now have to clear your browser's application data to see consistent results. On Chrome/OSX for example: **cmd+opt+j → Application tab -> Storage -> Clear site data**.

Once you do that, you can make a change in one browser and then refresh a different browser and see them round-trip:

Also notice that if we go offline for awhile, make some changes, then come back online, the mutations get sent when possible.

We don't have any conflicts in this simple data model, but Replicache makes it easy to reason about most conflicts. See the [How Replicache Works](/concepts/how-it-works) for more details.

The only thing left is to make it live — we obviously don't want the user to have to manually refresh to get new data 🙄.

## Next

The [next section](./poke.md) implements realtime updates.
