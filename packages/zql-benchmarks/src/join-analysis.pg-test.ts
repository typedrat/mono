// Is join over-fetching the connection for memory source and scanning the entire table?
// We can test by joining a thing that should only have 1 row.

// track -> playlist join should be fast. lets test that
import {avg, frameStats, log} from './shared.ts';
import {B} from 'mitata';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';
import {expect, test} from 'vitest';

const pgContent = await getChinook();
test('join analysis', async () => {
  const harness = await bootstrap({
    suiteName: 'pipeline_visit_analysis',
    zqlSchema: schema,
    pgContent,
  });
  const zql = harness.queries.memory;

  log`# Join Analysis

## Track -> Playlist Edge

Joining to a low cardinality row in a table should be fast
and only require a single row to be fetched.

There are only a few playlists that a track participates in.
This lookup should be quick. If it is not, we're scanning the entire
playlistTrack table for some reason.
`;

  const hydrateTracksJoinedToPlaylistEdge = await new B(
    'hydrate tracks joined to playlist',
    function* () {
      yield async () => {
        await zql.track.related('playlistEdge').limit(100);
      };
    },
  ).run();

  log`${frameStats(hydrateTracksJoinedToPlaylistEdge)}

## Track -> Album

This is our baseline. It should be similar in performance to the
playlist edge join.
`;

  const hydrateTracksJoinedToAlbum = await new B(
    'hydrate tracks joined to album',
    function* () {
      yield async () => {
        await zql.track.related('album').limit(100);
      };
    },
  ).run();

  log`${frameStats(hydrateTracksJoinedToAlbum)}`;

  expect(avg(hydrateTracksJoinedToPlaylistEdge)).toBeLessThan(
    avg(hydrateTracksJoinedToAlbum) * 1.5,
  );
});
