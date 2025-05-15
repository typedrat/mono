import {frameMaintStats, log, makeEdit} from './shared.ts';
import {B} from 'mitata';
import {getChinook} from '../../zql-integration-tests/src/chinook/get-deps.ts';
import {bootstrap} from '../../zql-integration-tests/src/helpers/runner.ts';
import {schema} from '../../zql-integration-tests/src/chinook/schema.ts';

const pgContent = await getChinook();

const harness = await bootstrap({
  suiteName: 'pipeline_visit_analysis',
  zqlSchema: schema,
  pgContent,
});
const {raw} = harness.dbs;
const edit = makeEdit(raw);
const zql = harness.queries.memory;

log`# Pipeline Visits

How expensive is it to visit a pipeline?
If there is 1 query running, how many writes can we do?
And 100 queries?
1,000 queries?

In short, do we need to invest in short-circuiting the pipeline visit?`;

const singlePoint = await new B(
  'maintain 1 point query',
  makeMaintainPointQueriesTest(1),
).run();

log`
## Maintain 1 point query
${frameMaintStats(singlePoint)}`;

const hundredPoint = await new B(
  'maintain 100 point queries',
  makeMaintainPointQueriesTest(100),
).run();
log`
## Maintain 100 point queries
${frameMaintStats(hundredPoint)}`;

const thousandPoint = await new B(
  'maintain 1,000 point queries',
  makeMaintainPointQueriesTest(1000),
).run();
log`
## Maintain 1000 point queries
${frameMaintStats(thousandPoint)}`;

const tenThousandPoint = await new B(
  'maintain 10,000 point queries',
  makeMaintainPointQueriesTest(10000),
).run();
log`
## Maintain 10,000 point queries
${frameMaintStats(tenThousandPoint)}`;

function makeMaintainPointQueriesTest(numQueries: number) {
  return function* () {
    const views = Array.from({length: numQueries}, (_, i) =>
      zql.mediaType.where('id', i + 1).materialize(),
    );
    let count = 0;
    yield () => {
      harness.delegates.memory
        .getSource('mediaType')!
        .push(edit('mediaType', count % 5, 'name', `new name ${count}`));
      count++;
    };
    views.forEach(view => view.destroy());
  };
}

log`
# Result
Pretty much a linear slowdown. 1,000 point queries allows ~100 writes per frame. Seems bearable
at the moment.
`;
