
//#nbts@code
type Field =
  | 'archived_at'
  | 'repeats'
  | 'ascendable_id'
  | 'grading_system'
  | 'date'
  | 'style'
  | 'steepness'
  | 'perceived_hardness'
  | 'provider'
  | 'vl_ascendable_id'
  | 'path'
  | 'user_id'
  | 'vl_user_id'
  | 'score'
  | 'eight_a_parent_id'
  | 'hold_color'
  | 'exposition'
  | 'parent_name'
  | 'comment'
  | 'shade'
  | 'eight_a_logbook'
  | 'project'
  | 'ascendable_type'
  | 'recommended'
  | 'created_at'
  | 'height'
  | 'protection'
  | 'updated_at'
  | 'ascendable_filter'
  | 'virtual'
  | 'eight_a_user_id'
  | 'ascendable_height'
  | 'ascendable_name'
  | 'eight_a_id'
  | 'difficulty'
  | 'safety_issues'
  | 'eight_a_ascendable_id'
  | 'type'
  | 'vl_parent_id'
  | 'parent_id'
  | 'tries'
  | 'repeat'
  | 'grade'
  | 'sits'
  | 'id'
  | 'rating'
  | 'features'
  | 'vl_id'
  | 'sub_type';

//#nbts@code
import { document } from 'jsr:@ry/jupyter-helper';
import pl from 'npm:nodejs-polars';
import * as Plot from 'npm:@observablehq/plot';
let { html } = Deno.jupyter;

//#nbts@code
let ascents = pl.readJSON('./data/ascents.jsonl', {
  inferSchemaLength: null,
  format: 'lines',
}) as pl.DataFrame<Record<Field, pl.Series>>;

let sortedAscents = ascents
  .getColumn('difficulty')
  .valueCounts()
  .sort('difficulty');

let ascentsBarPlot = Plot.plot({
  marks: [
    Plot.barY(sortedAscents.toRecords(), { x: 'difficulty', y: 'count' }),
  ],
  x: { type: 'band' },
  document,
});

html`<div style="display: flex">
  <div>${sortedAscents.toHTML()}</div>
  <div>${ascentsBarPlot}</div>
</div>`;

//#nbts@code
let boulders = pl.readJSON('./data/gym_boulders.jsonl', {
  inferSchemaLength: null,
  format: 'lines',
});
//#nbts@code
html`${boulders
  .getColumn('route_setter')
  .valueCounts()
  .sort('count', true)
  .toHTML()}`;
//#nbts@code
let detailedAscents = ascents.join(boulders, {
  leftOn: 'ascendable_id',
  rightOn: 'id',
});

//#nbts@code
let plot = Plot.plot({
  grid: true,
  width: 1000,
  height: 5000,
  marginRight: 120,
  marks: [
    Plot.frame(),
    Plot.barY(
      detailedAscents.toRecords(),
      Plot.groupX(
        { y: 'count' },
        {
          x: 'difficulty',
          fy: 'route_setter',
          sort: { fy: 'x', order: 'descending' },
        },
      ),
    ),
  ],
  x: { type: 'band' },
  document,
});
html`${plot}`;
