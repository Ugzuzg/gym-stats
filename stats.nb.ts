
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
let { html, md, display } = Deno.jupyter;

//#nbts@code
let ascents = pl.readJSON('./data/ascents.jsonl', {
  inferSchemaLength: null,
  format: 'lines',
}) as pl.DataFrame<Record<Field, pl.Series>>;
ascents = ascents
  //.filter(pl.col('difficulty').gtEq(pl.lit('6C')))
  .withColumns(
    pl.col('created_at').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S %z').cast(pl.Datetime('ms')),
    pl.col('updated_at').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S %z').cast(pl.Datetime('ms')),
  )
  .withColumns(pl.col('created_at').date.year().alias('year'))
  .withColumns(pl.col('created_at').date.day().alias('date'));

//#nbts@code
let boulders = pl
  .readJSON('./data/gym_boulders.jsonl', {
    inferSchemaLength: null,
    format: 'lines',
  })
  .withColumn(pl.col('route_card_label').str.slice(0, 3));

//#nbts@code
let detailedAscents = ascents.join(boulders, {
  leftOn: 'ascendable_id',
  rightOn: 'id',
});

//#nbts@code
let boulderScores = [
  { score: 12, v: 'VB', font: '2' },
  { score: 18, v: 'VB', font: '2+' },
  { score: 24, v: 'VB+', font: '3' },
  { score: 30, v: 'VB+', font: '3+' },
  { score: 36, v: 'V0-', font: '4' },
  { score: 42, v: 'V0', font: '4+' },
  { score: 48, v: 'V1', font: '5' },
  { score: 54, v: 'V2', font: '5+' },
  { score: 60, v: 'V3', font: '6a' },
  { score: 66, v: 'V3', font: '6a+' },
  { score: 72, v: 'V4', font: '6b' },
  { score: 78, v: 'V4', font: '6b+' },
  { score: 84, v: 'V5', font: '6c' },
  { score: 90, v: 'V5', font: '6c+' },
  { score: 96, v: 'V6', font: '7a' },
  { score: 102, v: 'V7', font: '7a+' },
  { score: 108, v: 'V7', font: '7b' },
  { score: 114, v: 'V8', font: '7b+' },
  { score: 120, v: 'V9', font: '7c' },
  { score: 126, v: 'V10', font: '7c+' },
].map((grade) => ({ ...grade, font: grade.font.toUpperCase() }));
let gradeToNumber = boulderScores.reduce((acc, grade) => {
  acc[grade.font] = grade.score;
  return acc;
}, {});

gradeToNumber;

//#nbts@code
detailedAscents = detailedAscents.withColumns(
  pl.col('difficulty').replace({ old: gradeToNumber }).cast(pl.Float32).alias('numberDifficulty'),
  pl.col('type').replace(['f', 'rp'], ['flash', 'redpoint']),
);

//#nbts@code
detailedAscents.toRecords()[0];

//#nbts@code
html`${detailedAscents.getColumn('route_setter').valueCounts().sort('count', true).toHTML()}`;

//#nbts@code
let baseWidth = 640;

//#nbts@code
let groupings = [
  {
    name: 'Everything',
    x: 'difficulty',
    fill: 'difficulty',
  },
  {
    name: 'By year',
    x: 'difficulty',
    fy: 'year',
    fill: 'difficulty',
  },
  {
    name: 'By type',
    x: 'difficulty',
    fy: 'type',
    fill: 'difficulty',
  },
  {
    name: 'By year and type',
    x: 'difficulty',
    fx: 'year',
    fy: 'type',
    fill: 'difficulty',
  },
];
for (const grouping of groupings) {
  await display(md`## ${grouping.name}`);
  await display(
    Plot.plot({
      grid: true,
      width: grouping.fx ? baseWidth * 2 : baseWidth,
      color: { legend: true },
      marks: [Plot.frame(), Plot.barY(detailedAscents.toRecords(), Plot.groupX({ y: 'count' }, grouping))],
      x: { type: 'band' },
      document,
    }),
  );
}

//#nbts@code
let sorted = detailedAscents
  .sort(pl.col('created_at'))
  .withColumns(
    pl.col('numberDifficulty').min().over(pl.col('created_at').date.strftime('%Y-%m'), 'type').alias('weekDiffMin'),
    pl.col('numberDifficulty').max().over(pl.col('created_at').date.strftime('%Y-%m'), 'type').alias('weekDiffMax'),
  )
  .toRecords();
let plot = Plot.plot({
  grid: true,
  color: { legend: true, type: 'categorical', domain: ['flash', 'redpoint'], range: ['#d8e135', '#dd4011'] },
  y: {
    ticks: boulderScores.filter((grade) => grade.font <= '7B').map((grade) => grade.score),
    tickFormat: (v) => boulderScores.find(({ score }) => v === score)?.font,
  },
  marks: [
    Plot.frame(),
    ...['redpoint', 'flash'].map((type) =>
      Plot.rectY(
        sorted.filter((v) => v.type === type),
        { interval: 'month', x: 'created_at', y1: 'weekDiffMin', y2: 'weekDiffMax', fill: 'type' },
      ),
    ),
  ],
  document,
});

await display(
  md`
## Poor man's CPR
  `,
);
await display(plot);
