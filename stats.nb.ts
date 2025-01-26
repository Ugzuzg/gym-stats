
//#nbts@code
import { document } from 'jsr:@ry/jupyter-helper';
import pl from 'npm:nodejs-polars';
import * as Plot from 'npm:@observablehq/plot';
let { html, md, display } = Deno.jupyter;

//#nbts@code
type Seriesify<T extends Record<string, pl.DataType>> = {
  [key in keyof T]: key extends string ? pl.Series<T[key], key> : pl.Series<T[key]>;
};

//#nbts@code
type Ascent = {
  created_at: pl.Datetime;
  ascendable_id: pl.Int64;
  ascendable_name: pl.String;
  ascendable_type: pl.Categorical; //'GymBoulder';
  grading_system: pl.Categorical; //'font';
  grade: pl.String;
  difficulty: pl.String;
  type: pl.Categorical; // 'rp' | 'f';
  sub_type: pl.String;
  tries: pl.Int64;
};

let ascents = pl.readJSON('./data/ascents.jsonl', {
  inferSchemaLength: null,
  format: 'lines',
}) as pl.DataFrame<Seriesify<Ascent>>;

ascents = ascents
  .withColumns(
    ascents.getColumn('created_at').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S %z').cast(pl.Datetime('ms')),
    ascents.getColumn('updated_at').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S %z').cast(pl.Datetime('ms')),
    ascents.getColumn('ascendable_type').cast(pl.Categorical),
    ascents.getColumn('grading_system').cast(pl.Categorical),
  )
  .withColumns(pl.col('created_at').date.year().alias('year'))
  // remove repeat ascents
  .unique({ subset: 'ascendable_id', keep: 'first' });

//#nbts@code
type Boulder = {
  id: pl.Int64;
  route_setter: pl.String;
  sector_name: pl.String;
  gym_wall_name: pl.String;
  set_at: pl.Datetime;
  route_card_label: pl.String;
};

let boulders = pl
  .readJSON('./data/gym_boulders.jsonl', {
    inferSchemaLength: null,
    format: 'lines',
  })
  .withColumns(
    pl.col('route_card_label').str.slice(0, 3),
    pl.col('set_at').str.strptime(pl.Datetime, '%Y-%m-%d %H:%M:%S %z').cast(pl.Datetime('ms')),
    pl.col('id').alias('nid'),
  ) as pl.DataFrame<Seriesify<Boulder>>;

//#nbts@code
let detailedAscents = ascents
  .join(boulders, {
    leftOn: 'ascendable_id',
    rightOn: 'nid',
  })
  .sort(pl.col('created_at'));

//#nbts@code
detailedAscents.toRecords()[0]
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
      color: { legend: true, type: 'categorical' },
      marks: [Plot.frame(), Plot.barY(detailedAscents.toRecords(), Plot.groupX({ y: 'count' }, grouping))],
      x: { type: 'band' },
      document,
    }),
  );
}

//#nbts@code
let sorted = detailedAscents
  .withColumns(
    pl.col('numberDifficulty').min().over(pl.col('created_at').date.strftime('%Y-%U'), 'type').sub(3).alias('weekDiffMin'),
    pl.col('numberDifficulty').max().over(pl.col('created_at').date.strftime('%Y-%U'), 'type').alias('weekDiffMax'),
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
        sorted.filter(v => v.type === type),
        {
          interval: 'week',
          x: 'created_at',
          y1: 'weekDiffMin',
          y2: 'weekDiffMax',
          fill: 'type',
        },
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
