
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
detailedAscents.toRecords()[0];

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
let ascentTypeColorDomain = { domain: ['flash', 'redpoint'], range: ['#ffd800', '#ff5151'] };

//#nbts@code
let groupings = [
  {
    name: 'Everything',
    y: 'difficulty',
    fill: 'type',
  },
  {
    name: 'By year',
    y: 'difficulty',
    fy: 'year',
    fill: 'type',
  },
  {
    name: 'By route setter',
    y: 'difficulty',
    fx: 'year',
    fy: 'route_setter',
    fill: 'type',
    sort: { fy: '-x', reduce: 'count' },
  },
];
for (const grouping of groupings) {
  await display(md`## ${grouping.name}`);
  await display(
    Plot.plot({
      grid: true,
      width: grouping.fx ? baseWidth * 2 : baseWidth,
      color: { legend: true, type: 'categorical', ...ascentTypeColorDomain },
      marks: [Plot.frame(), Plot.barX(detailedAscents.toRecords(), Plot.groupY({ x: 'count' }, grouping))],
      y: { type: 'band', reverse: true },
      document,
    }),
  );
}

//#nbts@code
await display(
  md`
## Poor man's CPR
  `,
);

Plot.plot(
  (() => {
    const data = detailedAscents
      .groupBy(pl.col('created_at').date.strftime('%Y-%U').alias('week'), 'type')
      .agg(
        pl.col('created_at').first(),
        pl.col('numberDifficulty').min().sub(3).alias('difficultyMin'),
        pl.col('numberDifficulty').max().alias('difficultyMax'),
        pl.col('created_at').count().alias('count'),
      );

    return {
      figure: true,
      grid: true,
      color: { legend: true, type: 'categorical', ...ascentTypeColorDomain },
      width: 1500,
      y: {
        ticks: boulderScores.filter((grade) => grade.font <= '7B').map((grade) => grade.score),
        tickFormat: (v) => boulderScores.find(({ score }) => v === score)?.font,
      },
      marks: [
        Plot.frame(),
        ...['redpoint', 'flash'].map((type) =>
          Plot.rectY(
            data.filter(pl.col('type').eq(pl.lit(type))).toRecords(),

            {
              interval: 'week',
              x: 'created_at',

              y1: 'difficultyMin',
              y2: 'difficultyMax',
              fill: 'type',
              title: (d) => `${d.difficultyMin} - ${d.difficultyMax}`,
              inset: 1,
              mixBlendMode: 'color-dodge',
            },
          ),
        ),
        /*
        Plot.rectY(data.toRecords(), { interval: 'week', x: 'created_at', y: 'count', fill: 'gray' }),
        Plot.text(data.toRecords(), {
          x: 'created_at',
          y: 'count',
          text: 'count',
          lineAnchor: 'bottom',
        }),
        */
      ],
      document,
    };
  })(),
);

//#nbts@code
import * as d3 from 'npm:d3';

Plot.plot(
  (() => {
    const data = detailedAscents
      .groupBy(pl.col('created_at').date.strftime('%Y-%U').alias('grouped'))
      .agg(
        pl.col('created_at').first().date.strftime('%Y-%m-%d').alias('date'),
        pl.col('numberDifficulty').sum().alias('totalNumberDifficulty'),
        pl.col('created_at').count().alias('count'),
        pl.col('difficulty'),
      )
      .toRecords();
    return {
      figure: true,
      x: { tickFormat: (d) => `Week ${d + 1}`, tickRotate: 90 },
      y: { tickFormat: '', tickSize: 0 },
      color: { scheme: 'YlGn' },
      marginBottom: 47,
      marks: [
        Plot.cell(data, {
          x: (d) => d3.utcWeek.count(d3.utcYear(new Date(d.date)), new Date(d.date)),
          y: (d) => new Date(d.date).getUTCFullYear(),
          fill: 'totalNumberDifficulty',
          title: (d) =>
            Object.entries(
              d.difficulty.reduce((acc, grade) => {
                if (!acc[grade]) {
                  acc[grade] = 0;
                }
                acc[grade] += 1;
                return acc;
              }, {}),
            )
              .toSorted((a, b) => a[0].localeCompare(b[0]))
              .reduce((acc, ascents) => [...acc, `${ascents[0]} x${ascents[1]}`], [] as string[])
              .join('; '),
          inset: 1,
        }),
      ],
      document,
    };
  })(),
);

//#nbts@code
Plot.plot(
  (() => {
    const data = detailedAscents
      .groupBy(pl.col('created_at').date.strftime('%Y-%U').alias('grouped'))
      .agg(
        pl.col('created_at').first().cast(pl.Date).alias('date'),
        pl.col('numberDifficulty').sum().alias('totalNumberDifficulty'),
        pl.col('created_at').count().alias('count'),
        pl.col('difficulty'),
      )
      .toRecords();
    return {
      figure: true,
      color: { scheme: 'YlGn' },
      marks: [
        Plot.barX(data, {
          interval: 'week',
          x: 'date',
          fill: 'totalNumberDifficulty',
          title: (d) =>
            Object.entries(
              d.difficulty.reduce((acc, grade) => {
                if (!acc[grade]) {
                  acc[grade] = 0;
                }
                acc[grade] += 1;
                return acc;
              }, {}),
            )
              .toSorted((a, b) => a[0].localeCompare(b[0]))
              .reduce((acc, ascents) => [...acc, `${ascents[0]} x${ascents[1]}`], [] as string[])
              .join('; '),
        }),
      ],
      document,
    };
  })(),
);
