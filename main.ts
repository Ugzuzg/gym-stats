import fsSync from 'node:fs';
import fs from 'node:fs/promises';

const me = 230474;

const fileExists = async (path) => {
  try {
    await fs.access(path, fs.constants.F_OK);
    return true;
  } catch (err) {
    return false;
  }
};

const getAccessToken = async () => {
  const oldToken = JSON.parse(await fs.readFile('token.json', 'utf-8'));
  const response = await fetch(
    'https://vlatka.vertical-life.info/auth/realms/Vertical-Life/protocol/openid-connect/token',
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: `grant_type=refresh_token&client_id=vertical-life-android-new&refresh_token=${oldToken.refresh_token}`,
    },
  );
  if (!response.ok) {
    console.log(await response.json());
    throw new Error('Failed to get access token');
  }
  return await response.json();
};

(async () => {
  const newToken = await getAccessToken();
  await fs.writeFile('token.json', JSON.stringify(newToken, null, 2));

  const limit = 150;
  let offset = 0;
  let singleReponse = [];
  const ascents = [];
  do {
    console.log(`Fetching ${offset} to ${offset + limit}`);
    const response = await fetch(
      `https://vlcapi.vertical-life.info/ascent/v1/app/users/${me}/ascents?offset=${offset}&limit=${limit}&zlaggable_filter=gym_bouldering&sorting=grade`,
      {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${newToken.access_token}`,
        },
      },
    );
    singleReponse = await response.json();
    offset += limit;
    ascents.push(...singleReponse);
  } while (singleReponse.length > 0);
  const ascentsStream = fsSync.createWriteStream('./data/ascents.jsonl');
  for (const ascent of ascents) {
    ascentsStream.write(JSON.stringify(ascent));
    ascentsStream.write('\n');
  }
  ascentsStream.close();

  const bouldersStream = fsSync.createWriteStream('./data/gym_boulders.jsonl');

  for (let i = 0; i < ascents.length; i++) {
    const ascent = ascents[i];
    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write(`Progress: ${i + 1} / ${ascents.length}`);

    const gymBoulder = await (async () => {
      const gymBoulderPath = `./data/gym_boulders/${ascent.ascendable_id}.json`;
      if (await fileExists(gymBoulderPath)) {
        return JSON.parse(await fs.readFile(gymBoulderPath, 'utf-8'));
      }
      const response = await fetch(
        `https://vlcapi.vertical-life.info/gym_boulders/${ascent.ascendable_id}`,
        {
          method: 'GET',
          headers: {
            Authorization: `Bearer ${newToken.access_token}`,
          },
        },
      );
      if (!response.ok) {
        console.log(await response.json());
        throw new Error('Failed to get gym boulder');
      }

      const result = await response.json();
      await fs.writeFile(gymBoulderPath, JSON.stringify(result, null, 2));

      return result;
    })();

    bouldersStream.write(JSON.stringify(gymBoulder));
    bouldersStream.write('\n');
  }
  bouldersStream.close();
  process.stdout.write('\n');
})();
