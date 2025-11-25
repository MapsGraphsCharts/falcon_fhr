# WebStorm Fingerprint Test Setup

Guidelines for spinning up a minimal Node.js Playwright project in WebStorm so you can evaluate whether [`playwright-with-fingerprints`](https://github.com/bablosoft/playwright-with-fingerprints) improves login success on fingerprint-aware targets.

Use this alongside the existing Python scraper. You only need the Node project for experiments; all production code can stay in `src/secure_scraper/`.

## 1. Prerequisites
- Node.js 18+ and npm (`node -v` to confirm).
- WebStorm (2023.3+ recommended) with the default Node.js tooling.
- Optional but recommended: an environment file copied from the Python project so credentials stay in one place.
- FingerprintSwitcher service key (paid tier for reliable fingerprints, leave blank for the limited free tier).

## 2. Scaffold the project
1. Create a sibling directory (for example `webstorm-experiments/`) and open it in WebStorm.
2. Run the following from WebStorm’s terminal:
   ```bash
   npm init -y
   npm i playwright playwright-with-fingerprints dotenv
   npx playwright install chromium
   ```
   The `dotenv` helper keeps env handling consistent with the Python project.

## 3. Configure environment variables
Create `.env` in the new project root:
```
FINGERPRINT_KEY=
FINGERPRINT_PROXY=          # optional, format socks5://host:port or http://host:port
LOGIN_USERNAME=             # reuse SCRAPER_USERNAME if desired
LOGIN_PASSWORD=             # reuse SCRAPER_PASSWORD
STORAGE_STATE=../data/logs/network/storage_state_latest.json
```
- `FINGERPRINT_KEY`: leave blank to use the free tier (expect throttling) or paste your paid key.
- `STORAGE_STATE`: points back to the Python project so you can reuse cookies by launching a persistent context (see step 5).

## 4. Add the login probe script
Create `scripts/login-fingerprint.js`:
```javascript
// Minimal login probe with fingerprint spoofing.
const { chromium } = require('playwright');
const { plugin } = require('playwright-with-fingerprints');
const path = require('node:path');
require('dotenv').config();

async function main() {
  plugin.setServiceKey(process.env.FINGERPRINT_KEY || '');
  if (process.env.FINGERPRINT_PROXY) {
    plugin.useProxy(process.env.FINGERPRINT_PROXY);
  }

  const fingerprint = await plugin.fetch({
    tags: ['Microsoft Windows', 'Chrome'],
  });
  plugin.useFingerprint(fingerprint);

  const browser = await plugin.launch({ headless: false });
  const context = await browser.newContext();
  const page = await context.newPage();

  await page.goto('https://example-login-host');
  await page.fill('#username', process.env.LOGIN_USERNAME);
  await page.fill('#password', process.env.LOGIN_PASSWORD);
  await page.click('button[type=submit]');
  await page.waitForTimeout(8000);

  await browser.close();
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
```
- Swap `https://example-login-host` and selectors with the real target. Keep timeout generous for MFA prompts.
- When you do not want fingerprint spoofing, temporarily switch to `chromium.launch()` and skip the `plugin` calls; this gives you a baseline run for comparison.

## 5. Reuse existing cookies or state (optional)
- Persistent profile: replace the `browser` launch with `plugin.launchPersistentContext(userDataDir, { headless: false })`, pointing `userDataDir` at a directory under the new project (do not reuse the Python profile directly).
- Storage state: keep `browser.newContext()` but pass `storageState`:\
  ```javascript
  const storageState = process.env.STORAGE_STATE && path.resolve(process.env.STORAGE_STATE);
  const context = await browser.newContext(
    storageState ? { storageState } : undefined
  );
  ```
  The path can reference the JSON produced by the Python Playwright runner so both projects share authenticated cookies.

## 6. Execute from WebStorm
1. Add a Node.js run configuration with `scripts/login-fingerprint.js` as the entry file.
2. Attach the `.env` file under *Environment variables*. WebStorm can load it automatically if you tick “Use env file”.
3. Run once with the plugin enabled. Capture console output and whether the login completes or triggers challenges.
4. Repeat with the plugin disabled to compare results. Log any differences (captchas, redirects, success) so you can decide whether to formalise the plugin in production code.

## 7. Next steps
- Cache fingerprints by writing `fingerprint` to disk (JSON) so reruns do not hit API limits.
- Exercise critical flows (login, navigation, API calls). If success rates improve, plan for a production integration layer that mirrors the Python configuration model.
- If the plugin does not help, revisit behavioural signals (delays, mouse movement) or evaluate alternatives such as Apify Fingerprint Suite or custom Chromium builds.

