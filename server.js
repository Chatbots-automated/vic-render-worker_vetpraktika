const express = require('express');
const { chromium } = require('playwright');
const fs = require('fs/promises');
const fsSync = require('fs');
const path = require('path');
const crypto = require('crypto');
const archiver = require('archiver');

console.log('SERVER VERSION: vic-direct-pdf-zip-v2');

const app = express();
app.use(express.json({ limit: '25mb' }));

const PORT = process.env.PORT || 3000;
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN;
const HEADLESS = String(process.env.PLAYWRIGHT_HEADLESS || 'true') === 'true';

// Important: do not set this to 50.
// 50 farms can be submitted at once, but the worker should process a few in parallel.
const MAX_PARALLEL_CONTEXTS = Number(process.env.MAX_PARALLEL_CONTEXTS || 4);

const VIC_LOGIN_URL = 'https://ise.vic.lt/Public/Login.aspx';
const LIVE_ANIMALS_URL = 'https://ise.vic.lt/GPSAS/Ataskaitos/GyvuGyvunuSarasas';

let browser;

function requireInternalAuth(req, res, next) {
  if (!INTERNAL_TOKEN) {
    return res.status(500).json({
      ok: false,
      error: 'Missing INTERNAL_TOKEN environment variable.'
    });
  }

  if (req.headers['x-internal-token'] !== INTERNAL_TOKEN) {
    return res.status(401).json({
      ok: false,
      error: 'Unauthorized'
    });
  }

  next();
}

function normalizeValue(value) {
  const cleaned = String(value ?? '').trim();
  return cleaned || null;
}

function normalizeCompact(value) {
  const cleaned = String(value ?? '').replace(/\s+/g, '').trim();
  return cleaned || null;
}

function fileSafe(value) {
  return String(value || 'file')
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, '_')
    .replace(/\s+/g, '_')
    .slice(0, 120);
}

function getLithuaniaTodayDate() {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Vilnius',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(new Date());

  const year = parts.find((p) => p.type === 'year')?.value;
  const month = parts.find((p) => p.type === 'month')?.value;
  const day = parts.find((p) => p.type === 'day')?.value;

  return `${year}-${month}-${day}`;
}

function normalizeDateValue(value, fallbackDate) {
  const raw = String(value ?? '').trim();

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }

  const digitsOnly = raw.replace(/[^\d]/g, '');

  if (digitsOnly.length === 8) {
    if (digitsOnly.startsWith('20')) {
      return `${digitsOnly.slice(0, 4)}-${digitsOnly.slice(4, 6)}-${digitsOnly.slice(6, 8)}`;
    }

    return `${digitsOnly.slice(4, 8)}-${digitsOnly.slice(2, 4)}-${digitsOnly.slice(0, 2)}`;
  }

  return fallbackDate || getLithuaniaTodayDate();
}

function getVetCredentialsFromBody(body) {
  const vet = body.vet || {};

  return {
    vic_username:
      normalizeValue(body.vic_username) ||
      normalizeValue(body.vicUsername) ||
      normalizeValue(body.vet_username) ||
      normalizeValue(body.vetUsername) ||
      normalizeValue(vet.vic_username) ||
      normalizeValue(vet.vicUsername) ||
      normalizeValue(vet.username),

    vic_password:
      normalizeValue(body.vic_password) ||
      normalizeValue(body.vicPassword) ||
      normalizeValue(body.vet_password) ||
      normalizeValue(body.vetPassword) ||
      normalizeValue(vet.vic_password) ||
      normalizeValue(vet.vicPassword) ||
      normalizeValue(vet.password)
  };
}

function normalizeFarmInput(farm, defaultVetCredentials, defaultSearchDate) {
  return {
    id: normalizeValue(farm.id || farm.farm_id),
    name: normalizeValue(farm.name || farm.farm_name),

    client_personal_code:
      normalizeCompact(farm.client_personal_code) ||
      normalizeCompact(farm.clientPersonalCode) ||
      normalizeCompact(farm.personal_code) ||
      normalizeCompact(farm.personalCode) ||
      normalizeCompact(farm.holder_code) ||
      normalizeCompact(farm.holderCode) ||
      normalizeCompact(farm.farm_code) ||
      normalizeCompact(farm.farmCode) ||
      normalizeCompact(farm.code),

    vic_username:
      normalizeValue(farm.vet_vic_username) ||
      normalizeValue(farm.vetVicUsername) ||
      normalizeValue(farm.vic_username) ||
      defaultVetCredentials.vic_username,

    vic_password:
      normalizeValue(farm.vet_vic_password) ||
      normalizeValue(farm.vetVicPassword) ||
      normalizeValue(farm.vic_password) ||
      defaultVetCredentials.vic_password,

    search_date: normalizeDateValue(
      farm.search_date || farm.searchDate || defaultSearchDate,
      getLithuaniaTodayDate()
    )
  };
}

async function getBrowser() {
  if (!browser) {
    browser = await chromium.launch({
      headless: HEADLESS,
      args: ['--disable-dev-shm-usage']
    });
  }

  return browser;
}

async function getBodyTextPreview(page) {
  try {
    return await page.evaluate(() => {
      return (document.body.innerText || '').slice(0, 5000);
    });
  } catch {
    return null;
  }
}

async function safeScreenshot(page, tmpDir, farmId, runId) {
  try {
    const screenshotPath = path.join(
      tmpDir,
      `${fileSafe(farmId || 'unknown')}-${runId}-error.png`
    );

    await page.screenshot({
      path: screenshotPath,
      fullPage: true
    });

    return screenshotPath;
  } catch {
    return null;
  }
}

async function fillLoginField(page, selector, value) {
  const locator = page.locator(selector).first();

  await locator.waitFor({
    state: 'visible',
    timeout: 30000
  });

  await locator.click({ timeout: 30000 });

  await page.keyboard.press('Control+A').catch(() => null);
  await page.keyboard.press('Meta+A').catch(() => null);
  await page.keyboard.press('Backspace').catch(() => null);

  await locator.fill('');
  await locator.type(String(value), { delay: 35 });

  await locator.evaluate((el) => {
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new Event('blur', { bubbles: true }));
  });

  const actualValue = await locator.inputValue().catch(() => '');

  if (!actualValue) {
    throw new Error(`Login field did not fill correctly: ${selector}`);
  }
}

async function waitForLoginResult(page) {
  await page
    .waitForFunction(
      () => {
        const bodyText = document.body.innerText || '';

        const hasGpsasLink =
          Array.from(document.querySelectorAll('a')).some((a) => {
            const href = a.href || '';
            const text = a.textContent || '';

            return (
              href.includes('/GPSAS') ||
              text.includes('Ūkinių gyvūnų registras')
            );
          });

        const stillOnLogin =
          !!document.querySelector('#ctl00_PublicPlaceHolder_UserName') ||
          !!document.querySelector('#ctl00_PublicPlaceHolder_Password');

        const hasLoginError =
          bodyText.includes('Būtinas laukas') ||
          bodyText.includes('Neteisingas') ||
          bodyText.includes('neteisingas') ||
          bodyText.includes('Nepavyko') ||
          bodyText.includes('Klaida');

        return hasGpsasLink || (stillOnLogin && hasLoginError);
      },
      null,
      { timeout: 60000 }
    )
    .catch(() => null);
}

async function loginToVic(page, vicUsername, vicPassword) {
  const maxAttempts = 3;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    console.log(`[loginToVic] attempt ${attempt}/${maxAttempts}`);

    await page.goto(VIC_LOGIN_URL, {
      waitUntil: 'domcontentloaded',
      timeout: 60000
    });

    await page.waitForTimeout(800);

    await fillLoginField(
      page,
      '#ctl00_PublicPlaceHolder_UserName',
      vicUsername
    );

    await page.waitForTimeout(300);

    await fillLoginField(
      page,
      '#ctl00_PublicPlaceHolder_Password',
      vicPassword
    );

    await page.waitForTimeout(300);

    const usernameValue = await page
      .locator('#ctl00_PublicPlaceHolder_UserName')
      .inputValue()
      .catch(() => '');

    const passwordValue = await page
      .locator('#ctl00_PublicPlaceHolder_Password')
      .inputValue()
      .catch(() => '');

    if (!usernameValue || !passwordValue) {
      if (attempt === maxAttempts) {
        throw new Error('Login fields were empty before submit.');
      }

      await page.waitForTimeout(1000);
      continue;
    }

    await Promise.all([
      page.waitForLoadState('domcontentloaded', { timeout: 60000 }).catch(() => null),
      page.locator('#ctl00_PublicPlaceHolder_LoginButton').click()
    ]);

    await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null);
    await waitForLoginResult(page);

    const bodyText = await getBodyTextPreview(page);
    const currentUrl = page.url();

    const loggedIn =
      currentUrl.includes('/GPSAS') ||
      (bodyText || '').includes('Ūkinių gyvūnų registras') ||
      (bodyText || '').includes('Registravimas') ||
      (bodyText || '').includes('Ataskaitos');

    if (loggedIn) {
      console.log('[loginToVic] login success');
      return;
    }

    console.log(`[loginToVic] login not confirmed. URL=${currentUrl}`);
    console.log(`[loginToVic] body preview: ${(bodyText || '').slice(0, 500)}`);

    if (attempt === maxAttempts) {
      throw new Error(
        `VIC login failed after ${maxAttempts} attempts. Preview: ${(bodyText || '').slice(0, 500)}`
      );
    }

    await page.waitForTimeout(1500);
  }
}

async function openLiveAnimalsPage(page) {
  await page.goto(LIVE_ANIMALS_URL, {
    waitUntil: 'domcontentloaded',
    timeout: 60000
  });

  await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null);

  await page.locator('#AsmKodas').waitFor({
    state: 'visible',
    timeout: 60000
  });
}

async function fillInputAndTriggerEvents(page, selector, value) {
  const locator = page.locator(selector).first();

  await locator.waitFor({
    state: 'visible',
    timeout: 30000
  });

  await locator.click({ timeout: 30000 });

  await page.keyboard.press('Control+A').catch(() => null);
  await page.keyboard.press('Meta+A').catch(() => null);
  await page.keyboard.press('Backspace').catch(() => null);

  await locator.fill('');
  await locator.type(String(value), { delay: 35 });

  await locator.evaluate((el) => {
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new Event('blur', { bubbles: true }));
  });

  await page.waitForTimeout(500);

  const actualValue = await locator.inputValue().catch(() => '');

  if (!actualValue) {
    throw new Error(`Input did not fill correctly: ${selector}`);
  }

  return actualValue;
}

async function fillClientCode(page, code) {
  const cleanCode = normalizeCompact(code);

  if (!cleanCode) {
    throw new Error('Missing client personal/company code.');
  }

  const locator = page.locator('#AsmKodas').first();

  await locator.waitFor({
    state: 'visible',
    timeout: 30000
  });

  for (let attempt = 1; attempt <= 3; attempt++) {
    console.log(`[fillClientCode] attempt ${attempt}, code=${cleanCode}`);

    await locator.click({ timeout: 30000 });

    await page.keyboard.press('Control+A').catch(() => null);
    await page.keyboard.press('Meta+A').catch(() => null);
    await page.keyboard.press('Backspace').catch(() => null);

    await locator.fill('');
    await locator.type(cleanCode, { delay: 55 });

    await page.waitForTimeout(1000);

    await locator.evaluate((el) => {
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
    });

    // VIC often needs keyboard confirmation/autocomplete-like behavior.
    await locator.press('Enter').catch(() => null);
    await page.waitForTimeout(700);

    await locator.press('Tab').catch(() => null);
    await page.waitForTimeout(800);

    await locator.evaluate((el) => {
      el.dispatchEvent(new Event('change', { bubbles: true }));
      el.dispatchEvent(new Event('blur', { bubbles: true }));
    });

    await page.waitForTimeout(700);

    const actual = normalizeCompact(await locator.inputValue().catch(() => ''));

    console.log(`[fillClientCode] actual=${actual}`);

    if (actual === cleanCode) {
      return actual;
    }
  }

  const actualValue = await locator.inputValue().catch(() => '');

  throw new Error(
    `AsmKodas did not fill correctly. expected=${cleanCode}, actual=${actualValue}`
  );
}

async function clickSearch(page) {
  const searchButton = page
    .locator('#searchBtn, button:has-text("Ieškoti"), input[value="Ieškoti"]')
    .first();

  await searchButton.waitFor({
    state: 'visible',
    timeout: 30000
  });

  await Promise.all([
    page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null),
    searchButton.click()
  ]);

  await page.waitForTimeout(4000);
}

async function waitForSearchResultOrState(page) {
  await page
    .waitForFunction(
      () => {
        const bodyText = document.body.innerText || '';

        const pdfButton = document.querySelector('#printBtn');

        const hasVisiblePdfButton =
          !!pdfButton &&
          pdfButton.offsetParent !== null &&
          window.getComputedStyle(pdfButton).display !== 'none' &&
          window.getComputedStyle(pdfButton).visibility !== 'hidden';

        const hasResults =
          bodyText.includes('Laikytojas') ||
          bodyText.includes('Banda') ||
          bodyText.includes('Rūšis') ||
          bodyText.includes('Iš viso ataskaitoje') ||
          bodyText.includes('Iš viso:');

        const hasNoDataMessage =
          bodyText.includes('Pagal pasirinktus paieškos kriterijus įrašų nerasta') ||
          bodyText.includes('įrašų nerasta') ||
          bodyText.includes('Duomenų nėra') ||
          bodyText.includes('duomenų nėra') ||
          bodyText.includes('Nerasta') ||
          bodyText.includes('nerasta') ||
          bodyText.includes('Nėra duomenų') ||
          bodyText.includes('nėra duomenų');

        const hasValidationMessage =
          bodyText.includes('Privalomas') ||
          bodyText.includes('privalomas') ||
          bodyText.includes('Įveskite') ||
          bodyText.includes('įveskite') ||
          bodyText.includes('Neteisingas') ||
          bodyText.includes('neteisingas');

        return (
          hasVisiblePdfButton ||
          hasResults ||
          hasNoDataMessage ||
          hasValidationMessage
        );
      },
      null,
      { timeout: 90000 }
    )
    .catch(() => null);

  await page.waitForTimeout(1500);
}

async function detectNoRecords(page) {
  const preview = await getBodyTextPreview(page);

  const noRecords =
    (preview || '').includes('Pagal pasirinktus paieškos kriterijus įrašų nerasta') ||
    (preview || '').includes('įrašų nerasta') ||
    (preview || '').includes('Duomenų nėra') ||
    (preview || '').includes('duomenų nėra') ||
    (preview || '').includes('Nėra duomenų') ||
    (preview || '').includes('nėra duomenų') ||
    (preview || '').includes('Nerasta') ||
    (preview || '').includes('nerasta');

  return {
    noRecords,
    bodyText: preview
  };
}

async function detectLoadedResults(page) {
  const preview = await getBodyTextPreview(page);

  const hasHolder =
    (preview || '').includes('Laikytojas') &&
    (preview || '').includes('Asmens/įmonės kodas');

  const hasHerd =
    (preview || '').includes('Banda') &&
    (preview || '').includes('Rūšis');

  const hasSummary =
    (preview || '').includes('Iš viso ataskaitoje') ||
    (preview || '').includes('Iš viso:');

  return {
    hasResults: hasHolder || hasHerd || hasSummary,
    bodyText: preview
  };
}

async function getPdfButtonState(page) {
  const pdfButton = page.locator('#printBtn').first();

  const exists = (await pdfButton.count().catch(() => 0)) > 0;

  if (!exists) {
    return {
      exists: false,
      visible: false
    };
  }

  const visible = await pdfButton
    .evaluate((el) => {
      const style = window.getComputedStyle(el);

      return (
        el.offsetParent !== null &&
        style.display !== 'none' &&
        style.visibility !== 'hidden'
      );
    })
    .catch(() => false);

  return {
    exists,
    visible
  };
}

async function runSearchAndInspect(page) {
  await clickSearch(page);
  await waitForSearchResultOrState(page);

  const noRecordCheck = await detectNoRecords(page);
  const resultCheck = await detectLoadedResults(page);
  const pdfState = await getPdfButtonState(page);

  const bodyText =
    resultCheck.bodyText ||
    noRecordCheck.bodyText ||
    (await getBodyTextPreview(page));

  return {
    noRecordCheck,
    resultCheck,
    pdfState,
    bodyText
  };
}

async function downloadPdf(page) {
  const pdfButton = page.locator('#printBtn').first();

  const exists = (await pdfButton.count().catch(() => 0)) > 0;

  if (!exists) {
    throw new Error('PDF button #printBtn does not exist.');
  }

  const visible = await pdfButton
    .evaluate((el) => {
      const style = window.getComputedStyle(el);

      return (
        el.offsetParent !== null &&
        style.display !== 'none' &&
        style.visibility !== 'hidden'
      );
    })
    .catch(() => false);

  if (!visible) {
    const preview = await getBodyTextPreview(page);

    throw new Error(
      `PDF button #printBtn exists but is hidden. Preview: ${(preview || '').slice(0, 700)}`
    );
  }

  const downloadPromise = page.waitForEvent('download', {
    timeout: 90000
  });

  await pdfButton.click();

  return await downloadPromise;
}

async function processFarmToPdf(farm) {
  const runId = crypto.randomUUID();
  const startedAt = new Date().toISOString();

  const tmpDir = '/tmp/vic-pdfs';
  await fs.mkdir(tmpDir, { recursive: true });

  const resultBase = {
    run_id: runId,
    farm_id: farm.id,
    farm_name: farm.name,
    client_personal_code: farm.client_personal_code,
    search_date: farm.search_date,
    vic_username: farm.vic_username,
    started_at: startedAt
  };

  if (!farm.id) {
    return {
      ...resultBase,
      success: false,
      stage: 'validation',
      error: 'Missing farm id.'
    };
  }

  if (!farm.vic_username || !farm.vic_password) {
    return {
      ...resultBase,
      success: false,
      stage: 'validation',
      error: 'Missing VIC credentials.'
    };
  }

  if (!farm.client_personal_code) {
    return {
      ...resultBase,
      success: false,
      stage: 'validation',
      error: 'Missing client_personal_code.'
    };
  }

  const b = await getBrowser();

  const context = await b.newContext({
    acceptDownloads: true,
    viewport: {
      width: 1600,
      height: 1000
    }
  });

  const page = await context.newPage();

  let stage = 'started';
  let localPath = null;

  try {
    stage = 'login';
    console.log(`[${runId}] Login as ${farm.vic_username}`);
    await loginToVic(page, farm.vic_username, farm.vic_password);

    stage = 'open_live_animals_page';
    console.log(`[${runId}] Open live animals page`);
    await openLiveAnimalsPage(page);

    stage = 'fill_client_code';
    console.log(`[${runId}] Fill client code ${farm.client_personal_code}`);
    await fillClientCode(page, farm.client_personal_code);

    stage = 'fill_search_date';
    console.log(`[${runId}] Fill search date ${farm.search_date}`);
    await fillInputAndTriggerEvents(page, '#PaieskosData', farm.search_date);

    await page.locator('#PaieskosData').press('Enter').catch(() => null);

    await page
      .locator('h4:has-text("Gyvų gyvūnų sąrašas")')
      .click()
      .catch(() => null);

    await page.waitForTimeout(500);

    stage = 'search';
    let inspection = await runSearchAndInspect(page);

    // Retry once if VIC did not clearly load results.
    if (
      !inspection.noRecordCheck.noRecords &&
      !inspection.resultCheck.hasResults &&
      !inspection.pdfState.visible
    ) {
      console.log(`[${runId}] unclear result, retrying once`);

      await page.waitForTimeout(1500);

      await fillClientCode(page, farm.client_personal_code);
      await fillInputAndTriggerEvents(page, '#PaieskosData', farm.search_date);
      await page.locator('#PaieskosData').press('Enter').catch(() => null);

      await page
        .locator('h4:has-text("Gyvų gyvūnų sąrašas")')
        .click()
        .catch(() => null);

      await page.waitForTimeout(800);

      inspection = await runSearchAndInspect(page);
    }

    const bodyTextPreview =
      inspection.bodyText ||
      inspection.noRecordCheck.bodyText ||
      (await getBodyTextPreview(page));

    if (inspection.noRecordCheck.noRecords) {
      await context.close().catch(() => null);

      return {
        ...resultBase,
        success: false,
        stage: 'no_records_found',
        error: 'No records found for selected search criteria.',
        current_url: page.url(),
        body_text_preview_after_search: (bodyTextPreview || '').slice(0, 3000),
        finished_at: new Date().toISOString()
      };
    }

    if (!inspection.pdfState.visible) {
      const shotPath = await safeScreenshot(page, tmpDir, farm.id, runId);

      await context.close().catch(() => null);

      return {
        ...resultBase,
        success: false,
        stage: 'no_pdf_available',
        error: `PDF button hidden or unavailable. pdfExists=${inspection.pdfState.exists}, hasResults=${inspection.resultCheck.hasResults}`,
        current_url: page.url(),
        body_text_preview_after_search: (bodyTextPreview || '').slice(0, 3000),
        screenshot_path: shotPath,
        finished_at: new Date().toISOString()
      };
    }

    stage = 'download_pdf';
    console.log(`[${runId}] Download PDF`);

    const download = await downloadPdf(page);
    const suggestedName = download.suggestedFilename();

    const fileName = fileSafe(
      suggestedName ||
        `live-animals-${farm.name || farm.id}-${farm.client_personal_code}-${farm.search_date}.pdf`
    );

    localPath = path.join(
      tmpDir,
      `${fileSafe(farm.id)}-${runId}-${fileName.endsWith('.pdf') ? fileName : `${fileName}.pdf`}`
    );

    await download.saveAs(localPath);

    const stat = await fs.stat(localPath);

    await context.close().catch(() => null);

    return {
      ...resultBase,
      success: true,
      stage: 'done',
      file_name: path.basename(localPath),
      file_path: localPath,
      file_size: stat.size,
      current_url: page.url(),
      finished_at: new Date().toISOString()
    };
  } catch (err) {
    const errorMessage = err.message || String(err);
    const bodyTextPreview = await getBodyTextPreview(page).catch(() => null);
    const shotPath = await safeScreenshot(page, tmpDir, farm.id, runId);

    if (localPath) {
      await fs.unlink(localPath).catch(() => null);
    }

    await context.close().catch(() => null);

    return {
      ...resultBase,
      success: false,
      stage,
      error: errorMessage,
      current_url: page.url(),
      body_text_preview: (bodyTextPreview || '').slice(0, 3000),
      screenshot_path: shotPath,
      finished_at: new Date().toISOString()
    };
  }
}

async function runWithConcurrency(items, concurrency, worker) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function runner() {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      results[currentIndex] = await worker(items[currentIndex], currentIndex);
    }
  }

  const runners = [];

  const runnerCount = Math.min(concurrency, items.length);

  for (let i = 0; i < runnerCount; i++) {
    runners.push(runner());
  }

  await Promise.all(runners);

  return results;
}

async function createZipFromResults(results, zipPath) {
  const output = fsSync.createWriteStream(zipPath);
  const archive = archiver('zip', {
    zlib: { level: 9 }
  });

  const finished = new Promise((resolve, reject) => {
    output.on('close', resolve);
    archive.on('error', reject);
  });

  archive.pipe(output);

  const manifest = {
    ok: true,
    created_at: new Date().toISOString(),
    total: results.length,
    success_count: results.filter((r) => r.success).length,
    failed_count: results.filter((r) => !r.success).length,
    results: results.map((r) => ({
      run_id: r.run_id,
      farm_id: r.farm_id,
      farm_name: r.farm_name,
      client_personal_code: r.client_personal_code,
      search_date: r.search_date,
      success: r.success,
      stage: r.stage,
      error: r.error || null,
      file_name: r.zip_file_name || r.file_name || null,
      file_size: r.file_size || null,
      current_url: r.current_url || null,
      body_text_preview: r.body_text_preview || r.body_text_preview_after_search || null,
      screenshot_path: r.screenshot_path || null,
      started_at: r.started_at,
      finished_at: r.finished_at
    }))
  };

  archive.append(JSON.stringify(manifest, null, 2), {
    name: 'manifest.json'
  });

  for (const result of results) {
    if (!result.success || !result.file_path) continue;

    const baseName = fileSafe(
      `${result.farm_name || result.farm_id || 'farm'}-${result.client_personal_code || 'code'}-${result.search_date || 'date'}.pdf`
    );

    const zipFileName = `pdfs/${baseName.endsWith('.pdf') ? baseName : `${baseName}.pdf`}`;

    result.zip_file_name = zipFileName;

    archive.file(result.file_path, {
      name: zipFileName
    });
  }

  await archive.finalize();
  await finished;
}

async function cleanupResultFiles(results) {
  for (const result of results) {
    if (result.file_path) {
      await fs.unlink(result.file_path).catch(() => null);
    }
  }
}

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    version: 'vic-direct-pdf-zip-v2',
    max_parallel_contexts: MAX_PARALLEL_CONTEXTS,
    headless: HEADLESS
  });
});

app.post('/download-live-animals-pdf', requireInternalAuth, async (req, res) => {
  const defaultVetCredentials = getVetCredentialsFromBody(req.body);

  const defaultSearchDate = normalizeDateValue(
    req.body.search_date || req.body.searchDate,
    getLithuaniaTodayDate()
  );

  const rawFarm = req.body.farm || req.body;

  const farm = normalizeFarmInput(
    rawFarm,
    defaultVetCredentials,
    defaultSearchDate
  );

  const result = await processFarmToPdf(farm);

  if (!result.success) {
    return res.status(422).json({
      ok: false,
      result
    });
  }

  const fileBuffer = await fs.readFile(result.file_path);

  await fs.unlink(result.file_path).catch(() => null);

  res.setHeader('Content-Type', 'application/pdf');
  res.setHeader(
    'Content-Disposition',
    `attachment; filename="${fileSafe(result.file_name || 'live-animals.pdf')}"`
  );
  res.setHeader('Content-Length', fileBuffer.length);
  res.setHeader('X-Farm-Id', result.farm_id || '');
  res.setHeader('X-Farm-Name', encodeURIComponent(result.farm_name || ''));
  res.setHeader('X-Client-Personal-Code', result.client_personal_code || '');
  res.setHeader('X-Search-Date', result.search_date || '');

  return res.send(fileBuffer);
});

app.post('/download-live-animals-pdfs', requireInternalAuth, async (req, res) => {
  let zipPath = null;
  let results = [];

  try {
    const rawFarms = Array.isArray(req.body.farms) ? req.body.farms : [];

    if (rawFarms.length === 0) {
      return res.status(400).json({
        ok: false,
        error: 'No farms provided. Expected body.farms array.'
      });
    }

    const defaultVetCredentials = getVetCredentialsFromBody(req.body);

    const defaultSearchDate = normalizeDateValue(
      req.body.search_date || req.body.searchDate,
      getLithuaniaTodayDate()
    );

    const farms = rawFarms.map((farm) =>
      normalizeFarmInput(farm, defaultVetCredentials, defaultSearchDate)
    );

    const requestedConcurrency = Number(req.body.concurrency || MAX_PARALLEL_CONTEXTS);

    const concurrency = Math.max(
      1,
      Math.min(requestedConcurrency, MAX_PARALLEL_CONTEXTS, farms.length)
    );

    console.log(
      `[download-live-animals-pdfs] farms=${farms.length}, concurrency=${concurrency}`
    );

    results = await runWithConcurrency(farms, concurrency, async (farm, index) => {
      console.log(
        `[batch] ${index + 1}/${farms.length} starting farm=${farm.name} code=${farm.client_personal_code}`
      );

      const result = await processFarmToPdf(farm);

      console.log(
        `[batch] ${index + 1}/${farms.length} finished success=${result.success} stage=${result.stage}`
      );

      return result;
    });

    const zipDir = '/tmp/vic-zips';
    await fs.mkdir(zipDir, { recursive: true });

    const zipName = `vic-live-animals-${defaultSearchDate}-${Date.now()}.zip`;
    zipPath = path.join(zipDir, zipName);

    await createZipFromResults(results, zipPath);

    const zipBuffer = await fs.readFile(zipPath);

    await cleanupResultFiles(results);
    await fs.unlink(zipPath).catch(() => null);

    res.setHeader('Content-Type', 'application/zip');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename="${zipName}"`
    );
    res.setHeader('Content-Length', zipBuffer.length);
    res.setHeader('X-Total-Farms', String(results.length));
    res.setHeader('X-Success-Count', String(results.filter((r) => r.success).length));
    res.setHeader('X-Failed-Count', String(results.filter((r) => !r.success).length));

    return res.send(zipBuffer);
  } catch (err) {
    console.error('[download-live-animals-pdfs] fatal:', err);

    await cleanupResultFiles(results).catch(() => null);

    if (zipPath) {
      await fs.unlink(zipPath).catch(() => null);
    }

    return res.status(500).json({
      ok: false,
      error: err.message || String(err),
      stack: err.stack || null,
      partial_results: results.map((r) => ({
        farm_id: r.farm_id,
        farm_name: r.farm_name,
        success: r.success,
        stage: r.stage,
        error: r.error || null
      }))
    });
  }
});

process.on('SIGINT', async () => {
  if (browser) {
    await browser.close().catch(() => null);
  }

  process.exit(0);
});

process.on('SIGTERM', async () => {
  if (browser) {
    await browser.close().catch(() => null);
  }

  process.exit(0);
});

app.listen(PORT, () => {
  console.log(`vic worker listening on ${PORT}`);
});
