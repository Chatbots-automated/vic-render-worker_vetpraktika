const express = require('express');
const { chromium } = require('playwright');
const { createClient } = require('@supabase/supabase-js');
const pLimit = require('p-limit').default;
const fs = require('fs/promises');
const path = require('path');
const crypto = require('crypto');

console.log('SERVER VERSION: vic-vet-login-client-code-stable-v8');

const app = express();
app.use(express.json({ limit: '10mb' }));

const PORT = process.env.PORT || 3000;
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN;
const HEADLESS = String(process.env.PLAYWRIGHT_HEADLESS || 'true') === 'true';
const MAX_PARALLEL_CONTEXTS = Number(process.env.MAX_PARALLEL_CONTEXTS || 1);
const STORAGE_BUCKET = process.env.SUPABASE_STORAGE_BUCKET || 'vic-pdfs';

const VIC_LOGIN_URL = 'https://ise.vic.lt/Public/Login.aspx';
const LIVE_ANIMALS_URL = 'https://ise.vic.lt/GPSAS/Ataskaitos/GyvuGyvunuSarasas';

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

let browser;
const limit = pLimit(MAX_PARALLEL_CONTEXTS);

function requireInternalAuth(req, res, next) {
  if (req.headers['x-internal-token'] !== INTERNAL_TOKEN) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  next();
}

function fileSafe(s) {
  return String(s || '').replace(/[<>:"/\\|?*\x00-\x1F]/g, '_');
}

function normalizeValue(value) {
  const cleaned = String(value ?? '').trim();
  return cleaned || null;
}

function normalizeCompact(value) {
  const cleaned = String(value ?? '').replace(/\s+/g, '').trim();
  return cleaned || null;
}

function getLithuaniaTodayDate() {
  const now = new Date();

  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Vilnius',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(now);

  const year = parts.find((p) => p.type === 'year')?.value;
  const month = parts.find((p) => p.type === 'month')?.value;
  const day = parts.find((p) => p.type === 'day')?.value;

  return `${year}-${month}-${day}`;
}

function normalizeDateValue(value, fallbackDate) {
  const compact = String(value ?? '').replace(/\s+/g, '').trim();

  if (/^\d{4}-\d{2}-\d{2}$/.test(compact)) {
    return compact;
  }

  const digitsOnly = compact.replace(/[^\d]/g, '');

  if (digitsOnly.length === 8) {
    return `${digitsOnly.slice(0, 4)}-${digitsOnly.slice(4, 6)}-${digitsOnly.slice(6, 8)}`;
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
      normalizeValue(vet.password),
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
    ),
  };
}

async function getBrowser() {
  if (!browser) {
    browser = await chromium.launch({ headless: HEADLESS });
  }

  return browser;
}

async function uploadToSupabase(localPath, storagePath) {
  const fileBuffer = await fs.readFile(localPath);

  console.log(
    `[supabase] uploading bucket=${STORAGE_BUCKET} path=${storagePath} bytes=${fileBuffer.length}`
  );

  const { data, error } = await supabase.storage
    .from(STORAGE_BUCKET)
    .upload(storagePath, fileBuffer, {
      contentType: 'application/pdf',
      upsert: true,
    });

  if (error) {
    console.error('[supabase] upload error raw:', error);

    throw new Error(
      `Supabase upload failed: ${error.message || JSON.stringify(error)} | bucket=${STORAGE_BUCKET} | path=${storagePath}`
    );
  }

  return data;
}

async function createSignedUrl(storagePath) {
  console.log(
    `[supabase] creating signed url bucket=${STORAGE_BUCKET} path=${storagePath}`
  );

  const { data, error } = await supabase.storage
    .from(STORAGE_BUCKET)
    .createSignedUrl(storagePath, 3600);

  if (error) {
    console.error('[supabase] signed url error raw:', error);

    throw new Error(
      `Supabase signed URL failed: ${error.message || JSON.stringify(error)} | bucket=${STORAGE_BUCKET} | path=${storagePath}`
    );
  }

  return data.signedUrl;
}

async function insertRun(runId, farm) {
  try {
    const { error } = await supabase.from('vic_download_runs').insert({
      id: runId,
      farm_id: farm.id,
      farm_name: farm.name,
      vic_username: farm.vic_username,
      status: 'running',
    });

    if (error) {
      console.error('[insertRun] Supabase error:', error);
    }
  } catch (err) {
    console.error('[insertRun] fatal:', err.message || err);
  }
}

async function updateRunSuccess(runId, storagePath) {
  try {
    const { error } = await supabase
      .from('vic_download_runs')
      .update({
        status: 'success',
        storage_path: storagePath,
        finished_at: new Date().toISOString(),
        error_message: null,
      })
      .eq('id', runId);

    if (error) {
      console.error('[updateRunSuccess] Supabase error:', error);
    }
  } catch (err) {
    console.error('[updateRunSuccess] fatal:', err.message || err);
  }
}

async function updateRunFailed(runId, errorMessage) {
  try {
    const { error } = await supabase
      .from('vic_download_runs')
      .update({
        status: 'failed',
        finished_at: new Date().toISOString(),
        error_message: errorMessage,
      })
      .eq('id', runId);

    if (error) {
      console.error('[updateRunFailed] Supabase error:', error);
    }
  } catch (err) {
    console.error('[updateRunFailed] fatal:', err.message || err);
  }
}

async function insertVicFile({ farmId, runId, storagePath, fileName }) {
  try {
    const { error } = await supabase.from('vic_files').insert({
      farm_id: farmId,
      run_id: runId,
      bucket: STORAGE_BUCKET,
      storage_path: storagePath,
      file_name: fileName,
    });

    if (error) {
      console.error('[insertVicFile] Supabase error:', error);
    }
  } catch (err) {
    console.error('[insertVicFile] fatal:', err.message || err);
  }
}

async function safeScreenshot(page, tmpDir, farmId, runId) {
  try {
    const shotPath = path.join(tmpDir, `${farmId || 'unknown'}-${runId}-error.png`);
    await page.screenshot({ path: shotPath, fullPage: true });
    return shotPath;
  } catch {
    return null;
  }
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

async function detectNoRecords(page) {
  const bodyText = await getBodyTextPreview(page);

  const noRecords =
    (bodyText || '').includes('Pagal pasirinktus paieškos kriterijus įrašų nerasta') ||
    (bodyText || '').includes('įrašų nerasta') ||
    (bodyText || '').includes('Duomenų nėra') ||
    (bodyText || '').includes('duomenų nėra') ||
    (bodyText || '').includes('Nėra duomenų') ||
    (bodyText || '').includes('nėra duomenų') ||
    (bodyText || '').includes('Nerasta') ||
    (bodyText || '').includes('nerasta');

  return {
    noRecords,
    bodyText,
  };
}

async function detectLoadedResults(page) {
  const bodyText = await getBodyTextPreview(page);

  const hasHolder =
    (bodyText || '').includes('Laikytojas') &&
    (bodyText || '').includes('Asmens/įmonės kodas');

  const hasHerd =
    (bodyText || '').includes('Banda') &&
    (bodyText || '').includes('Rūšis');

  const hasSummary =
    (bodyText || '').includes('Iš viso ataskaitoje') ||
    (bodyText || '').includes('Iš viso:');

  return {
    hasResults: hasHolder || hasHerd || hasSummary,
    bodyText,
  };
}

async function getPdfButtonState(page) {
  const pdfButton = page.locator('#printBtn').first();

  const exists = (await pdfButton.count().catch(() => 0)) > 0;

  if (!exists) {
    return {
      exists: false,
      visible: false,
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
    visible,
  };
}

async function fillInputAndTriggerEvents(page, selector, value) {
  const locator = page.locator(selector);

  await locator.waitFor({
    state: 'visible',
    timeout: 30000,
  });

  await locator.click({ timeout: 30000 });
  await locator.fill('');
  await locator.fill(String(value));

  await locator.evaluate((el) => {
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new Event('blur', { bubbles: true }));
  });

  await page.waitForTimeout(400);
}

async function fillClientCode(page, code) {
  const locator = page.locator('#AsmKodas');

  await locator.waitFor({
    state: 'visible',
    timeout: 30000,
  });

  await locator.click({ timeout: 30000 });
  await locator.fill('');
  await locator.type(String(code), { delay: 25 });

  await page.waitForTimeout(700);

  await locator.evaluate((el) => {
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
  });

  await locator.press('Enter').catch(() => null);
  await page.waitForTimeout(600);

  await locator.press('Tab').catch(() => null);
  await page.waitForTimeout(700);

  await locator.evaluate((el) => {
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new Event('blur', { bubbles: true }));
  });

  const actual = await locator.inputValue().catch(() => '');
  const expectedClean = String(code).replace(/\s+/g, '');
  const actualClean = String(actual).replace(/\s+/g, '');

  if (!actualClean || actualClean !== expectedClean) {
    throw new Error(`AsmKodas did not fill correctly. expected=${code} actual=${actual}`);
  }
}

async function fillLoginField(page, selector, value) {
  const locator = page.locator(selector);

  await locator.waitFor({
    state: 'visible',
    timeout: 30000,
  });

  await locator.click({ timeout: 30000 });

  await page.keyboard.press('Control+A').catch(() => null);
  await page.keyboard.press('Meta+A').catch(() => null);

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
  return await page.waitForFunction(
    () => {
      const bodyText = document.body.innerText || '';

      const hasGpsasLink =
        !!document.querySelector(
          '#ctl00_RptMenu_ctl05_CtlMenuNode_RptMenu_ctl00_ctl00_HplMenu'
        ) ||
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
  );
}

async function loginToVic(page, vicUsername, vicPassword) {
  const maxAttempts = 3;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    console.log(`[loginToVic] attempt ${attempt}/${maxAttempts}`);

    await page.goto(VIC_LOGIN_URL, {
      waitUntil: 'domcontentloaded',
      timeout: 60000,
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

    console.log(
      `[loginToVic] usernameFilled=${!!usernameValue} passwordFilled=${!!passwordValue}`
    );

    if (!usernameValue || !passwordValue) {
      if (attempt === maxAttempts) {
        throw new Error('Login fields were empty before submit.');
      }

      await page.waitForTimeout(1000);
      continue;
    }

    await Promise.all([
      page.waitForLoadState('domcontentloaded', { timeout: 60000 }).catch(() => null),
      page.locator('#ctl00_PublicPlaceHolder_LoginButton').click(),
    ]);

    await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null);
    await waitForLoginResult(page).catch(() => null);

    const bodyText = await getBodyTextPreview(page);
    const currentUrl = page.url();

    const gpsasLinkCount = await page
      .locator(
        '#ctl00_RptMenu_ctl05_CtlMenuNode_RptMenu_ctl00_ctl00_HplMenu, a[href="https://ise.vic.lt/GPSAS"], a[href*="/GPSAS"]'
      )
      .count()
      .catch(() => 0);

    const loggedIn =
      currentUrl.includes('/GPSAS') ||
      (bodyText || '').includes('Ūkinių gyvūnų registras') ||
      gpsasLinkCount > 0;

    if (loggedIn) {
      console.log('[loginToVic] login success');
      return;
    }

    console.log(`[loginToVic] login not confirmed, currentUrl=${currentUrl}`);
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
  const currentUrl = page.url();
  const bodyText = await getBodyTextPreview(page);

  const alreadyInsideGpsas =
    currentUrl.includes('/GPSAS') ||
    (bodyText || '').includes('Ūkinių gyvūnų registras');

  if (!alreadyInsideGpsas) {
    const gpsasLink = page
      .locator(
        '#ctl00_RptMenu_ctl05_CtlMenuNode_RptMenu_ctl00_ctl00_HplMenu, a[href="https://ise.vic.lt/GPSAS"], a[href*="/GPSAS"]'
      )
      .first();

    await gpsasLink.waitFor({
      state: 'visible',
      timeout: 60000,
    });

    await Promise.all([
      page.waitForLoadState('domcontentloaded', { timeout: 60000 }).catch(() => null),
      gpsasLink.click(),
    ]);

    await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null);
  }

  await page.goto(LIVE_ANIMALS_URL, {
    waitUntil: 'domcontentloaded',
    timeout: 60000,
  });

  await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null);

  await page.locator('#AsmKodas').waitFor({
    state: 'visible',
    timeout: 60000,
  });
}

async function waitForSearchResultOrState(page) {
  return await page.waitForFunction(
    () => {
      const bodyText = document.body.innerText || '';

      const pdfButton = document.querySelector('#printBtn');

      const hasVisiblePdfButton =
        !!pdfButton &&
        pdfButton.offsetParent !== null &&
        window.getComputedStyle(pdfButton).display !== 'none' &&
        window.getComputedStyle(pdfButton).visibility !== 'hidden';

      const hasHolder =
        bodyText.includes('Laikytojas') &&
        bodyText.includes('Asmens/įmonės kodas');

      const hasHerd =
        bodyText.includes('Banda') &&
        bodyText.includes('Rūšis');

      const hasSummary =
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
        hasHolder ||
        hasHerd ||
        hasSummary ||
        hasNoDataMessage ||
        hasValidationMessage
      );
    },
    null,
    { timeout: 90000 }
  );
}

async function clickSearch(page) {
  const searchButton = page
    .locator('#searchBtn, button:has-text("Ieškoti")')
    .first();

  await searchButton.waitFor({
    state: 'visible',
    timeout: 30000,
  });

  await Promise.all([
    page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null),
    searchButton.click(),
  ]);
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
    const bodyText = await getBodyTextPreview(page);

    throw new Error(
      `PDF button #printBtn exists but is hidden. Preview: ${(bodyText || '').slice(0, 700)}`
    );
  }

  const downloadPromise = page.waitForEvent('download', {
    timeout: 90000,
  });

  await pdfButton.click();

  return await downloadPromise;
}

async function runSearchAndInspect(page, farm, runId) {
  console.log(`[${runId}] Clicking search`);
  await clickSearch(page);

  await page.waitForTimeout(5000);

  await waitForSearchResultOrState(page);

  await page.waitForTimeout(1500);

  const noRecordCheck = await detectNoRecords(page);
  const resultCheck = await detectLoadedResults(page);
  const pdfState = await getPdfButtonState(page);

  const bodyText =
    resultCheck.bodyText ||
    noRecordCheck.bodyText ||
    (await getBodyTextPreview(page));

  console.log(
    `[${runId}] search inspected noRecords=${noRecordCheck.noRecords} hasResults=${resultCheck.hasResults} pdfExists=${pdfState.exists} pdfVisible=${pdfState.visible}`
  );

  return {
    noRecordCheck,
    resultCheck,
    pdfState,
    bodyText,
  };
}

async function processOneFarm(farm) {
  const browser = await getBrowser();

  const context = await browser.newContext({
    acceptDownloads: true,
    viewport: {
      width: 1600,
      height: 1000,
    },
  });

  const page = await context.newPage();

  const tmpDir = '/tmp/vic';
  await fs.mkdir(tmpDir, { recursive: true });

  const runId = crypto.randomUUID();
  const startedAt = new Date().toISOString();

  if (!farm.id) {
    await context.close().catch(() => null);

    return {
      success: false,
      stage: 'validation',
      error: 'Missing farm id.',
      run_id: runId,
    };
  }

  if (!farm.vic_username || !farm.vic_password) {
    await context.close().catch(() => null);

    return {
      farm_id: farm.id,
      farm_name: farm.name,
      success: false,
      stage: 'validation',
      error: 'Missing vet VIC credentials.',
      run_id: runId,
    };
  }

  if (!farm.client_personal_code) {
    await context.close().catch(() => null);

    return {
      farm_id: farm.id,
      farm_name: farm.name,
      success: false,
      stage: 'validation',
      error: 'Missing client_personal_code for #AsmKodas.',
      run_id: runId,
    };
  }

  let currentUrl = null;
  let localPath = null;
  let stage = 'started';
  let bodyTextPreviewAfterSearch = null;

  try {
    stage = 'insert_run';
    await insertRun(runId, farm);

    stage = 'login';
    console.log(`[${runId}] Logging into VIC as ${farm.vic_username}`);
    await loginToVic(page, farm.vic_username, farm.vic_password);

    stage = 'open_live_animals_page';
    console.log(`[${runId}] Opening live animals page`);
    await openLiveAnimalsPage(page);

    stage = 'fill_client_code';
    console.log(`[${runId}] Filling client code ${farm.client_personal_code}`);
    await fillClientCode(page, farm.client_personal_code);

    await page.waitForTimeout(700);

    stage = 'fill_search_date';
    console.log(`[${runId}] Filling search date ${farm.search_date}`);
    await fillInputAndTriggerEvents(page, '#PaieskosData', farm.search_date);

    stage = 'press_enter_date';
    await page.locator('#PaieskosData').press('Enter');

    await page.waitForTimeout(500);

    stage = 'click_h4_blur';
    await page
      .locator('h4:has-text("Gyvų gyvūnų sąrašas")')
      .click()
      .catch(() => null);

    await page.waitForTimeout(500);

    stage = 'wait_for_search_result';

    let inspection = await runSearchAndInspect(page, farm, runId);

    // Retry once if VIC did not load results and did not show no-record message.
    if (
      !inspection.noRecordCheck.noRecords &&
      !inspection.resultCheck.hasResults &&
      !inspection.pdfState.visible
    ) {
      console.log(`[${runId}] No clear result after first search, retrying once`);

      await page.waitForTimeout(1200);

      await fillClientCode(page, farm.client_personal_code);
      await page.waitForTimeout(700);

      await fillInputAndTriggerEvents(page, '#PaieskosData', farm.search_date);
      await page.locator('#PaieskosData').press('Enter').catch(() => null);

      await page
        .locator('h4:has-text("Gyvų gyvūnų sąrašas")')
        .click()
        .catch(() => null);

      await page.waitForTimeout(800);

      inspection = await runSearchAndInspect(page, farm, runId);
    }

    bodyTextPreviewAfterSearch = inspection.bodyText;

    if (inspection.noRecordCheck.noRecords) {
      stage = 'no_records_found';

      await updateRunFailed(
        runId,
        `[${stage}] No records found for client_personal_code=${farm.client_personal_code}`
      );

      currentUrl = page.url();

      await context.close().catch(() => null);

      return {
        farm_id: farm.id,
        farm_name: farm.name,
        client_personal_code: farm.client_personal_code,
        search_date: farm.search_date,
        vic_username: farm.vic_username,
        success: false,
        stage,
        error: 'No records found for selected search criteria.',
        current_url: currentUrl,
        body_text_preview_after_search: bodyTextPreviewAfterSearch,
        run_id: runId,
      };
    }

    if (!inspection.pdfState.visible) {
      stage = 'no_pdf_available';

      await updateRunFailed(
        runId,
        `[${stage}] PDF button hidden or unavailable. hasResults=${inspection.resultCheck.hasResults}, pdfExists=${inspection.pdfState.exists}`
      );

      currentUrl = page.url();

      await context.close().catch(() => null);

      return {
        farm_id: farm.id,
        farm_name: farm.name,
        client_personal_code: farm.client_personal_code,
        search_date: farm.search_date,
        vic_username: farm.vic_username,
        success: false,
        stage,
        error: `PDF button hidden or unavailable. hasResults=${inspection.resultCheck.hasResults}, pdfExists=${inspection.pdfState.exists}`,
        current_url: currentUrl,
        body_text_preview_after_search: bodyTextPreviewAfterSearch,
        run_id: runId,
      };
    }

    stage = 'download_pdf';
    console.log(`[${runId}] Downloading PDF`);
    const download = await downloadPdf(page);

    stage = 'save_pdf_local';
    const fileName = `live-animals-${farm.client_personal_code}-${farm.search_date}.pdf`;

    localPath = path.join(
      tmpDir,
      `${farm.id}-${runId}-${fileSafe(fileName)}`
    );

    await download.saveAs(localPath);

    const storagePath = `${farm.id}/${farm.search_date}/${fileName}`;

    stage = 'upload_supabase';
    console.log(`[${runId}] Uploading to Supabase ${storagePath}`);
    await uploadToSupabase(localPath, storagePath);

    stage = 'create_signed_url';
    const signedUrl = await createSignedUrl(storagePath);

    stage = 'insert_vic_file';
    await insertVicFile({
      farmId: farm.id,
      runId,
      storagePath,
      fileName,
    });

    stage = 'update_run_success';
    await updateRunSuccess(runId, storagePath);

    await fs.unlink(localPath).catch(() => null);
    localPath = null;

    currentUrl = page.url();

    await context.close().catch(() => null);

    return {
      farm_id: farm.id,
      farm_name: farm.name,
      client_personal_code: farm.client_personal_code,
      search_date: farm.search_date,
      vic_username: farm.vic_username,
      success: true,
      stage: 'done',
      storage_path: storagePath,
      file_name: fileName,
      signed_url: signedUrl,
      run_id: runId,
      started_at: startedAt,
      finished_at: new Date().toISOString(),
      current_url: currentUrl,
      body_text_preview_after_search: bodyTextPreviewAfterSearch,
    };
  } catch (err) {
    const errorMessage = err.message || String(err);

    try {
      currentUrl = page.url();
    } catch {
      currentUrl = null;
    }

    const bodyTextPreview = await getBodyTextPreview(page);
    const shotPath = await safeScreenshot(page, tmpDir, farm.id, runId);

    await updateRunFailed(runId, `[${stage}] ${errorMessage}`);

    if (localPath) {
      await fs.unlink(localPath).catch(() => null);
    }

    await context.close().catch(() => null);

    return {
      farm_id: farm.id,
      farm_name: farm.name,
      client_personal_code: farm.client_personal_code,
      search_date: farm.search_date,
      vic_username: farm.vic_username,
      success: false,
      stage,
      error: errorMessage,
      current_url: currentUrl,
      body_text_preview_after_search: bodyTextPreviewAfterSearch,
      body_text_preview: bodyTextPreview,
      screenshot_path: shotPath,
      run_id: runId,
    };
  }
}

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    version: 'vic-vet-login-client-code-stable-v8',
  });
});

app.post('/run-batch', requireInternalAuth, async (req, res) => {
  try {
    console.log('[run-batch] incoming body:', JSON.stringify({
      hasVet: !!req.body.vet,
      farmCount: Array.isArray(req.body.farms) ? req.body.farms.length : 0,
      searchDate: req.body.search_date || req.body.searchDate || null,
    }));

    const rawFarms = Array.isArray(req.body.farms) ? req.body.farms : [];

    const defaultVetCredentials = getVetCredentialsFromBody(req.body);

    const defaultSearchDate = normalizeDateValue(
      req.body.search_date || req.body.searchDate,
      getLithuaniaTodayDate()
    );

    const farms = rawFarms.map((farm) =>
      normalizeFarmInput(farm, defaultVetCredentials, defaultSearchDate)
    );

    const results = [];

    for (const farm of farms) {
      const result = await limit(() => processOneFarm(farm));
      results.push(result);
    }

    return res.json({
      ok: true,
      count: results.length,
      results,
    });
  } catch (err) {
    console.error('[run-batch] fatal error:', err);

    return res.status(500).json({
      ok: false,
      error: err.message || String(err),
      stack: err.stack || null,
    });
  }
});

app.listen(PORT, () => {
  console.log(`vic worker listening on ${PORT}`);
});
