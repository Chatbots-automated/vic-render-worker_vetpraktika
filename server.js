const express = require('express');
const { chromium } = require('playwright');
const { createClient } = require('@supabase/supabase-js');
const pLimit = require('p-limit').default;
const fs = require('fs/promises');
const path = require('path');
const crypto = require('crypto');

const app = express();
app.use(express.json({ limit: '10mb' }));

const PORT = process.env.PORT || 3000;
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN;
const HEADLESS = String(process.env.PLAYWRIGHT_HEADLESS || 'true') === 'true';
const MAX_PARALLEL_CONTEXTS = Number(process.env.MAX_PARALLEL_CONTEXTS || 1);
const STORAGE_BUCKET = process.env.SUPABASE_STORAGE_BUCKET || 'vic-pdfs';

const VIC_LOGIN_URL = 'https://ise.vic.lt/Public/Login.aspx';

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
      normalizeValue(farm.client_personal_code) ||
      normalizeValue(farm.clientPersonalCode) ||
      normalizeValue(farm.personal_code) ||
      normalizeValue(farm.personalCode) ||
      normalizeValue(farm.holder_code) ||
      normalizeValue(farm.holderCode) ||
      normalizeValue(farm.farm_code) ||
      normalizeValue(farm.farmCode) ||
      normalizeValue(farm.code),

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

    search_date:
      normalizeValue(farm.search_date) ||
      normalizeValue(farm.searchDate) ||
      defaultSearchDate ||
      getLithuaniaTodayDate(),
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

  const { error } = await supabase.storage
    .from(STORAGE_BUCKET)
    .upload(storagePath, fileBuffer, {
      contentType: 'application/pdf',
      upsert: true,
    });

  if (error) throw error;
}

async function createSignedUrl(storagePath) {
  const { data, error } = await supabase.storage
    .from(STORAGE_BUCKET)
    .createSignedUrl(storagePath, 3600);

  if (error) throw error;

  return data.signedUrl;
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
}

async function loginToVic(page, vicUsername, vicPassword) {
  await page.goto(VIC_LOGIN_URL, {
    waitUntil: 'domcontentloaded',
    timeout: 60000,
  });

  await page.locator('#ctl00_PublicPlaceHolder_UserName').waitFor({
    state: 'visible',
    timeout: 30000,
  });

  await page.locator('#ctl00_PublicPlaceHolder_UserName').fill(vicUsername);
  await page.locator('#ctl00_PublicPlaceHolder_Password').fill(vicPassword);

  await Promise.all([
    page.waitForLoadState('domcontentloaded', { timeout: 60000 }).catch(() => null),
    page.locator('#ctl00_PublicPlaceHolder_LoginButton').click(),
  ]);

  await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null);
}

async function openLiveAnimalsPageViaMenu(page) {
  // 1. Click Ūkinių gyvūnų registras
  const gpsasLink = page.locator(
    '#ctl00_RptMenu_ctl05_CtlMenuNode_RptMenu_ctl00_ctl00_HplMenu, a[href="https://ise.vic.lt/GPSAS"]'
  ).first();

  await gpsasLink.waitFor({
    state: 'visible',
    timeout: 60000,
  });

  await Promise.all([
    page.waitForLoadState('domcontentloaded', { timeout: 60000 }).catch(() => null),
    gpsasLink.click(),
  ]);

  await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null);

  // 2. Click Meniu tab
  const menuTab = page.locator('a[data-toggle="tab"][href="#tabMeniu"], a:has-text("Meniu")').first();

  await menuTab.waitFor({
    state: 'visible',
    timeout: 60000,
  });

  await menuTab.click();

  await page.waitForTimeout(700);

  // 3. Click Gyvų gyvūnų sąrašas.
  // The link has target="_blank", so remove target to keep same page.
  const liveAnimalsLink = page.locator(
    'a[href="https://ise.vic.lt/GPSAS/Ataskaitos/GyvuGyvunuSarasas"], a:has-text("Gyvų gyvūnų sąrašas")'
  ).first();

  await liveAnimalsLink.waitFor({
    state: 'visible',
    timeout: 60000,
  });

  await liveAnimalsLink.evaluate((a) => {
    a.removeAttribute('target');
  });

  await Promise.all([
    page.waitForLoadState('domcontentloaded', { timeout: 60000 }).catch(() => null),
    liveAnimalsLink.click(),
  ]);

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

      const hasPdfButton = Array.from(
        document.querySelectorAll('button, a, span')
      ).some((el) => {
        const text = el.textContent || '';
        const visible =
          el.offsetParent !== null ||
          window.getComputedStyle(el).display !== 'none';

        return visible && text.includes('Pažyma (PDF)');
      });

      const hasNoDataMessage =
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

      return hasPdfButton || hasNoDataMessage || hasValidationMessage;
    },
    null,
    { timeout: 90000 }
  );
}

async function clickSearch(page) {
  const searchButton = page.locator(
    '#searchBtn, button:has-text("Ieškoti"), span.ladda-label:has-text("Ieškoti")'
  ).first();

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
  const pdfButton = page.locator(
    'button:has-text("Pažyma (PDF)"), a:has-text("Pažyma (PDF)"), span.ladda-label:has-text("Pažyma (PDF)")'
  ).first();

  const pdfButtonCount = await pdfButton.count();

  if (!pdfButtonCount) {
    throw new Error('PDF button was not found after search.');
  }

  await pdfButton.waitFor({
    state: 'visible',
    timeout: 30000,
  });

  const downloadPromise = page.waitForEvent('download', {
    timeout: 60000,
  });

  await pdfButton.click();

  return await downloadPromise;
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
      error: 'Missing client_personal_code for #AsmKodas.',
      run_id: runId,
    };
  }

  try {
    await supabase.from('vic_download_runs').insert({
      id: runId,
      farm_id: farm.id,
      farm_name: farm.name,
      vic_username: farm.vic_username,
      status: 'running',
    });

    // EXACT FLOW:
    // Login page -> login -> GPSAS -> Meniu -> Gyvų gyvūnų sąrašas.
    await loginToVic(page, farm.vic_username, farm.vic_password);

    await openLiveAnimalsPageViaMenu(page);

    // Enter client code.
    await fillInputAndTriggerEvents(page, '#AsmKodas', farm.client_personal_code);

    await page.waitForTimeout(700);

    // Enter today's/search date.
    await fillInputAndTriggerEvents(page, '#PaieskosData', farm.search_date);

    // Press Enter inside date field.
    await page.locator('#PaieskosData').press('Enter');

    await page.waitForTimeout(500);

    // Click off somewhere stable.
    await page.locator('h4:has-text("Gyvų gyvūnų sąrašas")').click().catch(() => null);

    await page.waitForTimeout(500);

    // Click Ieškoti.
    await clickSearch(page);

    await page.waitForTimeout(3000);

    await waitForSearchResultOrState(page);

    const bodyTextPreviewAfterSearch = await getBodyTextPreview(page);

    // Click Pažyma (PDF) and download.
    const download = await downloadPdf(page);

    const fileName = `live-animals-${farm.client_personal_code}-${farm.search_date}.pdf`;
    const localPath = path.join(
      tmpDir,
      `${farm.id}-${runId}-${fileSafe(fileName)}`
    );

    await download.saveAs(localPath);

    const storagePath = `${farm.id}/${farm.search_date}/${fileName}`;

    await uploadToSupabase(localPath, storagePath);

    const signedUrl = await createSignedUrl(storagePath);

    await supabase.from('vic_files').insert({
      farm_id: farm.id,
      run_id: runId,
      bucket: STORAGE_BUCKET,
      storage_path: storagePath,
      file_name: fileName,
    });

    await supabase
      .from('vic_download_runs')
      .update({
        status: 'success',
        storage_path: storagePath,
        finished_at: new Date().toISOString(),
        error_message: null,
      })
      .eq('id', runId);

    await fs.unlink(localPath).catch(() => null);
    await context.close().catch(() => null);

    return {
      farm_id: farm.id,
      farm_name: farm.name,
      client_personal_code: farm.client_personal_code,
      search_date: farm.search_date,
      vic_username: farm.vic_username,
      success: true,
      storage_path: storagePath,
      file_name: fileName,
      signed_url: signedUrl,
      run_id: runId,
      started_at: startedAt,
      finished_at: new Date().toISOString(),
      body_text_preview_after_search: bodyTextPreviewAfterSearch,
    };
  } catch (err) {
    const errorMessage = err.message || String(err);

    await supabase
      .from('vic_download_runs')
      .update({
        status: 'failed',
        finished_at: new Date().toISOString(),
        error_message: errorMessage,
      })
      .eq('id', runId)
      .catch(() => null);

    const shotPath = await safeScreenshot(page, tmpDir, farm.id, runId);
    const bodyTextPreview = await getBodyTextPreview(page);

    await context.close().catch(() => null);

    return {
      farm_id: farm.id,
      farm_name: farm.name,
      client_personal_code: farm.client_personal_code,
      search_date: farm.search_date,
      vic_username: farm.vic_username,
      success: false,
      error: errorMessage,
      current_url: page.url(),
      body_text_preview: bodyTextPreview,
      screenshot_path: shotPath,
      run_id: runId,
    };
  }
}

app.get('/health', (_req, res) => {
  res.json({ ok: true });
});

app.post('/run-batch', requireInternalAuth, async (req, res) => {
  const rawFarms = Array.isArray(req.body.farms) ? req.body.farms : [];

  const defaultVetCredentials = getVetCredentialsFromBody(req.body);

  const defaultSearchDate =
    normalizeValue(req.body.search_date) ||
    normalizeValue(req.body.searchDate) ||
    getLithuaniaTodayDate();

  const farms = rawFarms.map((farm) =>
    normalizeFarmInput(farm, defaultVetCredentials, defaultSearchDate)
  );

  const results = await Promise.all(
    farms.map((farm) => limit(() => processOneFarm(farm)))
  );

  res.json({
    ok: true,
    count: results.length,
    results,
  });
});

app.listen(PORT, () => {
  console.log(`vic worker listening on ${PORT}`);
});
