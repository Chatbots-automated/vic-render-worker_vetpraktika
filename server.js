
const express = require('express');
const { chromium } = require('playwright');
const fs = require('fs/promises');
const fsSync = require('fs');
const path = require('path');
const crypto = require('crypto');

const VERSION = 'vic-single-pdf-stream-v9-disabled-holder-safe-selection';

console.log(`SERVER VERSION: ${VERSION}`);

const app = express();
app.use(express.json({ limit: '10mb' }));

const PORT = process.env.PORT || 3000;
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN;
const HEADLESS = String(process.env.PLAYWRIGHT_HEADLESS || 'true') === 'true';

const VIC_LOGIN_URL = 'https://ise.vic.lt/Public/Login.aspx';
const LIVE_ANIMALS_URL =
  'https://ise.vic.lt/GPSAS/Ataskaitos/GyvuGyvunuSarasas';

const TMP_DIR = '/tmp/vic-pdfs';

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
  return String(value ?? '').trim() || null;
}

function normalizeCompact(value) {
  return String(value ?? '').replace(/\s+/g, '').trim() || null;
}

function fileSafe(value) {
  return String(value || 'file')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[<>:"/\\|?*\x00-\x1F\x7F-\x9F]/g, '_')
    .replace(/[^\x20-\x7E]/g, '_')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 140) || 'file';
}

function buildContentDispositionFileName(filename) {
  const ascii = fileSafe(filename).replace(/["\\]/g, '_');
  const encoded = encodeURIComponent(filename)
    .replace(/['()]/g, escape)
    .replace(/\*/g, '%2A');

  return `attachment; filename="${ascii}"; filename*=UTF-8''${encoded}`;
}

function getLithuaniaTodayDate() {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Vilnius',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(new Date());

  const get = type => parts.find(p => p.type === type)?.value;

  return `${get('year')}-${get('month')}-${get('day')}`;
}

function normalizeDateValue(value, fallback) {
  const raw = String(value ?? '').trim();

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }

  const digits = raw.replace(/\D/g, '');

  if (digits.length === 8) {
    if (digits.startsWith('20')) {
      return `${digits.slice(0, 4)}-${digits.slice(4, 6)}-${digits.slice(6, 8)}`;
    }

    return `${digits.slice(4, 8)}-${digits.slice(2, 4)}-${digits.slice(0, 2)}`;
  }

  return fallback || getLithuaniaTodayDate();
}

function firstValue(...values) {
  for (const value of values) {
    const normalized = normalizeValue(value);
    if (normalized) return normalized;
  }
  return null;
}

function getVetCredentialsFromBody(body) {
  const vet = body.vet || {};

  return {
    vic_username: firstValue(
      body.vic_username,
      body.vicUsername,
      body.vet_username,
      body.vetUsername,
      vet.vic_username,
      vet.vicUsername,
      vet.username
    ),
    vic_password: firstValue(
      body.vic_password,
      body.vicPassword,
      body.vet_password,
      body.vetPassword,
      vet.vic_password,
      vet.vicPassword,
      vet.password
    )
  };
}

function normalizeFarmInput(farm, credentials, searchDate) {
  return {
    id: firstValue(farm.id, farm.farm_id),
    name: firstValue(farm.name, farm.farm_name),

    client_personal_code: normalizeCompact(
      firstValue(
        farm.client_personal_code,
        farm.clientPersonalCode,
        farm.personal_code,
        farm.personalCode,
        farm.holder_code,
        farm.holderCode,
        farm.farm_code,
        farm.farmCode,
        farm.code
      )
    ),

    vic_username: firstValue(
      farm.vet_vic_username,
      farm.vetVicUsername,
      farm.vic_username,
      credentials.vic_username
    ),

    vic_password: firstValue(
      farm.vet_vic_password,
      farm.vetVicPassword,
      farm.vic_password,
      credentials.vic_password
    ),

    search_date: normalizeDateValue(
      farm.search_date || farm.searchDate || searchDate,
      getLithuaniaTodayDate()
    ),

    skip_client_code_fill:
      farm.skip_client_code_fill === true ||
      farm.skipClientCodeFill === true ||
      String(farm.skip_client_code_fill || '').toLowerCase() === 'true' ||
      String(farm.skipClientCodeFill || '').toLowerCase() === 'true'
  };
}

async function createBrowser() {
  return chromium.launch({
    headless: HEADLESS,
    args: [
      '--disable-dev-shm-usage',
      '--no-sandbox',
      '--disable-setuid-sandbox'
    ]
  });
}

async function getBodyTextPreview(page) {
  try {
    return await page.evaluate(
      () => (document.body.innerText || '').slice(0, 5000)
    );
  } catch {
    return null;
  }
}

async function safeScreenshot(page, farmId, runId) {
  try {
    await fs.mkdir(TMP_DIR, { recursive: true });

    const filename = `${fileSafe(farmId)}-${runId}-error.png`;
    const screenshotPath = path.join(TMP_DIR, filename);

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

  await locator.click();
  await locator.fill('');
  await locator.type(String(value), { delay: 35 });

  await locator.evaluate(el => {
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new Event('blur', { bubbles: true }));
  });

  if (!await locator.inputValue()) {
    throw new Error(`Login field did not fill: ${selector}`);
  }
}

async function loginToVic(page, username, password) {
  const maxAttempts = 3;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    console.log(`[loginToVic] Attempt ${attempt}/${maxAttempts}`);

    await page.goto(VIC_LOGIN_URL, {
      waitUntil: 'domcontentloaded',
      timeout: 60000
    });

    await page.waitForTimeout(800);

    await fillLoginField(
      page,
      '#ctl00_PublicPlaceHolder_UserName',
      username
    );

    await page.waitForTimeout(300);

    await fillLoginField(
      page,
      '#ctl00_PublicPlaceHolder_Password',
      password
    );

    await page.waitForTimeout(300);

    await Promise.all([
      page.waitForLoadState('domcontentloaded', {
        timeout: 60000
      }).catch(() => null),

      page.locator(
        '#ctl00_PublicPlaceHolder_LoginButton'
      ).click()
    ]);

    await page.waitForLoadState('networkidle', {
      timeout: 30000
    }).catch(() => null);

    await page.waitForTimeout(1500);

    const currentUrl = page.url();
    const bodyText = await getBodyTextPreview(page);

    const loggedIn =
      currentUrl.includes('/GPSAS') ||
      (bodyText || '').includes('Ūkinių gyvūnų registras') ||
      (bodyText || '').includes('Registravimas') ||
      (bodyText || '').includes('Ataskaitos');

    if (loggedIn) {
      console.log('[loginToVic] Login successful');
      return;
    }

    console.log(
      `[loginToVic] Login not confirmed: ${currentUrl}`
    );

    if (attempt === maxAttempts) {
      throw new Error(
        `VIC login failed. Preview: ${(bodyText || '').slice(0, 500)}`
      );
    }

    await page.waitForTimeout(1500);
  }
}

async function selectVicProfileIfNeeded(page) {
  const url = page.url();
  const text = await getBodyTextPreview(page);

  const isSelection =
    url.includes('/GPSAS/Profile/ProfileSettings') ||
    (text || '').includes('Profilio pasirinkimas') ||
    (text || '').includes('Pasirinkite, kuriuo profiliu dirbsite');

  if (!isSelection) {
    console.log('[selectVicProfile] Profile selection not needed');
    return false;
  }

  console.log('[selectVicProfile] Selecting veterinary profile');

  const preferred = [
    /Privati\s+vet\.?\s+gydytoja/i,
    /Privatus\s+vet\.?\s+gydytojas/i,
    /Privati\s+veterinarijos\s+gydytoja/i,
    /Privatus\s+veterinarijos\s+gydytojas/i,
    /Vet\.?\s+gydytoja/i,
    /Vet\.?\s+gydytojas/i,
    /Veterinarijos\s+gydytoja/i,
    /Veterinarijos\s+gydytojas/i,
    /gydytoja/i,
    /gydytojas/i
  ];

  const excluded = /įsagai|isagai|laikytojams/i;

  async function stillOnSelection() {
    const url = page.url();
    const text = await getBodyTextPreview(page);

    return (
      url.includes('/GPSAS/Profile/ProfileSettings') ||
      (text || '').includes('Profilio pasirinkimas') ||
      (text || '').includes('Pasirinkite, kuriuo profiliu dirbsite')
    );
  }

  for (const regex of preferred) {
    const locators = [
      page.locator('a').filter({ hasText: regex }),
      page.locator('button').filter({ hasText: regex }),
      page.locator('tr').filter({ hasText: regex }),
      page.locator('td').filter({ hasText: regex }),
      page.locator('div').filter({ hasText: regex }),
      page.locator('span').filter({ hasText: regex })
    ];

    for (const locator of locators) {
      const count = Math.min(
        await locator.count().catch(() => 0),
        10
      );

      for (let i = 0; i < count; i++) {
        const candidate = locator.nth(i);

        if (!await candidate.isVisible().catch(() => false)) {
          continue;
        }

        const label = await candidate.evaluate(el =>
          (el.innerText || el.textContent || '')
            .replace(/\s+/g, ' ')
            .trim()
        ).catch(() => '');

        if (!label || excluded.test(label)) {
          continue;
        }

        try {
          await candidate.click({
            timeout: 3000
          });

          await page.waitForLoadState('domcontentloaded', {
            timeout: 15000
          }).catch(() => null);

          await page.waitForTimeout(1500);

          if (!await stillOnSelection()) {
            console.log('[selectVicProfile] Selected:', label);
            return true;
          }
        } catch {
          continue;
        }
      }
    }
  }

  throw new Error(
    'VIC profile selection shown, but no veterinary profile could be selected.'
  );
}

async function openLiveAnimalsPage(page) {
  await page.goto(LIVE_ANIMALS_URL, {
    waitUntil: 'domcontentloaded',
    timeout: 60000
  });

  await page.waitForLoadState('networkidle', {
    timeout: 30000
  }).catch(() => null);

  let text = await getBodyTextPreview(page);

  if (
    page.url().includes('/GPSAS/Profile/ProfileSettings') ||
    (text || '').includes('Profilio pasirinkimas')
  ) {
    await selectVicProfileIfNeeded(page);

    await page.goto(LIVE_ANIMALS_URL, {
      waitUntil: 'domcontentloaded',
      timeout: 60000
    });
  }

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

  await locator.click();
  await locator.fill('');
  await locator.type(String(value), { delay: 35 });

  await locator.evaluate(el => {
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
    el.dispatchEvent(new Event('blur', { bubbles: true }));
  });

  await page.waitForTimeout(500);

  const actual = await locator.inputValue();

  if (!actual) {
    throw new Error(`Input did not fill: ${selector}`);
  }

  return actual;
}

// VIC can disable #AsmKodas for certain account profiles.
// Never force-enable it. Check authorized herd selectors instead.

async function getHolderSearchControls(page) {
  return page.evaluate(() => {
    const holder = document.querySelector('#AsmKodas');

    const selects = Array.from(
      document.querySelectorAll('select')
    ).map(el => ({
      id: el.id || '',
      name: el.name || '',
      disabled: el.disabled,
      visible: !!el.getClientRects().length,
      value: el.value,

      options: Array.from(el.options).map(opt => ({
        value: opt.value,
        text: (opt.textContent || '').trim(),
        selected: opt.selected
      }))
    }));

    return {
      holder: holder
        ? {
            disabled: holder.disabled,
            value: holder.value
          }
        : null,
      selects
    };
  });
}

function exactHerdCode(text) {
  const match = String(text || '')
    .trim()
    .match(/^(\d{9,11})(?=\s|\(|$)/);

  return match ? match[1] : null;
}

async function selectMatchingHerdWhenCodeDisabled(page, code) {
  const state = await getHolderSearchControls(page);

  if (!state.holder?.disabled) {
    throw new Error(
      'Herd fallback called when holder input is enabled.'
    );
  }

  const matches = [];

  for (const select of state.selects) {
    if (select.disabled || !select.visible) {
      continue;
    }

    for (const option of select.options) {
      if (exactHerdCode(option.text) === code) {
        matches.push({ select, option });
      }
    }
  }

  if (matches.length !== 1) {
    const diagnostic = state.selects.map(el => ({
      id: el.id,
      name: el.name,
      disabled: el.disabled,
      visible: el.visible,
      options: el.options
        .map(option => option.text)
        .slice(0, 15)
    }));

    throw new Error(
      `VIC disabled #AsmKodas. Found ${matches.length} ` +
      `exact enabled dropdown matches for ${code}. ` +
      `Controls: ${JSON.stringify(diagnostic).slice(0, 3500)}`
    );
  }

  const match = matches[0];

  if (!match.select.id && !match.select.name) {
    throw new Error(
      'Matching herd selector has no stable ID or name.'
    );
  }

  const selector = match.select.id
    ? `select[id=${JSON.stringify(match.select.id)}]`
    : `select[name=${JSON.stringify(match.select.name)}]`;

  console.log(
    '[fillClientCode] Selecting herd:',
    match.option.text
  );

  await page.locator(selector).selectOption(
    match.option.value
  );

  await page.waitForTimeout(500);

  const selected = await page.locator(selector).evaluate(el => ({
    value: el.value,
    text: el.selectedOptions?.[0]?.textContent?.trim() || ''
  }));

  if (
    selected.value !== match.option.value ||
    exactHerdCode(selected.text) !== code
  ) {
    throw new Error(
      `Herd selection failed: ${JSON.stringify(selected)}`
    );
  }

  return {
    mode: 'herd_dropdown',
    code,
    herd: selected.text
  };
}

async function fillClientCode(page, code) {
  const cleanCode = normalizeCompact(code);

  if (!/^\d{9,11}$/.test(cleanCode || '')) {
    throw new Error(
      'Client personal/company code must contain 9–11 digits.'
    );
  }

  const locator = page.locator('#AsmKodas').first();

  await locator.waitFor({
    state: 'visible',
    timeout: 30000
  });

  await page.waitForTimeout(700);

  if (!await locator.isEnabled()) {
    return selectMatchingHerdWhenCodeDisabled(
      page,
      cleanCode
    );
  }

  for (let attempt = 1; attempt <= 3; attempt++) {
    console.log(
      `[fillClientCode] Attempt ${attempt}, code=${cleanCode}`
    );

    if (!await locator.isEnabled()) {
      return selectMatchingHerdWhenCodeDisabled(
        page,
        cleanCode
      );
    }

    await locator.fill(cleanCode);
    await locator.dispatchEvent('input');
    await locator.dispatchEvent('change');
    await locator.press('Tab').catch(() => null);

    await page.waitForTimeout(800);

    const actual = normalizeCompact(
      await locator.inputValue().catch(() => '')
    );

    if (actual === cleanCode) {
      return {
        mode: 'holder_input',
        code: cleanCode
      };
    }
  }

  throw new Error(
    `AsmKodas did not retain requested code ${cleanCode}`
  );
}

async function clickSearch(page) {
  const search = page.locator(
    '#searchBtn, button:has-text("Ieškoti"), input[value="Ieškoti"]'
  ).first();

  await search.waitFor({
    state: 'visible',
    timeout: 30000
  });

  await Promise.all([
    page.waitForLoadState('networkidle', {
      timeout: 60000
    }).catch(() => null),

    search.click()
  ]);

  await page.waitForTimeout(4000);
}

async function waitForSearchResultOrState(page) {
  await page.waitForFunction(() => {
    const text = document.body.innerText || '';
    const pdf = document.querySelector('#printBtn');

    const hasPdf =
      !!pdf &&
      pdf.offsetParent !== null &&
      getComputedStyle(pdf).display !== 'none' &&
      getComputedStyle(pdf).visibility !== 'hidden';

    const hasResults =
      (
        text.includes('Laikytojas') &&
        text.includes('Asmens/įmonės kodas')
      ) ||
      (
        text.includes('Bandos adresas') &&
        text.includes('Rūšis')
      ) ||
      text.includes('Iš viso ataskaitoje') ||
      text.includes('Iš viso:');

    const hasNoData =
      /įrašų nerasta|duomenų nėra|nėra duomenų|nerasta/i
        .test(text);

    const hasValidation =
      /privalomas|įveskite|neteisingas/i.test(text);

    return hasPdf || hasResults || hasNoData || hasValidation;
  }, null, {
    timeout: 90000
  }).catch(() => null);

  await page.waitForTimeout(1500);
}

async function detectNoRecords(page) {
  const text = await getBodyTextPreview(page);

  return {
    noRecords:
      /įrašų nerasta|duomenų nėra|nėra duomenų|nerasta/i
        .test(text || ''),
    bodyText: text
  };
}

async function detectLoadedResults(page) {
  const text = await getBodyTextPreview(page);

  const hasHolder =
    (text || '').includes('Laikytojas') &&
    (text || '').includes('Asmens/įmonės kodas');

  const hasHerd =
    (text || '').includes('Bandos adresas') &&
    (text || '').includes('Rūšis');

  const hasSummary =
    (text || '').includes('Iš viso ataskaitoje') ||
    (text || '').includes('Iš viso:');

  return {
    hasResults: hasHolder || hasHerd || hasSummary,
    bodyText: text
  };
}

async function getPdfButtonState(page) {
  const pdf = page.locator('#printBtn').first();

  const exists = await pdf.count().catch(() => 0) > 0;

  if (!exists) {
    return {
      exists: false,
      visible: false
    };
  }

  const visible = await pdf.evaluate(el => {
    const style = getComputedStyle(el);

    return (
      el.offsetParent !== null &&
      style.display !== 'none' &&
      style.visibility !== 'hidden'
    );
  }).catch(() => false);

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

  return {
    noRecordCheck,
    resultCheck,
    pdfState,
    bodyText:
      resultCheck.bodyText ||
      noRecordCheck.bodyText ||
      await getBodyTextPreview(page)
  };
}

async function downloadPdf(page) {
  const pdf = page.locator('#printBtn').first();

  const state = await getPdfButtonState(page);

  if (!state.exists || !state.visible) {
    throw new Error(
      'PDF button #printBtn is missing or hidden.'
    );
  }

  const downloadPromise = page.waitForEvent('download', {
    timeout: 90000
  });

  await pdf.click();

  return downloadPromise;
}

async function verifyPdf(localPath) {
  const handle = await fs.open(localPath, 'r');

  try {
    const buffer = Buffer.alloc(5);
    await handle.read(buffer, 0, 5, 0);

    if (buffer.toString('ascii') !== '%PDF-') {
      throw new Error(
        'Downloaded response is not a valid PDF file.'
      );
    }
  } finally {
    await handle.close();
  }
}

async function processFarmToPdf(farm) {
  const runId = crypto.randomUUID();
  const startedAt = new Date().toISOString();

  await fs.mkdir(TMP_DIR, { recursive: true });

  const base = {
    run_id: runId,
    farm_id: farm.id,
    farm_name: farm.name,
    client_personal_code: farm.client_personal_code,
    search_date: farm.search_date,
    vic_username: farm.vic_username,
    started_at: startedAt
  };

  function validationError(message) {
    return {
      ...base,
      success: false,
      stage: 'validation',
      error: message,
      finished_at: new Date().toISOString()
    };
  }

  if (!farm.id) {
    return validationError('Missing farm id.');
  }

  if (!farm.vic_username || !farm.vic_password) {
    return validationError('Missing VIC credentials.');
  }

  if (!farm.client_personal_code) {
    return validationError('Missing client_personal_code.');
  }

  if (!/^\d{9,11}$/.test(farm.client_personal_code)) {
    return validationError(
      'client_personal_code must contain 9–11 digits.'
    );
  }

  let browser = null;
  let context = null;
  let page = null;
  let stage = 'started';
  let localPath = null;

  try {
    browser = await createBrowser();

    context = await browser.newContext({
      acceptDownloads: true,
      viewport: {
        width: 1200,
        height: 800
      }
    });

    page = await context.newPage();

    stage = 'login';

    console.log(`[${runId}] Login: ${farm.vic_username}`);

    await loginToVic(
      page,
      farm.vic_username,
      farm.vic_password
    );

    stage = 'select_profile';

    await selectVicProfileIfNeeded(page);

    stage = 'open_live_animals_page';

    await openLiveAnimalsPage(page);

    stage = 'fill_client_code';

    if (farm.skip_client_code_fill) {
      const input = page.locator('#AsmKodas');

      const existingCode = normalizeCompact(
        await input.inputValue().catch(() => '')
      );

      if (existingCode !== farm.client_personal_code) {
        throw new Error(
          'Skipping client code entry is unsafe because ' +
          'the current holder code does not match the requested farm.'
        );
      }
    } else {
      const selection = await fillClientCode(
        page,
        farm.client_personal_code
      );

      console.log(
        `[${runId}] Holder selection:`,
        JSON.stringify(selection)
      );
    }

    stage = 'fill_search_date';

    await fillInputAndTriggerEvents(
      page,
      '#PaieskosData',
      farm.search_date
    );

    await page.locator('#PaieskosData')
      .press('Enter')
      .catch(() => null);

    await page.locator(
      'h4:has-text("Gyvų gyvūnų sąrašas")'
    ).click().catch(() => null);

    await page.waitForTimeout(500);

    stage = 'search';

    let inspection = await runSearchAndInspect(page);

    if (
      !inspection.noRecordCheck.noRecords &&
      !inspection.resultCheck.hasResults &&
      !inspection.pdfState.visible
    ) {
      console.log(`[${runId}] Retrying search once`);

      await page.waitForTimeout(1500);

      if (!farm.skip_client_code_fill) {
        await fillClientCode(
          page,
          farm.client_personal_code
        );
      }

      await fillInputAndTriggerEvents(
        page,
        '#PaieskosData',
        farm.search_date
      );

      await page.locator('#PaieskosData')
        .press('Enter')
        .catch(() => null);

      await page.locator(
        'h4:has-text("Gyvų gyvūnų sąrašas")'
      ).click().catch(() => null);

      inspection = await runSearchAndInspect(page);
    }

    if (inspection.noRecordCheck.noRecords) {
      return {
        ...base,
        success: false,
        stage: 'no_records_found',
        error: 'No records found.',
        current_url: page.url(),
        body_text_preview_after_search:
          (inspection.bodyText || '').slice(0, 3000),
        finished_at: new Date().toISOString()
      };
    }

    if (!inspection.pdfState.visible) {
      const screenshotPath = await safeScreenshot(
        page,
        farm.id,
        runId
      );

      return {
        ...base,
        success: false,
        stage: 'no_pdf_available',
        error: 'PDF button hidden or unavailable.',
        pdf_exists: inspection.pdfState.exists,
        has_results: inspection.resultCheck.hasResults,
        current_url: page.url(),
        body_text_preview_after_search:
          (inspection.bodyText || '').slice(0, 3000),
        screenshot_path: screenshotPath,
        finished_at: new Date().toISOString()
      };
    }

    stage = 'download_pdf';

    const download = await downloadPdf(page);

    const originalName = download.suggestedFilename() ||
      `live-animals-${farm.name || farm.id}-${farm.search_date}.pdf`;

    let safeName = fileSafe(originalName);

    if (!safeName.toLowerCase().endsWith('.pdf')) {
      safeName += '.pdf';
    }

    localPath = path.join(
      TMP_DIR,
      `${fileSafe(farm.id)}-${runId}-${safeName}`
    );

    await download.saveAs(localPath);
    await verifyPdf(localPath);

    const stat = await fs.stat(localPath);

    return {
      ...base,
      success: true,
      stage: 'done',
      file_name: path.basename(localPath),
      file_path: localPath,
      file_size: stat.size,
      current_url: page.url(),
      finished_at: new Date().toISOString()
    };
  } catch (err) {
    const bodyText = page
      ? await getBodyTextPreview(page).catch(() => null)
      : null;

    const screenshotPath = page
      ? await safeScreenshot(page, farm.id, runId)
      : null;

    if (localPath) {
      await fs.unlink(localPath).catch(() => null);
    }

    return {
      ...base,
      success: false,
      stage,
      error: err.message || String(err),
      current_url: page ? page.url() : null,
      body_text_preview: (bodyText || '').slice(0, 3000),
      screenshot_path: screenshotPath,
      finished_at: new Date().toISOString()
    };
  } finally {
    if (context) {
      await context.close().catch(() => null);
    }

    if (browser) {
      await browser.close().catch(() => null);
    }
  }
}

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    version: VERSION,
    headless: HEADLESS
  });
});

async function handleLiveAnimalsRequest(req, res) {
  let result = null;

  try {
    const credentials = getVetCredentialsFromBody(req.body);

    const searchDate = normalizeDateValue(
      req.body.search_date || req.body.searchDate,
      getLithuaniaTodayDate()
    );

    const rawFarm =
      req.body.farm ||
      (
        Array.isArray(req.body.farms) &&
        req.body.farms.length > 0
          ? req.body.farms[0]
          : null
      ) ||
      req.body;

    const farm = normalizeFarmInput(
      rawFarm,
      credentials,
      searchDate
    );

    result = await processFarmToPdf(farm);

    if (!result.success) {
      return res.status(422).json({
        ok: false,
        result
      });
    }

    const filename =
      `live-animals-${result.farm_name || result.farm_id}-` +
      `${result.client_personal_code}-${result.search_date}.pdf`;

    const stat = await fs.stat(result.file_path);

    res.setHeader('Content-Type', 'application/pdf');

    res.setHeader(
      'Content-Disposition',
      buildContentDispositionFileName(filename)
    );

    res.setHeader('Content-Length', stat.size);

    res.setHeader(
      'X-Farm-Id',
      fileSafe(result.farm_id || '')
    );

    res.setHeader(
      'X-Farm-Name',
      encodeURIComponent(result.farm_name || '')
    );

    res.setHeader(
      'X-Client-Personal-Code',
      fileSafe(result.client_personal_code || '')
    );

    res.setHeader(
      'X-Search-Date',
      fileSafe(result.search_date || '')
    );

    const stream = fsSync.createReadStream(result.file_path);

    const cleanup = async () => {
      if (result?.file_path) {
        await fs.unlink(result.file_path).catch(() => null);
      }
    };

    res.on('close', cleanup);

    stream.on('error', async err => {
      console.error('[PDF Stream Error]', err);

      await cleanup();

      if (!res.headersSent) {
        res.status(500).json({
          ok: false,
          error: err.message || String(err)
        });
      } else {
        res.destroy(err);
      }
    });

    return stream.pipe(res);
  } catch (err) {
    console.error('[VIC Worker Fatal Error]', err);

    if (result?.file_path) {
      await fs.unlink(result.file_path).catch(() => null);
    }

    if (res.headersSent) {
      return res.destroy(err);
    }

    return res.status(500).json({
      ok: false,
      error: err.message || String(err)
    });
  }
}

app.post(
  '/download-live-animals-pdf',
  requireInternalAuth,
  handleLiveAnimalsRequest
);

// Backward-compatible alias for existing workflows.
app.post(
  '/run-batch',
  requireInternalAuth,
  handleLiveAnimalsRequest
);

process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));

app.listen(PORT, () => {
  console.log(`VIC worker listening on port ${PORT}`);
});
