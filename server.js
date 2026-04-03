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
const MAX_PARALLEL_CONTEXTS = Number(process.env.MAX_PARALLEL_CONTEXTS || 3);
const STORAGE_BUCKET = process.env.SUPABASE_STORAGE_BUCKET || 'vic-pdfs';

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

function dateStamp() {
  return new Date().toISOString().slice(0, 10);
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

async function processOneFarm(farm) {
  const browser = await getBrowser();
  const context = await browser.newContext({ acceptDownloads: true });
  const page = await context.newPage();

  const tmpDir = '/tmp/vic';
  await fs.mkdir(tmpDir, { recursive: true });

  const runId = crypto.randomUUID();

  try {
    await supabase.from('vic_download_runs').insert({
      id: runId,
      farm_id: farm.id,
      farm_name: farm.name,
      vic_username: farm.vic_username,
      status: 'running',
    });

    await page.goto('https://is.vic.lt', { waitUntil: 'domcontentloaded', timeout: 60000 });

    await page.locator('#ctl00_PublicPlaceHolder_UserName').fill(farm.vic_username);
    await page.locator('#ctl00_PublicPlaceHolder_Password').fill(farm.vic_password);
    await page.locator('#ctl00_PublicPlaceHolder_LoginButton').click();

    await page.waitForLoadState('networkidle', { timeout: 60000 });

    await page.goto('https://ise.vic.lt/GPSAS', {
      waitUntil: 'domcontentloaded',
      timeout: 60000,
    });
    await page.waitForLoadState('networkidle', { timeout: 60000 });

    await page.goto('https://ise.vic.lt/GPSAS/Ataskaitos/GyvuGyvunuSarasas', {
      waitUntil: 'domcontentloaded',
      timeout: 60000,
    });
    await page.waitForLoadState('networkidle', { timeout: 60000 });

    const dateInput = page.locator('#PaieskosData');
    await dateInput.fill(String(Math.floor(Math.random() * 9) + 1));
    await dateInput.press('Enter');
    await page.locator('h4:has-text("Gyvų gyvūnų sąrašas")').click();

    await page.getByRole('button', { name: /Ieškoti/i }).click();
    await page.waitForLoadState('networkidle', { timeout: 60000 });

    const downloadPromise = page.waitForEvent('download', { timeout: 60000 });
    await page.getByRole('button', { name: /Pažyma \(PDF\)/i }).click();
    const download = await downloadPromise;

    const fileName = `live-animals-${dateStamp()}.pdf`;
    const localPath = path.join(tmpDir, `${farm.id}-${fileSafe(fileName)}`);
    await download.saveAs(localPath);

    const storagePath = `${farm.id}/${dateStamp()}/${fileName}`;
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

    await context.close();

    return {
      farm_id: farm.id,
      farm_name: farm.name,
      vic_username: farm.vic_username,
      success: true,
      storage_path: storagePath,
      file_name: fileName,
      signed_url: signedUrl,
      run_id: runId,
    };
  } catch (err) {
    await supabase
      .from('vic_download_runs')
      .update({
        status: 'failed',
        finished_at: new Date().toISOString(),
        error_message: err.message || String(err),
      })
      .eq('id', runId);

    try {
      const shotPath = path.join(tmpDir, `${farm.id}-error.png`);
      await page.screenshot({ path: shotPath, fullPage: true });
    } catch {}

    await context.close();

    return {
      farm_id: farm.id,
      farm_name: farm.name,
      vic_username: farm.vic_username,
      success: false,
      error: err.message || String(err),
      run_id: runId,
    };
  }
}

app.get('/health', (_req, res) => {
  res.json({ ok: true });
});

app.post('/run-batch', requireInternalAuth, async (req, res) => {
  const farms = Array.isArray(req.body.farms) ? req.body.farms : [];

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
