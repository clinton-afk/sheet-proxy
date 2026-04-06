// index.js
import express from 'express';
import bodyParser from 'body-parser';
import { google } from 'googleapis';
import fetch from 'node-fetch';

const app = express();
app.use(bodyParser.json());

const {
  SPREADSHEET_ID,
  API_SECRET,           // secret for Vercel → CloudRun calls
  MPESA_ENV = 'sandbox',// or 'production'
  MPESA_CONSUMER_KEY,
  MPESA_CONSUMER_SECRET,
  MPESA_SHORTCODE,      // Business Shortcode / Paybill or Till
  MPESA_PASSKEY         // Lipa na Mpesa Online Passkey (production)
} = process.env;

if (!SPREADSHEET_ID || !API_SECRET) {
  console.error('SPREADSHEET_ID and API_SECRET are required');
  process.exit(1);
}

const MPESA_BASE = MPESA_ENV === 'production'
  ? 'https://api.safaricom.co.ke'
  : 'https://sandbox.safaricom.co.ke';

function requireSecret(req, res, next) {
  const key = req.header('x-api-key') || req.query.api_key;
  if (!key || key !== API_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

async function getSheetsClient() {
  // On Cloud Run, google.auth.getClient will pick up the service account
  const auth = await google.auth.getClient({ scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
  return google.sheets({ version: 'v4', auth });
}

async function getValues(sheetName) {
  const sheets = await getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A1:Z1000`
  });
  return res.data.values || [];
}

async function appendRow(sheetName, row) {
  const sheets = await getSheetsClient();
  await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A1:Z1000`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [row] }
  });
}

async function updateCell(sheetName, rowIndex1Based, colIndex1Based, value) {
  const sheets = await getSheetsClient();
  const colLetter = (colIndex1Based > 0) ? (String.fromCharCode(64 + colIndex1Based)) : 'A';
  const range = `${sheetName}!${colLetter}${rowIndex1Based}`;
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [[value]] }
  });
}

async function findRowIndexByColumnValue(sheetName, columnHeader, matchValue) {
  const values = await getValues(sheetName);
  if (!values.length) return -1;
  const headers = values[0];
  const colIndex = headers.indexOf(columnHeader);
  if (colIndex === -1) return -1;
  for (let i = 1; i < values.length; i++) {
    if ((values[i][colIndex] || '') === matchValue) return i + 1; // 1-based row number
  }
  return -1;
}

/* --- sheet endpoints --- */

// GET /sheet?name=Master_Jobs
app.get('/sheet', requireSecret, async (req, res) => {
  try {
    const name = req.query.name;
    if (!name) return res.status(400).json({ error: 'Missing sheet name' });
    const values = await getValues(name);
    const headers = values[0] || [];
    const rows = values.slice(1).map(r => {
      const obj = {};
      headers.forEach((h, i) => (obj[h] = r[i] || ''));
      return obj;
    });
    res.json({ headers, rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// POST /append — { sheetName, row: [...] }
app.post('/append', requireSecret, async (req, res) => {
  try {
    const { sheetName, row } = req.body;
    if (!sheetName || !Array.isArray(row)) return res.status(400).json({ error: 'Missing sheetName or row' });
    await appendRow(sheetName, row);
    res.status(201).json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

/* --- M-Pesa STK helpers --- */

async function getMpesaToken() {
  if (!MPESA_CONSUMER_KEY || !MPESA_CONSUMER_SECRET) {
    throw new Error('Missing M-Pesa consumer credentials');
  }
  const tokenUrl = `${MPESA_BASE}/oauth/v1/generate?grant_type=client_credentials`;
  const basic = Buffer.from(`${MPESA_CONSUMER_KEY}:${MPESA_CONSUMER_SECRET}`).toString('base64');
  const r = await fetch(tokenUrl, {
    method: 'GET',
    headers: { Authorization: `Basic ${basic}` }
  });
  if (!r.ok) throw new Error('Failed to get mpesa token');
  const j = await r.json();
  return j.access_token;
}

function timestampNow() {
  const d = new Date();
  const yyyy = d.getFullYear().toString();
  const MM = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  return `${yyyy}${MM}${dd}${hh}${mm}${ss}`;
}

function password(shortcode, passkey, ts) {
  return Buffer.from(`${shortcode}${passkey}${ts}`).toString('base64');
}

/* POST /start-stk
   body: { phone: '2547xxxxxxx', amount: 1000, accountReference: 'MTD-20260401-007', description: 'Payment' }
*/
app.post('/start-stk', requireSecret, async (req, res) => {
  try {
    const { phone, amount, accountReference, description = 'Payment' } = req.body;
    if (!phone || !amount || !accountReference) return res.status(400).json({ error: 'Missing fields' });

    const token = await getMpesaToken();
    const ts = timestampNow();
    const pwd = password(MPESA_SHORTCODE, MPESA_PASSKEY, ts);

    const payload = {
      BusinessShortCode: MPESA_SHORTCODE,
      Password: pwd,
      Timestamp: ts,
      TransactionType: "CustomerPayBillOnline",
      Amount: String(amount),
      PartyA: phone,
      PartyB: MPESA_SHORTCODE,
      PhoneNumber: phone,
      CallBackURL: `${req.protocol}://${req.get('host')}/stk-callback`,
      AccountReference: accountReference,
      TransactionDesc: description
    };

    const r = await fetch(`${MPESA_BASE}/mpesa/stkpush/v1/processrequest`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    const j = await r.json();
    // j contains CheckoutRequestID etc
    return res.json(j);
  } catch (err) {
    console.error('start-stk error', err);
    res.status(500).json({ error: err.message });
  }
});

/* POST /stk-callback
   Daraja will POST a JSON body with Body.stkCallback
*/
app.post('/stk-callback', async (req, res) => {
  try {
    // Acknowledge quickly
    res.status(200).json({ receipt: 'ok' });

    const body = req.body || {};
    const cb = (body.Body && body.Body.stkCallback) ? body.Body.stkCallback : (body.stkCallback || body);
    if (!cb) {
      console.warn('STK callback missing expected structure', body);
      return;
    }

    const { MerchantRequestID, CheckoutRequestID, ResultCode, ResultDesc } = cb;
    // When success, CallbackMetadata.item will include Amount, MpesaReceiptNumber, PhoneNumber
    let amount = '';
    let mpesaReceipt = '';
    let phone = '';
    let accountRef = '';
    if (cb.CallbackMetadata && Array.isArray(cb.CallbackMetadata.Item)) {
      for (const item of cb.CallbackMetadata.Item) {
        const name = item.Name || item.name;
        if (name === 'Amount') amount = item.Value ?? item.value ?? '';
        if (name === 'MpesaReceiptNumber') mpesaReceipt = item.Value ?? item.value ?? '';
        if (name === 'PhoneNumber') phone = item.Value ?? item.value ?? '';
        if (name === 'AccountReference') accountRef = item.Value ?? item.value ?? '';
      }
    }

    // If accountRef not found in metadata, some implementations put it elsewhere — fallback to TransactionDesc
    if (!accountRef && cb.TransactionDesc) accountRef = cb.TransactionDesc;

    // Append to Payments sheet: [Timestamp, Request_ID, Phone, Amount, MpesaReceipt, MerchantRequestID, CheckoutRequestID, ResultCode, ResultDesc]
    const ts = new Date().toISOString();
    await appendRow('Payments', [ts, accountRef, phone, amount, mpesaReceipt, MerchantRequestID, CheckoutRequestID, ResultCode, ResultDesc]);

    // Update Master_Jobs Payment_Status to Paid if ResultCode == 0
    if (String(ResultCode) === '0' && accountRef) {
      // Find row in Master_Jobs where Request_ID == accountRef
      const values = await getValues('Master_Jobs');
      const headers = values[0] || [];
      const rows = values.slice(1);
      const requestCol = headers.indexOf('Request_ID');
      const paymentCol = headers.indexOf('Payment_Status');

      if (requestCol !== -1 && paymentCol !== -1) {
        for (let i = 0; i < rows.length; i++) {
          if ((rows[i][requestCol] || '') === accountRef) {
            const rowNumber = i + 2; // header row occupies row 1
            await updateCell('Master_Jobs', rowNumber, paymentCol + 1, 'Paid'); // column indices are 1-based
            // Optionally write Mpesa Receipt & Amount in other columns if you have them; attempt to find 'Amount_Paid' etc.
            const amountCol = headers.indexOf('Amount_Paid');
            const receiptCol = headers.indexOf('Payment_Transaction_ID');
            if (amountCol !== -1) await updateCell('Master_Jobs', rowNumber, amountCol + 1, amount);
            if (receiptCol !== -1) await updateCell('Master_Jobs', rowNumber, receiptCol + 1, mpesaReceipt);
            break;
          }
        }
      }
    }

  } catch (err) {
    console.error('stk-callback process error', err);
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Proxy listening on ${PORT}`));
