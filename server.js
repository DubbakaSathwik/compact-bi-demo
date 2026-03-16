const http = require('http');
const fs = require('fs');
const path = require('path');
const { Readable } = require('stream');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const xlsx = require('xlsx');
const csvParser = require('csv-parser');
const initSqlJs = require('sql.js');
const PDFDocument = require('pdfkit');

function loadEnvFile(filePath) {
  if (!fs.existsSync(filePath)) {
    return;
  }

  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split(/\r?\n/);
  lines.forEach(line => {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) {
      return;
    }

    const separatorIndex = trimmed.indexOf('=');
    if (separatorIndex <= 0) {
      return;
    }

    const key = trimmed.slice(0, separatorIndex).trim();
    const value = trimmed.slice(separatorIndex + 1).trim();
    if (!process.env[key]) {
      process.env[key] = value;
    }
  });
}

loadEnvFile(path.join(__dirname, '.env'));

const PORT = process.env.PORT || 3000;
const DATA_DIR = path.join(__dirname, 'data');
let geminiApiKey = process.env.GEMINI_API_KEY || 'PASTE_GEMINI_API_KEY_HERE';
let adminDemoMode = /^(true|1|yes|on)$/i.test(process.env.DEMO_MODE || 'false');
const GEMINI_MODEL = 'models/gemini-2.5-flash';
const MAX_ROWS = 5000;
const MAX_UPLOAD_BYTES = 15 * 1024 * 1024;
const TEMP_UPLOAD_TTL_MS = 6 * 60 * 60 * 1000;
const MAX_HISTORY = 8;
const MAX_QUERY_REPORT_HISTORY = 120;
const DEMO_DIFFICULTIES = ['simple', 'standard', 'complex'];
const MAX_SQL_REPAIR_ATTEMPTS = 2;
const DEMO_QUERY_COUNT = 8;
const CHART_TYPES = [
  'bar_chart',
  'horizontal_bar_chart',
  'line_chart',
  'area_chart',
  'step_line_chart',
  'pie_chart',
  'donut_chart',
  'radar_chart',
  'polar_area_chart',
  'scatter_plot',
  'bubble_chart',
  'lollipop_chart',
  'ranked_table',
  'table'
];
const JWT_SECRET = process.env.JWT_SECRET || 'change_this_jwt_secret_before_production';
const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN || '8h';
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const UPLOAD_REGISTRY_FILE = path.join(DATA_DIR, 'upload_registry.json');
const DATASET_TABLE_NAME = 'dataset';
const SUPPORTED_UPLOAD_EXTENSIONS = new Set(['.csv', '.xlsx', '.xls', '.json', '.tsv', '.txt']);
const INTERNAL_DATA_FILES = new Set(['users.json', 'upload_registry.json']);
const LEGACY_TEMP_FILE_REGEX = /(?:^|_)temp_\d{10,}(?:\.[a-z0-9]+)?$/i;
const conversations = new Map();
const queryHistoryStore = new Map();
const temporaryUploads = new Map();
const datasetIntelligenceStore = new Map();
const datasetCache = new Map();
let SQL_MODULE = null;

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

if (!fs.existsSync(USERS_FILE)) {
  fs.writeFileSync(USERS_FILE, '[]', 'utf8');
}

if (!fs.existsSync(UPLOAD_REGISTRY_FILE)) {
  fs.writeFileSync(UPLOAD_REGISTRY_FILE, '{}', 'utf8');
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function hasConfiguredApiKey() {
  return !!(geminiApiKey && geminiApiKey !== 'PASTE_GEMINI_API_KEY_HERE');
}

function isAiEnabled() {
  return hasConfiguredApiKey() && !adminDemoMode;
}

function toApiError(error, fallbackMessage = 'Something went wrong. Please try again.') {
  // User-facing validation errors (pre-validation, schema mismatch) — pass message through as-is
  if (error && error.userFacing) {
    return { statusCode: 422, message: error.message };
  }

  const raw = (error && error.message ? String(error.message) : '').trim();
  const lower = raw.toLowerCase();

  if (!raw) {
    return { statusCode: 500, message: fallbackMessage };
  }

  if (lower.includes('file query parameter is required') || lower.includes('file parameter required') || lower.includes('sqlquery is required') || lower.includes('filename and question are required') || lower.includes('name, email and password are required') || lower.includes('email and password are required') || lower.includes('please provide a valid email address') || lower.includes('password must be at least 8 characters long') || lower.includes('invalid json payload') || lower.includes('request too large') || lower.includes('invalid api key') || lower.includes('multipart/form-data')) {
    return { statusCode: 400, message: raw };
  }

  if (lower.includes('authorization token is required') || lower.includes('invalid or expired token') || lower.includes('authentication required')) {
    return { statusCode: 401, message: raw };
  }

  if (lower.includes('account already exists')) {
    return { statusCode: 409, message: raw };
  }

  if (lower.includes('selected csv file was not found') || lower.includes('no csv files') || lower.includes('csv file must contain')) {
    return {
      statusCode: 404,
      message: 'The relational schema is not present. Please verify the selected dataset and try again.'
    };
  }

  if (lower.includes('selected dataset file was not found') || lower.includes('excel file must contain') || lower.includes('json file must contain') || lower.includes('tsv file must contain') || lower.includes('txt file does not appear')) {
    return {
      statusCode: 404,
      message: 'The relational schema is not present. Please verify the selected dataset and try again.'
    };
  }

  if (lower.includes('unsupported file format')) {
    return {
      statusCode: 400,
      message: 'Unsupported file format.\n\nSupported formats:\nCSV, Excel, JSON, TSV, TXT'
    };
  }

  if (lower.includes('table') && lower.includes('does not match dataset')) {
    return {
      statusCode: 422,
      message: 'The relational schema does not match this query. Please verify the selected table and try again.'
    };
  }

  if (lower.includes('only select queries are allowed') || lower.includes('blocked keyword detected') || lower.includes('expected table') || lower.includes('unsupported') || lower.includes('failed to execute generated sql')) {
    return {
      statusCode: 422,
      message: 'We could not validate that query safely. Please rephrase and try again.'
    };
  }

  if (lower.includes('double-check failed')) {
    return {
      statusCode: 422,
      message: raw
    };
  }

  if (lower.includes('sql coverage mismatch')) {
    return {
      statusCode: 422,
      message: raw
    };
  }

  if (lower.includes('answer count mismatch')) {
    return {
      statusCode: 422,
      message: raw
    };
  }

  if (lower.includes('gemini request failed: 429') || lower.includes('resource_exhausted') || lower.includes('quota')) {
    return {
      statusCode: 503,
      message: 'AI service is temporarily busy. Please try again in a moment.'
    };
  }

  if (lower.includes('gemini request failed') || lower.includes('ai did not return json') || lower.includes('ai response missing sql_query')) {
    return {
      statusCode: 502,
      message: 'AI response could not be validated. Please try a clearer question.'
    };
  }

  if (lower.includes('upload is too large')) {
    return { statusCode: 413, message: raw };
  }

  return { statusCode: 500, message: fallbackMessage };
}

function readUsers() {
  try {
    const raw = fs.readFileSync(USERS_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_error) {
    return [];
  }
}

function writeUsers(users) {
  fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2), 'utf8');
}

function readUploadRegistry() {
  try {
    const raw = fs.readFileSync(UPLOAD_REGISTRY_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch (_error) {
    return {};
  }
}

function writeUploadRegistry(registry) {
  fs.writeFileSync(UPLOAD_REGISTRY_FILE, JSON.stringify(registry || {}, null, 2), 'utf8');
}

function cleanupUploadRegistry() {
  const registry = readUploadRegistry();
  const next = {};
  Object.entries(registry).forEach(([fileName, entry]) => {
    const safeName = path.basename(fileName || '');
    if (!safeName || safeName !== fileName) {
      return;
    }
    const filePath = path.join(DATA_DIR, safeName);
    if (fs.existsSync(filePath)) {
      next[safeName] = entry;
    }
  });
  writeUploadRegistry(next);
  return next;
}

function isLegacyTempDatasetFileName(fileName) {
  const safeName = path.basename(fileName || '');
  if (!safeName) {
    return false;
  }

  const ext = path.extname(safeName).toLowerCase();
  if (!SUPPORTED_UPLOAD_EXTENSIONS.has(ext)) {
    return false;
  }

  const base = path.basename(safeName, ext);
  return LEGACY_TEMP_FILE_REGEX.test(base);
}

function migrateLegacyPublicTempDatasets() {
  const registry = cleanupUploadRegistry();
  let changed = false;

  const files = fs.readdirSync(DATA_DIR)
    .filter(fileName => SUPPORTED_UPLOAD_EXTENSIONS.has(path.extname(fileName).toLowerCase()));

  files.forEach(fileName => {
    if (!isLegacyTempDatasetFileName(fileName)) {
      return;
    }

    const entry = registry[fileName];

    // Keep modern private uploads (signed-in users) even when they have temp suffixes.
    if (entry && entry.visibility === 'user' && entry.ownerUserId) {
      return;
    }

    const filePath = path.join(DATA_DIR, fileName);
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }

    if (entry) {
      delete registry[fileName];
      changed = true;
    }
  });

  if (changed) {
    writeUploadRegistry(registry);
  }
}

function extractSessionId(req, parsedUrl = null, fallback = '') {
  const fromHeader = (req.headers['x-session-id'] || '').toString().trim();
  if (fromHeader) {
    return fromHeader;
  }
  const fromQuery = parsedUrl ? (parsedUrl.searchParams.get('sessionId') || '').toString().trim() : '';
  if (fromQuery) {
    return fromQuery;
  }
  return (fallback || '').toString().trim();
}

function isProtectedInternalDataFile(fileName) {
  const safeName = path.basename(fileName || '').toLowerCase();
  return INTERNAL_DATA_FILES.has(safeName);
}

function isDatasetVisibleToContext(fileName, context = {}) {
  const safeName = path.basename(fileName || '');
  if (!safeName) {
    return false;
  }

  if (isProtectedInternalDataFile(safeName)) {
    return false;
  }

  cleanupTemporaryUploads();
  const temporaryEntry = temporaryUploads.get(safeName);
  if (temporaryEntry) {
    if (context.auth && temporaryEntry.ownerUserId) {
      return String(context.auth.sub) === String(temporaryEntry.ownerUserId);
    }
    if (!context.auth && temporaryEntry.ownerSessionId) {
      return String(context.sessionId || '') === String(temporaryEntry.ownerSessionId);
    }
    return false;
  }

  const filePath = path.join(DATA_DIR, safeName);
  if (!fs.existsSync(filePath)) {
    return false;
  }

  const registry = readUploadRegistry();
  const entry = registry[safeName];
  if (!entry) {
    return true;
  }

  if (entry.visibility === 'user') {
    if (!context.auth || !context.auth.sub) {
      return false;
    }
    return String(context.auth.sub) === String(entry.ownerUserId);
  }

  return true;
}

function ensureDatasetAccessible(fileName, context = {}) {
  if (!isDatasetVisibleToContext(fileName, context)) {
    throw new Error('Selected dataset file was not found in the data folder.');
  }
}

function sanitizeUser(user) {
  return {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role,
    created_at: user.created_at
  };
}

function createAuthToken(user) {
  return jwt.sign(
    { sub: user.id, email: user.email, role: user.role },
    JWT_SECRET,
    { expiresIn: JWT_EXPIRES_IN }
  );
}

function getBearerToken(req) {
  const auth = req.headers.authorization || '';
  const match = auth.match(/^Bearer\s+(.+)$/i);
  return match ? match[1].trim() : null;
}

function requireAuth(req) {
  const token = getBearerToken(req);
  if (!token) {
    throw new Error('Authorization token is required.');
  }
  try {
    return jwt.verify(token, JWT_SECRET);
  } catch (_error) {
    throw new Error('Invalid or expired token.');
  }
}

function getOptionalAuth(req) {
  const token = getBearerToken(req);
  if (!token) {
    return null;
  }
  try {
    return jwt.verify(token, JWT_SECRET);
  } catch (_error) {
    throw new Error('Invalid or expired token.');
  }
}

function sendFile(res, filePath, contentType) {
  fs.readFile(filePath, (error, content) => {
    if (error) {
      sendJson(res, 500, { error: 'Failed to read file.' });
      return;
    }
    res.writeHead(200, { 'Content-Type': contentType + '; charset=utf-8' });
    res.end(content);
  });
}

function collectJsonBody(req, maxBytes = 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => {
      body += chunk.toString('utf8');
      if (Buffer.byteLength(body, 'utf8') > maxBytes) {
        reject(new Error('Request too large.'));
        req.destroy();
      }
    });
    req.on('end', () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (error) {
        reject(new Error('Invalid JSON payload.'));
      }
    });
    req.on('error', reject);
  });
}

function collectRawBody(req, maxBytes = MAX_UPLOAD_BYTES) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;

    req.on('data', chunk => {
      chunks.push(chunk);
      size += chunk.length;
      if (size > maxBytes) {
        reject(new Error('Upload is too large. Max 15 MB.'));
        req.destroy();
      }
    });

    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function parseCsvLine(line) {
  const values = [];
  let current = '';
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === ',' && !inQuotes) {
      values.push(current.trim());
      current = '';
      continue;
    }

    current += char;
  }

  values.push(current.trim());
  return values;
}

function unwrapEmbeddedCsv(text) {
  const normalized = text.replace(/^\uFEFF/, '');
  const preOpen = normalized.match(/<pre[^>]*>/i);
  const preCloseIndex = normalized.toLowerCase().indexOf('</pre>');

  if (preOpen && preCloseIndex > -1) {
    const start = preOpen.index + preOpen[0].length;
    return normalized.slice(start, preCloseIndex);
  }

  return normalized;
}

function normalizeValue(raw) {
  if (raw == null) {
    return null;
  }
  if (raw instanceof Date && !Number.isNaN(raw.getTime())) {
    return raw.toISOString().slice(0, 10);
  }

  const text = String(raw).replace(/^\uFEFF/, '').trim();
  if (text === '') {
    return null;
  }

  const compactNumeric = text.replace(/,/g, '');
  if (/^-?\d+(\.\d+)?$/.test(compactNumeric) && !/^0\d+$/.test(compactNumeric)) {
    return Number(compactNumeric);
  }

  const parsedDate = Date.parse(text);
  if (!Number.isNaN(parsedDate) && /[-/]|\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/i.test(text)) {
    return new Date(parsedDate).toISOString().slice(0, 10);
  }

  return text;
}

function sanitizeColumnName(header, index, usedNames = new Set()) {
  const raw = String(header == null ? '' : header)
    .replace(/^\uFEFF/, '')
    .replace(/<[^>]+>/g, ' ')
    .trim();
  const base = raw.replace(/[^a-zA-Z0-9]+/g, '_').replace(/^_+|_+$/g, '') || `column_${index + 1}`;
  let candidate = base;
  let suffix = 2;
  while (usedNames.has(candidate.toLowerCase())) {
    candidate = `${base}_${suffix}`;
    suffix += 1;
  }
  usedNames.add(candidate.toLowerCase());
  return candidate;
}

function normalizeRows(rawRows) {
  const rowsArray = Array.isArray(rawRows) ? rawRows : [];
  const sourceHeaders = [];
  rowsArray.forEach(row => {
    if (!row || typeof row !== 'object' || Array.isArray(row)) {
      return;
    }
    Object.keys(row).forEach(key => {
      if (!sourceHeaders.includes(key)) {
        sourceHeaders.push(key);
      }
    });
  });

  const usedNames = new Set();
  const headerMap = sourceHeaders.map((header, index) => ({
    source: header,
    normalized: sanitizeColumnName(header, index, usedNames)
  }));
  const headers = headerMap.map(item => item.normalized);

  const rows = [];
  rowsArray.forEach(rawRow => {
    if (!rawRow || typeof rawRow !== 'object' || Array.isArray(rawRow)) {
      return;
    }

    const normalizedRow = {};
    let hasValue = false;
    headerMap.forEach(item => {
      const value = normalizeValue(rawRow[item.source]);
      normalizedRow[item.normalized] = value;
      if (value !== null && value !== '') {
        hasValue = true;
      }
    });

    if (hasValue) {
      rows.push(normalizedRow);
    }
  });

  return { headers, rows };
}

function parseDelimitedLine(line, delimiter) {
  if (delimiter !== ',') {
    return String(line || '').split(delimiter).map(cell => cell.trim());
  }
  return parseCsvLine(String(line || ''));
}

function parseDelimitedText(text, delimiter, emptyErrorMessage) {
  const cleaned = unwrapEmbeddedCsv(String(text || ''));
  const lines = cleaned.split(/\r?\n/).filter(line => line.trim() !== '');
  if (lines.length < 2) {
    throw new Error(emptyErrorMessage);
  }

  const headers = parseDelimitedLine(lines[0], delimiter);
  const rawRows = lines.slice(1).map(line => {
    const values = parseDelimitedLine(line, delimiter);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] == null ? null : values[index];
    });
    return row;
  });

  const normalized = normalizeRows(rawRows);
  return {
    headers: normalized.headers,
    rows: normalized.rows.slice(0, MAX_ROWS),
    truncated: normalized.rows.length > MAX_ROWS
  };
}

async function parseCsvFile(filePath) {
  return new Promise((resolve, reject) => {
    const rawRows = [];
    let truncated = false;

    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on('data', row => {
        if (rawRows.length < MAX_ROWS + 1) {
          rawRows.push(row);
        } else {
          truncated = true;
        }
      })
      .on('end', () => {
        const normalized = normalizeRows(rawRows);
        resolve({
          headers: normalized.headers,
          rows: normalized.rows.slice(0, MAX_ROWS),
          truncated: truncated || normalized.rows.length > MAX_ROWS
        });
      })
      .on('error', reject);
  });
}

function parseExcelFile(filePath) {
  const workbook = xlsx.readFile(filePath, { cellDates: true });
  const sheetName = workbook.SheetNames[0];
  if (!sheetName) {
    throw new Error('Excel file must contain at least one sheet.');
  }
  const worksheet = workbook.Sheets[sheetName];
  const rawRows = xlsx.utils.sheet_to_json(worksheet, { defval: null, raw: false });
  const normalized = normalizeRows(rawRows);
  return {
    headers: normalized.headers,
    rows: normalized.rows.slice(0, MAX_ROWS),
    truncated: normalized.rows.length > MAX_ROWS
  };
}

function parseJsonFile(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  const parsed = JSON.parse(content);
  let rawRows = [];

  if (Array.isArray(parsed)) {
    rawRows = parsed;
  } else if (parsed && typeof parsed === 'object') {
    const firstArrayEntry = Object.values(parsed).find(value => Array.isArray(value));
    if (Array.isArray(firstArrayEntry)) {
      rawRows = firstArrayEntry;
    } else {
      rawRows = [parsed];
    }
  }

  const normalized = normalizeRows(rawRows);
  if (!normalized.rows.length) {
    throw new Error('JSON file must contain an array of objects or an object with tabular records.');
  }
  return {
    headers: normalized.headers,
    rows: normalized.rows.slice(0, MAX_ROWS),
    truncated: normalized.rows.length > MAX_ROWS
  };
}

function detectTxtDelimiter(text) {
  const lines = String(text || '').split(/\r?\n/).filter(line => line.trim() !== '');
  if (!lines.length) {
    return null;
  }
  const sample = lines[0];
  if (sample.includes('\t')) return '\t';
  if (sample.includes(',')) return ',';
  if (sample.includes('|')) return '|';
  if (sample.includes(';')) return ';';
  if (/\S+\s{2,}\S+/.test(sample)) return /\s{2,}/;
  return null;
}

function parseTxtFile(filePath) {
  const text = fs.readFileSync(filePath, 'utf8');
  const delimiter = detectTxtDelimiter(text);
  if (!delimiter) {
    throw new Error('TXT file does not appear to contain tabular data.');
  }

  if (delimiter instanceof RegExp) {
    const lines = text.split(/\r?\n/).filter(line => line.trim() !== '');
    if (lines.length < 2) {
      throw new Error('TXT file must contain a header row and at least one data row.');
    }
    const headers = lines[0].trim().split(delimiter);
    const rawRows = lines.slice(1).map(line => {
      const values = line.trim().split(delimiter);
      const row = {};
      headers.forEach((header, index) => {
        row[header] = values[index] == null ? null : values[index];
      });
      return row;
    });
    const normalized = normalizeRows(rawRows);
    return {
      headers: normalized.headers,
      rows: normalized.rows.slice(0, MAX_ROWS),
      truncated: normalized.rows.length > MAX_ROWS
    };
  }

  return parseDelimitedText(text, delimiter, 'TXT file must contain a header row and at least one data row.');
}

async function parseDatasetFile(filePath, fileName) {
  const extension = path.extname(fileName || filePath).toLowerCase();
  if (!SUPPORTED_UPLOAD_EXTENSIONS.has(extension)) {
    throw new Error('Unsupported file format.\n\nSupported formats:\nCSV, Excel, JSON, TSV, TXT');
  }

  if (extension === '.csv') {
    return parseCsvFile(filePath);
  }
  if (extension === '.xlsx' || extension === '.xls') {
    return parseExcelFile(filePath);
  }
  if (extension === '.json') {
    return parseJsonFile(filePath);
  }
  if (extension === '.tsv') {
    return parseDelimitedText(fs.readFileSync(filePath, 'utf8'), '\t', 'TSV file must contain a header row and at least one data row.');
  }
  return parseTxtFile(filePath);
}

function inferColumnType(values, headerName = '') {
  const filtered = values.filter(value => value !== null && value !== undefined && value !== '');
  if (filtered.length === 0) {
    return 'TEXT';
  }
  const numeric = filtered.every(value => typeof value === 'number');
  if (numeric) {
    return 'NUMBER';
  }
  const asStrings = filtered.map(value => String(value).trim());
  const isoLike = asStrings.filter(value => /^\d{4}-\d{2}-\d{2}(?:[ T].*)?$/.test(value)).length;
  const slashLike = asStrings.filter(value => /^\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}$/.test(value)).length;
  const monthNameLike = asStrings.filter(value => /^[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}$/.test(value)).length;
  const dateByValues = (isoLike + slashLike + monthNameLike) / asStrings.length >= 0.7;
  const dateByHeader = /date|time|month|year|quarter/i.test(String(headerName || ''));
  const dateLike = dateByValues || (dateByHeader && asStrings.length > 0);
  if (dateLike) {
    return 'DATE';
  }
  return 'TEXT';
}

function inferSchema(headers, rows, fileName) {
  const columns = headers.map(header => {
    const values = rows.map(row => row[header]);
    return {
      name: header,
      type: inferColumnType(values, header)
    };
  });

  return {
    tableName: DATASET_TABLE_NAME,
    columns
  };
}

function tokenizeSemantic(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9_\s]/g, ' ')
    .replace(/_/g, ' ')
    .split(/\s+/)
    .map(token => token.trim())
    .filter(token => token.length >= 3);
}

function normalizeColumnPhrase(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/_/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function resolveColumnByPhrase(phrase, schema, preferNonNumeric = true) {
  const normalizedPhrase = normalizeColumnPhrase(phrase);
  if (!normalizedPhrase || !schema || !Array.isArray(schema.columns)) {
    return null;
  }

  const phraseTokens = new Set(tokenizeSemantic(normalizedPhrase));
  const pool = preferNonNumeric
    ? schema.columns.filter(column => column.type !== 'NUMBER')
    : schema.columns;

  const scored = pool
    .map(column => {
      const normalizedColumn = normalizeColumnPhrase(column.name);
      const columnTokens = new Set(tokenizeSemantic(column.name));
      let score = 0;

      if (normalizedColumn === normalizedPhrase) {
        score += 100;
      }
      if (normalizedColumn.includes(normalizedPhrase) || normalizedPhrase.includes(normalizedColumn)) {
        score += 40;
      }

      let overlap = 0;
      phraseTokens.forEach(token => {
        if (columnTokens.has(token)) {
          overlap += 1;
        }
      });

      score += overlap * 8;
      if (phraseTokens.size > 0 && overlap === phraseTokens.size) {
        score += 20;
      }

      return { column, score };
    })
    .filter(item => item.score > 0)
    .sort((a, b) => b.score - a.score);

  return scored.length ? scored[0].column : null;
}

function analyzeSchemaMetadata(schema, rows, fileName) {
  const rowCount = Array.isArray(rows) ? rows.length : 0;
  const numericColumns = [];
  const dateColumns = [];
  const categoricalColumns = [];
  const textColumns = [];

  schema.columns.forEach(column => {
    const values = rows.map(row => row[column.name]).filter(value => value != null && value !== '');
    const uniqueCount = new Set(values.map(value => String(value))).size;
    const uniqueRatio = values.length > 0 ? uniqueCount / values.length : 1;
    const avgLength = values.length > 0
      ? values.reduce((acc, value) => acc + String(value).length, 0) / values.length
      : 0;
    const columnNameLower = column.name.toLowerCase();
    const idLike = /(^|_)id$/.test(columnNameLower);

    if (column.type === 'NUMBER') {
      if (idLike) {
        categoricalColumns.push(column.name);
      } else {
        numericColumns.push(column.name);
      }
      return;
    }

    if (column.type === 'DATE') {
      dateColumns.push(column.name);
      return;
    }

    const looksCategoricalByName = /type|category|segment|channel|region|country|city|status|group|tier|class|gender|source|medium|audience|campaign/i.test(columnNameLower);
    const lowCardinality = uniqueCount <= Math.max(12, Math.floor(rowCount * 0.2));
    const shortText = avgLength > 0 && avgLength <= 30;

    if (looksCategoricalByName || (lowCardinality && shortText) || uniqueRatio <= 0.25) {
      categoricalColumns.push(column.name);
    } else {
      textColumns.push(column.name);
    }
  });

  const metricCandidates = numericColumns.filter(name => !/(^|_)id$/i.test(name));
  const dimensionCandidates = [...categoricalColumns, ...dateColumns];
  const keywordColumnMap = {};
  const categoricalValueMap = {};

  schema.columns.forEach(column => {
    const tokens = new Set(tokenizeSemantic(column.name));
    // basic singular/plural normalization for query matching
    Array.from(tokens).forEach(token => {
      if (token.endsWith('s') && token.length > 3) {
        tokens.add(token.slice(0, -1));
      } else {
        tokens.add(token + 's');
      }
    });
    tokens.forEach(token => {
      if (!keywordColumnMap[token]) {
        keywordColumnMap[token] = [];
      }
      if (!keywordColumnMap[token].includes(column.name)) {
        keywordColumnMap[token].push(column.name);
      }
    });
  });

  [...categoricalColumns, ...dateColumns].forEach(columnName => {
    const values = rows
      .map(row => row[columnName])
      .filter(value => value != null && value !== '')
      .map(value => String(value).trim());
    const unique = [...new Set(values)];
    categoricalValueMap[columnName] = unique.slice(0, 200);
  });

  return {
    dataset_key: `${fileName}::${schema.tableName}`,
    table_name: schema.tableName,
    numeric_columns: numericColumns,
    categorical_columns: categoricalColumns,
    date_columns: dateColumns,
    text_columns: textColumns,
    metric_candidates: metricCandidates,
    dimension_candidates: dimensionCandidates,
    keyword_column_map: keywordColumnMap,
    categorical_value_map: categoricalValueMap
  };
}

function semantic_mapper(schema, rows, fileName) {
  return analyzeSchemaMetadata(schema, rows, fileName);
}

function attachSchemaIntelligence(fileName, schema, rows) {
  const safeName = path.basename(fileName || schema.tableName || 'dataset');
  const cacheKey = `${safeName}::${schema.tableName}`;
  const fresh = semantic_mapper(schema, rows, safeName);
  datasetIntelligenceStore.set(cacheKey, fresh);
  schema.intelligence = fresh;
  return fresh;
}

function getSchemaIntelligence(schema) {
  if (schema && schema.intelligence) {
    return schema.intelligence;
  }
  return {
    numeric_columns: schema.columns.filter(column => column.type === 'NUMBER').map(column => column.name),
    categorical_columns: schema.columns.filter(column => column.type === 'TEXT').map(column => column.name),
    date_columns: schema.columns.filter(column => column.type === 'DATE').map(column => column.name),
    text_columns: schema.columns.filter(column => column.type === 'TEXT').map(column => column.name),
    metric_candidates: schema.columns.filter(column => column.type === 'NUMBER').map(column => column.name),
    dimension_candidates: schema.columns.filter(column => column.type !== 'NUMBER').map(column => column.name),
    keyword_column_map: {}
  };
}

function cleanupTemporaryUploads() {
  const now = Date.now();
  for (const [fileName, entry] of temporaryUploads.entries()) {
    if (!entry || !entry.uploadedAt || now - entry.uploadedAt > TEMP_UPLOAD_TTL_MS) {
      temporaryUploads.delete(fileName);
      datasetCache.delete(fileName);
    }
  }
}

function reserveUploadFileName(originalFileName) {
  const safe = path.basename(originalFileName || 'upload.csv');
  const ext = path.extname(safe).toLowerCase();
  if (!SUPPORTED_UPLOAD_EXTENSIONS.has(ext)) {
    throw new Error('Unsupported file format.\n\nSupported formats:\nCSV, Excel, JSON, TSV, TXT');
  }
  const base = path.basename(safe, path.extname(safe)).replace(/[^a-zA-Z0-9_\-]/g, '_') || 'upload';

  const diskExists = fs.existsSync(path.join(DATA_DIR, base + ext));
  const tempExists = temporaryUploads.has(base + ext);
  if (!diskExists && !tempExists) {
    return base + ext;
  }

  return base + '_temp_' + Date.now() + ext;
}

function getDatasetFiles(context = {}) {
  cleanupTemporaryUploads();
  const tempFiles = Array.from(temporaryUploads.entries())
    .filter(([_fileName, entry]) => {
      if (context.auth && entry && entry.ownerUserId) {
        return String(context.auth.sub) === String(entry.ownerUserId);
      }
      if (!context.auth && entry && entry.ownerSessionId) {
        return String(context.sessionId || '') === String(entry.ownerSessionId);
      }
      return false;
    })
    .map(([fileName]) => fileName);

  const diskFiles = fs.readdirSync(DATA_DIR)
    .filter(fileName => SUPPORTED_UPLOAD_EXTENSIONS.has(path.extname(fileName).toLowerCase()))
    .filter(fileName => isDatasetVisibleToContext(fileName, context));

  return [...tempFiles, ...diskFiles.filter(fileName => !temporaryUploads.has(fileName))];
}

function escapeSqlIdentifier(identifier) {
  return `"${String(identifier || '').replace(/"/g, '""')}"`;
}

function buildSqliteDatabase(schema, rows) {
  if (!SQL_MODULE) {
    throw new Error('SQLite engine is not ready yet.');
  }

  const db = new SQL_MODULE.Database();
  const columnDefinitions = schema.columns.map(column => {
    const sqlType = column.type === 'NUMBER' ? 'REAL' : 'TEXT';
    return `${escapeSqlIdentifier(column.name)} ${sqlType}`;
  }).join(', ');
  db.run(`CREATE TABLE ${escapeSqlIdentifier(schema.tableName)} (${columnDefinitions})`);

  if (rows.length > 0) {
    const columnList = schema.columns.map(column => escapeSqlIdentifier(column.name)).join(', ');
    const placeholders = schema.columns.map(() => '?').join(', ');
    const statement = db.prepare(`INSERT INTO ${escapeSqlIdentifier(schema.tableName)} (${columnList}) VALUES (${placeholders})`);
    rows.forEach(row => {
      statement.run(schema.columns.map(column => row[column.name] == null ? null : row[column.name]));
    });
    statement.free();
  }

  return db;
}

async function loadDataset(fileName, context = null) {
  const safeName = path.basename(fileName);
  cleanupTemporaryUploads();

  if (isProtectedInternalDataFile(safeName)) {
    throw new Error('Selected dataset file was not found in the data folder.');
  }

  if (context) {
    ensureDatasetAccessible(safeName, context);
  }

  const temporaryEntry = temporaryUploads.get(safeName);
  if (temporaryEntry && (typeof temporaryEntry.content === 'string' || typeof temporaryEntry.contentBase64 === 'string')) {
    const extension = path.extname(safeName).toLowerCase();
    const tempFilePath = path.join(DATA_DIR, `__temp_${Date.now()}${extension || '.tmp'}`);
    if (typeof temporaryEntry.contentBase64 === 'string') {
      fs.writeFileSync(tempFilePath, Buffer.from(temporaryEntry.contentBase64, 'base64'));
    } else {
      fs.writeFileSync(tempFilePath, temporaryEntry.content);
    }
    try {
      const parsedTemp = await parseDatasetFile(tempFilePath, safeName);
      const schemaTemp = inferSchema(parsedTemp.headers, parsedTemp.rows, safeName);
      attachSchemaIntelligence(safeName, schemaTemp, parsedTemp.rows);
      const dataset = { fileName: safeName, ...parsedTemp, schema: schemaTemp, sqliteDb: buildSqliteDatabase(schemaTemp, parsedTemp.rows) };
      return dataset;
    } finally {
      if (fs.existsSync(tempFilePath)) {
        fs.unlinkSync(tempFilePath);
      }
    }
  }

  const filePath = path.join(DATA_DIR, safeName);
  if (!fs.existsSync(filePath)) {
    throw new Error('Selected dataset file was not found in the data folder.');
  }

  const stats = fs.statSync(filePath);
  const cacheKey = `${safeName}:${stats.mtimeMs}:${stats.size}`;
  const cached = datasetCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const parsed = await parseDatasetFile(filePath, safeName);
  const schema = inferSchema(parsed.headers, parsed.rows, safeName);
  attachSchemaIntelligence(safeName, schema, parsed.rows);
  const dataset = { fileName: safeName, ...parsed, schema, sqliteDb: buildSqliteDatabase(schema, parsed.rows) };
  datasetCache.clear();
  datasetCache.set(cacheKey, dataset);
  return dataset;
}

const uploadSingleDataset = multer({
  storage: multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, DATA_DIR),
    filename: (_req, file, cb) => {
      try {
        cb(null, reserveUploadFileName(file.originalname || 'upload.csv'));
      } catch (error) {
        cb(error);
      }
    }
  }),
  limits: { fileSize: MAX_UPLOAD_BYTES },
  fileFilter: (_req, file, cb) => {
    const extension = path.extname(file.originalname || '').toLowerCase();
    if (!SUPPORTED_UPLOAD_EXTENSIONS.has(extension)) {
      cb(new Error('Unsupported file format.\n\nSupported formats:\nCSV, Excel, JSON, TSV, TXT'));
      return;
    }
    cb(null, true);
  }
}).single('file');

function runUploadMiddleware(req, res) {
  return new Promise((resolve, reject) => {
    uploadSingleDataset(req, res, error => {
      if (error) {
        reject(error);
        return;
      }
      resolve(req.file || null);
    });
  });
}

function findQuestionMatchedColumn(question, schema, predicate = () => true) {
  const lower = String(question || '').toLowerCase();
  const normalizedQuestion = normalizeColumnPhrase(question);
  return schema.columns.find(column => {
    if (!predicate(column)) {
      return false;
    }
    const raw = String(column.name || '').toLowerCase();
    const spaced = raw.replace(/_/g, ' ');
    const normalizedColumn = normalizeColumnPhrase(column.name);
    return lower.includes(raw) || lower.includes(spaced) || (normalizedColumn && normalizedQuestion.includes(normalizedColumn));
  }) || null;
}

const METRIC_LANGUAGE_MAP = {
  sales: ['revenue', 'amount', 'income'],
  income: ['revenue', 'amount'],
  profit: ['revenue', 'margin', 'roi'],
  spend: ['cost', 'acquisition_cost'],
  cost: ['acquisition_cost', 'cost'],
  engagement: ['engagement_score', 'engagement'],
  'click rate': ['clicks', 'ctr', 'click_rate'],
  clicks: ['clicks', 'click_rate'],
  conversion: ['conversions', 'conversion_rate'],
  conversions: ['conversions', 'conversion_rate']
};

function findRequestedMetric(question, schema) {
  const metrics = findRequestedMetrics(question, schema, true);
  return metrics[0] || null;
}

function findRequestedMetrics(question, schema, allowFallback = true) {
  const lower = String(question || '').toLowerCase();
  const intelligence = getSchemaIntelligence(schema);
  const metricNames = intelligence.metric_candidates || [];
  const numericColumns = metricNames
    .map(name => schema.columns.find(column => column.name === name))
    .filter(Boolean);
  const queryTokens = new Set(tokenizeSemantic(question));
  const scored = numericColumns.map(column => {
    const colLower = column.name.toLowerCase();
    const spaced = colLower.replace(/_/g, ' ');
    const columnTokens = new Set(tokenizeSemantic(column.name));
    let score = 0;

    if (lower.includes(colLower) || lower.includes(spaced)) {
      score += 10;
    }

    columnTokens.forEach(token => {
      if (queryTokens.has(token)) {
        score += 4;
      }
      if (token.endsWith('s') && queryTokens.has(token.slice(0, -1))) {
        score += 2;
      }
      if (!token.endsWith('s') && queryTokens.has(token + 's')) {
        score += 2;
      }
    });

    // Semantic language mapping, e.g., sales -> revenue, engagement -> engagement_score.
    Array.from(queryTokens).forEach(token => {
      const mapped = METRIC_LANGUAGE_MAP[token] || [];
      mapped.forEach(alias => {
        const aliasLower = String(alias).toLowerCase();
        if (colLower.includes(aliasLower) || spaced.includes(aliasLower.replace(/_/g, ' '))) {
          score += 3;
        }
      });
    });

    const asksIdLike = /\bid\b|\bidentifier\b/.test(lower);
    const asksRecordCount = /\bcount\b|\bhow many\b|\bnumber of\b/.test(lower);
    if (/_id$/i.test(column.name) && !asksIdLike && !asksRecordCount) {
      score -= 6;
    }

    return { column, score };
  }).sort((a, b) => b.score - a.score);

  const exactMatches = scored.filter(item => item.score >= 10).map(item => item.column);
  if (exactMatches.length > 0) {
    const exactSeen = new Set();
    return exactMatches.filter(column => {
      if (exactSeen.has(column.name)) return false;
      exactSeen.add(column.name);
      return true;
    });
  }

  const matched = scored.filter(item => item.score > 0).map(item => item.column);
  const deduped = [];
  const seen = new Set();
  matched.forEach(column => {
    if (!seen.has(column.name)) {
      seen.add(column.name);
      deduped.push(column);
    }
  });

  if (deduped.length > 0) {
    return deduped;
  }

  if (!allowFallback) {
    return [];
  }

  const fallback = numericColumns[0] || schema.columns.find(column => column.type === 'NUMBER') || null;
  return fallback ? [fallback] : [];
}

// Synonym dictionary for mapping natural language words to column name fragments
const COLUMN_SYNONYMS = [
  ['channel', 'channel'],
  ['marketing channel', 'channel'],
  ['campaign', 'campaign'],
  ['campaign type', 'campaign'],
  ['audience', 'audience'],
  ['segment', 'segment'],
  ['customer segment', 'segment'],
  ['region', 'region'],
  ['country', 'country'],
  ['city', 'city'],
  ['category', 'category'],
  ['product', 'product'],
  ['language', 'language'],
  ['gender', 'gender'],
  ['age', 'age'],
  ['platform', 'platform'],
  ['source', 'source'],
  ['medium', 'medium'],
  ['type', 'type'],
  ['status', 'status'],
  ['date', 'date'],
  ['month', 'month'],
  ['year', 'year'],
];

function resolveColumnByWord(word, schema, preferNonNumeric = true) {
  const w = word.toLowerCase().trim();
  const normalizedWord = w.replace(/_/g, ' ').replace(/\s+/g, ' ').trim();
  const phraseResolved = resolveColumnByPhrase(normalizedWord, schema, preferNonNumeric);
  if (phraseResolved) {
    return phraseResolved;
  }
  // Check synonym dictionary first
  for (const [synonym, fragment] of COLUMN_SYNONYMS) {
    if (w === synonym || w.includes(synonym) || synonym.includes(w)) {
      const scored = schema.columns
        .map(col => {
          const colLower = col.name.toLowerCase().replace(/_/g, ' ').trim();
          let score = 0;
          if (colLower === normalizedWord) score += 10;
          if (colLower.includes(normalizedWord)) score += 8;
          if (normalizedWord.includes(colLower)) score += 6;
          if (colLower === synonym) score += 7;
          if (colLower.includes(synonym)) score += 5;
          if (colLower.includes(fragment) || col.name.toLowerCase().includes(fragment)) score += 4;
          if (normalizedWord.includes('type') && colLower.includes('type')) score += 4;
          if (normalizedWord.includes('id') && colLower.includes('id')) score += 4;
          if (!normalizedWord.includes('id') && /\bid\b/.test(colLower)) score -= 3;
          if (preferNonNumeric && col.type === 'NUMBER') score -= 20;
          return { col, score };
        })
        .filter(item => item.score > 0)
        .sort((a, b) => b.score - a.score);

      const found = scored.length ? scored[0].col : null;
      if (found && (!preferNonNumeric || found.type !== 'NUMBER')) return found;
    }
  }
  // Direct partial match against column names
  const pool = preferNonNumeric
    ? schema.columns.filter(c => c.type !== 'NUMBER')
    : schema.columns;
  const exact = pool.find(col => col.name.toLowerCase().replace(/_/g, ' ').trim() === normalizedWord);
  if (exact) {
    return exact;
  }
  return pool.find(col => {
    const colLower = col.name.toLowerCase().replace(/_/g, ' ');
    return colLower === w || colLower.includes(w) || w.includes(colLower);
  }) || null;
}

function extractGroupingDimensionPhrase(question) {
  const lower = String(question || '').toLowerCase();
  const patterns = [
    /\bgrouped\s+by\s+([\w\s,]+?)(?=$|\bfor\b|\bonly\b|\bwhere\b|\bin\b|\bbelonging to\b|\btop\b|\bbottom\b|\blimit\b|\border\b|\bgroup\b|\bhaving\b|\bas\b|\busing\b|\bwith\b)/i,
    /\bfor\s+each\s+([\w\s,]+?)(?=$|\bfor\b|\bonly\b|\bwhere\b|\bin\b|\bbelonging to\b|\btop\b|\bbottom\b|\blimit\b|\border\b|\bgroup\b|\bhaving\b|\bas\b|\busing\b|\bwith\b)/i,
    /\bper\s+([\w\s,]+?)(?=$|\bfor\b|\bonly\b|\bwhere\b|\bin\b|\bbelonging to\b|\btop\b|\bbottom\b|\blimit\b|\border\b|\bgroup\b|\bhaving\b|\bas\b|\busing\b|\bwith\b)/i,
    /\bacross\s+([\w\s,]+?)(?=$|\bfor\b|\bonly\b|\bwhere\b|\bin\b|\bbelonging to\b|\btop\b|\bbottom\b|\blimit\b|\border\b|\bgroup\b|\bhaving\b|\bas\b|\busing\b|\bwith\b)/i,
    /\bby\s+([\w\s,]+?)(?=$|\bfor\b|\bonly\b|\bwhere\b|\bin\b|\bbelonging to\b|\btop\b|\bbottom\b|\blimit\b|\border\b|\bgroup\b|\bhaving\b|\bas\b|\busing\b|\bwith\b)/i
  ];

  for (const pattern of patterns) {
    const match = lower.match(pattern);
    if (match && match[1]) {
      return match[1].trim();
    }
  }

  return null;
}

function hasGroupingDimensionLanguage(question) {
  const lower = String(question || '').toLowerCase();
  return /\bby\b|\bfor\s+each\b|\bper\b|\bacross\b|\bgrouped\s+by\b/.test(lower);
}

function findRequestedDimension(question, schema) {
  const lower = String(question || '').toLowerCase();
  const intelligence = getSchemaIntelligence(schema);
  const dimensionPool = (intelligence.dimension_candidates || [])
    .map(name => schema.columns.find(column => column.name === name))
    .filter(Boolean);

  // Direct semantic scan for dimension words even without explicit "by".
  const queryTokens = tokenizeSemantic(question);
  const semanticDimension = queryTokens.reduce((found, token) => found || resolveColumnByWord(token, schema, true), null);
  if (semanticDimension && semanticDimension.type !== 'NUMBER') {
    return semanticDimension;
  }

  // Ranking phrasing often puts the dimension before "by", e.g. "top 5 regions by revenue".
  const rankingLead = lower.match(/\b(?:top|bottom)\s+\d{1,3}\s+([\w\s]+?)\s+by\b/);
  if (rankingLead) {
    const candidatePhrase = rankingLead[1].trim();
    const resolvedRankingDimension = resolveColumnByWord(candidatePhrase, schema, true)
      || candidatePhrase.split(/\s+/).reduce((found, token) => found || resolveColumnByWord(token, schema, true), null);
    if (resolvedRankingDimension && resolvedRankingDimension.type !== 'NUMBER') {
      return resolvedRankingDimension;
    }
  }

  // Extract grouped dimension phrase using equivalent grouping language.
  const groupingPhrase = extractGroupingDimensionPhrase(question);
  if (groupingPhrase) {
    const phrase = groupingPhrase;
    const exactByPhrase = schema.columns.find(column => column.name.toLowerCase().replace(/_/g, ' ') === phrase);
    if (exactByPhrase) {
      return exactByPhrase;
    }
    // Try full phrase, then each word, preferring non-numeric columns
    const resolved = resolveColumnByWord(phrase, schema, true)
      || phrase.split(/\s+/).reduce((found, word) => found || resolveColumnByWord(word, schema, true), null)
      || resolveColumnByWord(phrase, schema, false)
      || phrase.split(/\s+/).reduce((found, word) => found || resolveColumnByWord(word, schema, false), null);
    if (resolved) return resolved;
  }

  // Fallback: non-numeric column mentioned in the question
  const directMention = findQuestionMatchedColumn(question, { ...schema, columns: dimensionPool.length ? dimensionPool : schema.columns }, column => column.type !== 'NUMBER');
  if (directMention) {
    return directMention;
  }

  const asksBreakdown = hasGroupingDimensionLanguage(question) || /\beach\b|\bcompare\b|\brank\b|\btop\b|\bbottom\b|\bhighest\b|\blowest\b|\bbest\b|\bworst\b/.test(lower);
  if (asksBreakdown) {
    return dimensionPool[0] || schema.columns.find(column => column.type === 'TEXT') || null;
  }

  return null;
}

function findRequestedDimensions(question, schema) {
  const lower = String(question || '').toLowerCase();
  const dimensions = [];
  const seen = new Set();

  const addDimension = column => {
    if (!column || column.type === 'NUMBER') {
      return;
    }
    if (seen.has(column.name)) {
      return;
    }
    seen.add(column.name);
    dimensions.push(column);
  };

  // Multi-dimension grouping pattern: by / for each / per / across / grouped by.
  const groupingPhrase = extractGroupingDimensionPhrase(question);
  if (groupingPhrase) {
    const rawBySegment = groupingPhrase;
    const candidateParts = rawBySegment
      .split(/\b(?:and|along with)\b|,/i)
      .map(part => part.trim())
      .filter(Boolean);

    candidateParts.forEach(part => {
      const exact = schema.columns.find(column => column.name.toLowerCase().replace(/_/g, ' ') === part);
      if (exact && exact.type !== 'NUMBER') {
        addDimension(exact);
        return;
      }

      const resolved = resolveColumnByWord(part, schema, true)
        || part.split(/\s+/).reduce((found, token) => found || resolveColumnByWord(token, schema, true), null)
        || null;
      addDimension(resolved);
    });
  }

  // General rule: if multiple dimension words are connected by "and",
  // include all matching dimensions in GROUP BY.
  if (/\band\b/.test(lower)) {
    const intelligence = getSchemaIntelligence(schema);
    const dimensionNames = Array.isArray(intelligence?.dimension_candidates)
      ? intelligence.dimension_candidates
      : schema.columns.filter(column => column.type !== 'NUMBER').map(column => column.name);

    dimensionNames.forEach(name => {
      const normalized = name.toLowerCase().replace(/_/g, ' ');
      if (normalized && lower.includes(normalized)) {
        const column = schema.columns.find(item => item.name === name);
        addDimension(column || null);
      }
    });
  }

  if (dimensions.length > 0) {
    return dimensions;
  }

  const single = findRequestedDimension(question, schema);
  if (single) {
    addDimension(single);
  }

  return dimensions;
}

function buildExtremaSql(question, schema) {
  const lower = String(question || '').toLowerCase();
  const table = schema.tableName;
  const metric = findRequestedMetric(question, schema);
  const dimension = findRequestedDimension(question, schema);
  const asksHighest = /\bhighest\b|\bmaximum\b|\bmax\b|\bbest\b/.test(lower);
  const asksLowest = /\blowest\b|\bminimum\b|\bmin\b|\bworst\b/.test(lower);
  const rankingSignal = /\btop\b|\bbottom\b|\bhighest\b|\blowest\b|\bbest\b|\bworst\b/.test(lower);

  if (!metric) {
    return null;
  }

  // When a dimension is present or ranking intent is detected, do not force raw MAX/MIN.
  if (dimension && rankingSignal) {
    return null;
  }

  if (asksHighest && asksLowest) {
    if (dimension) {
      return {
        sql_query: `SELECT ${dimension.name}, MAX(${metric.name}) AS highest_${metric.name}, MIN(${metric.name}) AS lowest_${metric.name} FROM ${table} GROUP BY ${dimension.name} ORDER BY ${dimension.name} LIMIT 100`,
        chart_type: 'table',
        mode: 'extrema_override'
      };
    }

    return {
      sql_query: `SELECT MAX(${metric.name}) AS highest_${metric.name}, MIN(${metric.name}) AS lowest_${metric.name} FROM ${table}`,
      chart_type: 'table',
      mode: 'extrema_override'
    };
  }

  if (asksHighest) {
    return {
      sql_query: `SELECT MAX(${metric.name}) AS highest_${metric.name} FROM ${table}`,
      chart_type: 'table',
      mode: 'extrema_override'
    };
  }

  if (asksLowest) {
    return {
      sql_query: `SELECT MIN(${metric.name}) AS lowest_${metric.name} FROM ${table}`,
      chart_type: 'table',
      mode: 'extrema_override'
    };
  }

  return null;
}

function buildTopBottomMetricSql(question, schema) {
  const lower = String(question || '').toLowerCase();
  const table = schema.tableName;
  const metric = findRequestedMetrics(question, schema, false)[0] || findRequestedMetric(question, schema);
  if (!metric) {
    return null;
  }

  const topMatch = lower.match(/\btop\s+(\d{1,3})\b/);
  const bottomMatch = lower.match(/\bbottom\s+(\d{1,3})\b/);
  const highestLike = /\bhighest\b|\bbest\b/.test(lower);
  const lowestLike = /\blowest\b|\bworst\b/.test(lower);
  if (!topMatch && !bottomMatch && !highestLike && !lowestLike) {
    return null;
  }

  const requestedN = Number((topMatch || bottomMatch || [null, '1'])[1]);
  const n = Number.isFinite(requestedN) && requestedN > 0 ? requestedN : 1;
  const direction = (topMatch || highestLike) ? 'DESC' : 'ASC';

  const dimension = findRequestedDimension(question, schema) || schema.columns.find(column => column.type === 'TEXT');
  if (dimension) {
    return {
      sql_query: `SELECT ${dimension.name}, SUM(${metric.name}) AS total_${metric.name} FROM ${table} GROUP BY ${dimension.name} ORDER BY total_${metric.name} ${direction} LIMIT ${n}`,
      chart_type: 'horizontal_bar_chart',
      mode: 'top_bottom_override'
    };
  }

  const detailDimension = dimension || schema.columns.find(column => /id/i.test(column.name)) || null;
  if (detailDimension && detailDimension.name !== metric.name) {
    return {
      sql_query: `SELECT ${detailDimension.name}, ${metric.name} FROM ${table} ORDER BY ${metric.name} ${direction} LIMIT ${n}`,
      chart_type: 'bar_chart',
      mode: 'top_bottom_override'
    };
  }

  return {
    sql_query: `SELECT ${metric.name} FROM ${table} ORDER BY ${metric.name} ${direction} LIMIT ${n}`,
    chart_type: 'table',
    mode: 'top_bottom_override'
  };
}

function inferAnalysisType(question) {
  const lower = String(question || '').toLowerCase();
  if (lower.includes('trend') || lower.includes('over time') || lower.includes('monthly') || lower.includes('month')) {
    return 'trend';
  }
  if (lower.includes('correlation') || lower.includes('relationship') || lower.includes('relation') || lower.includes('versus') || /\bhow\s+.+\s+affects?\s+.+\b/.test(lower)) {
    return 'correlation';
  }
  if (lower.includes('yearly') || lower.includes('year over year') || /\byoy\b/.test(lower)) {
    return 'trend';
  }
  if (lower.includes('top') || lower.includes('highest') || lower.includes('rank') || lower.includes('bottom') || lower.includes('lowest')) {
    return 'ranking';
  }
  if (lower.includes('distribution') || lower.includes('share') || lower.includes('percentage')) {
    return 'distribution';
  }
  if (lower.includes('compare') || lower.includes(' vs ') || lower.includes(' versus ') || lower.includes('along with')) {
    return 'comparison';
  }
  return 'aggregation';
}

function inferAggregationType(question) {
  const lower = String(question || '').toLowerCase();
  if (lower.includes('average') || lower.includes('avg') || lower.includes('mean')) {
    return 'AVG';
  }
  if (lower.includes('count') || lower.includes('how many') || lower.includes('number of')) {
    return 'COUNT';
  }
  if (lower.includes('maximum') || lower.includes('max')) {
    return 'MAX';
  }
  if (lower.includes('minimum') || lower.includes('min')) {
    return 'MIN';
  }
  return 'SUM';
}

function isTrendComparisonQuestion(question) {
  const lower = String(question || '').toLowerCase();
  const trendSignal = /\btrend\b|\bover time\b|\bmonthly\b|\byearly\b|\bacross\b/.test(lower);
  const compareSignal = /\bcompare\b|\bcomparison\b|\bvs\b|\bversus\b|\balong with\b|\bdifferent types\b|\bother\b/.test(lower);
  return trendSignal && compareSignal;
}

function detectGroupedRankingScope(question) {
  const lower = String(question || '').toLowerCase();
  return {
    month: /\bin each month\b|\bper month\b/.test(lower),
    year: /\bfor each year\b|\bin each year\b|\bper year\b/.test(lower),
    category: /\bwithin each category\b/.test(lower)
  };
}

function inferTrendComparisonCategoryDimension(question, schema, dateColumn) {
  const lower = String(question || '').toLowerCase();
  const isDateName = name => dateColumn && String(name || '').toLowerCase() === String(dateColumn.name || '').toLowerCase();

  const requested = findRequestedDimensions(question, schema)
    .filter(column => column && column.type !== 'NUMBER' && !isDateName(column.name));
  if (requested.length > 0) {
    return requested[0];
  }

  const semanticHints = ['campaign type', 'category', 'type', 'segment', 'channel', 'region', 'product'];
  for (const hint of semanticHints) {
    if (!lower.includes(hint)) {
      continue;
    }
    const resolved = resolveColumnByWord(hint, schema, true);
    if (resolved && resolved.type !== 'NUMBER' && !isDateName(resolved.name)) {
      return resolved;
    }
  }

  const intelligence = getSchemaIntelligence(schema);
  const candidates = Array.isArray(intelligence?.dimension_candidates)
    ? intelligence.dimension_candidates
    : [];

  const prioritizedName = candidates.find(name => /type|category|segment|channel|region|product|campaign/i.test(name));
  if (prioritizedName) {
    const match = schema.columns.find(column => column.name === prioritizedName);
    if (match && match.type !== 'NUMBER' && !isDateName(match.name)) {
      return match;
    }
  }

  return schema.columns.find(column => column.type !== 'NUMBER' && !isDateName(column.name)) || null;
}

function escapeSqlString(value) {
  return String(value).replace(/'/g, "''");
}

function detectQuestionFilters(question, schema) {
  const lower = String(question || '').toLowerCase();
  const intelligence = getSchemaIntelligence(schema);
  const valueMap = intelligence.categorical_value_map || {};
  const extracted = [];
  const seen = new Set();

  const signalRegex = /\b(?:for(?!\s+each\b)|only|where|belonging to|in)\s+([a-z0-9_\-\s]+?)(?=$|\bby\b|\bwith\b|\busing\b|\btop\b|\bbottom\b|\border\b|\blimit\b|\bgroup\b|\bper\b|\bacross\b)/gi;
  const segments = [];
  let match;
  while ((match = signalRegex.exec(lower)) !== null) {
    const segment = String(match[1] || '').trim();
    if (!segment) continue;
    // Skip pure year mentions; handled by dedicated year filter.
    if (/^20\d{2}$/.test(segment)) continue;
    segments.push(segment);
  }

  const byDimension = findRequestedDimension(question, schema);

  const tryAddFilter = (column, value) => {
    const key = `${column}::${value}`.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    extracted.push({ column, value });
  };

  segments.forEach(segment => {
    // Highest confidence: exact/contains match against known categorical values.
    Object.keys(valueMap).forEach(columnName => {
      const values = Array.isArray(valueMap[columnName]) ? valueMap[columnName] : [];
      values.forEach(rawValue => {
        const normalized = String(rawValue).toLowerCase();
        if (!normalized) return;
        if (segment.includes(normalized) || normalized.includes(segment)) {
          tryAddFilter(columnName, rawValue);
        }
      });
    });

    // If value not directly found but phrase references a known dimension token, treat leftover word as value.
    if (!extracted.length && byDimension && byDimension.name) {
      const cleaned = segment
        .replace(/\bcampaigns?\b|\bchannels?\b|\bregions?\b|\bsegments?\b|\bcategories?\b|\btypes?\b/g, '')
        .replace(/\beach\b/g, '')
        .trim();
      if (cleaned && !/^20\d{2}$/.test(cleaned)) {
        // Title-case a plain token for common categorical data style.
        const value = cleaned.split(/\s+/).map(token => token.charAt(0).toUpperCase() + token.slice(1)).join(' ');
        tryAddFilter(byDimension.name, value);
      }
    }
  });

  return extracted;
}

function intent_extractor(question, schema) {
  const lower = String(question || '').toLowerCase();
  const dimensions = [];
  const filters = {};

  findRequestedDimensions(question, schema).forEach(dimension => {
    dimensions.push(dimension.name);
  });

  const yearMatch = lower.match(/\b(20\d{2})\b/);
  if (yearMatch) {
    filters.year = yearMatch[1];
  }

  const phraseFilters = detectQuestionFilters(question, schema);
  if (phraseFilters.length > 0) {
    filters.conditions = phraseFilters;
  }

  // STRICT RULE: only metrics explicitly referenced in the user question.
  const metrics = findRequestedMetrics(question, schema, false).map(col => col.name);

  let analysisType = inferAnalysisType(question);
  if ((/\bby\b/.test(lower) || dimensions.length > 0) && /\bhighest\b|\blowest\b|\bbest\b|\bworst\b|\btop\b|\bbottom\b/.test(lower)) {
    analysisType = 'ranking';
  }

  let aggregationType = inferAggregationType(question);
  if (analysisType === 'ranking' && dimensions.length > 0) {
    aggregationType = 'SUM';
  }

  return {
    dimensions,
    metrics,
    filters,
    aggregation_type: aggregationType,
    analysis_type: analysisType
  };
}

function query_planner(intent, schema, question) {
  const lower = String(question || '').toLowerCase();
  const rawGroupBy = Array.isArray(intent?.dimensions) ? [...intent.dimensions] : [];
  const groupBy = rawGroupBy.filter(columnName => {
    const column = schema.columns.find(item => item.name === columnName);
    return column && (column.type !== 'NUMBER' || /(^|_)id$/i.test(column.name));
  });
  const metrics = Array.isArray(intent?.metrics) ? intent.metrics : [];
  const aggregationType = String(intent?.aggregation_type || 'SUM').toUpperCase();
  const dateColumn = schema.columns.find(column => column.type === 'DATE' || /date/i.test(column.name));
  const intelligence = getSchemaIntelligence(schema);
  const isTrendComparison = (intent?.analysis_type === 'trend') && isTrendComparisonQuestion(question);
  const groupedRankingScope = detectGroupedRankingScope(question);
  const isGroupedRanking = intent?.analysis_type === 'ranking' && (groupedRankingScope.month || groupedRankingScope.year || groupedRankingScope.category);
  const isCorrelation = intent?.analysis_type === 'correlation'
    || /\brelationship\s+between\b|\bcorrelation\s+between\b|\bhow\s+.+\s+affects?\s+.+\b/.test(lower);

  const aggregations = {};
  metrics.forEach(metric => {
    const shouldForceRankingAgg = (intent?.analysis_type === 'ranking') && groupBy.length > 0;
    const chosen = shouldForceRankingAgg ? 'SUM' : aggregationType;
    aggregations[metric] = ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN'].includes(chosen) ? chosen : 'SUM';
  });

  if (!groupBy.length && intelligence.dimension_candidates && intelligence.dimension_candidates.length) {
    const fallbackDimension = intelligence.dimension_candidates.find(name => {
      const column = schema.columns.find(item => item.name === name);
      return column && column.type !== 'NUMBER';
    });
    if (fallbackDimension && /\bby\b|\btop\b|\bbottom\b|\bcompare\b|\brank\b/i.test(lower)) {
      groupBy.push(fallbackDimension);
    }
  }

  if (isGroupedRanking && dateColumn && (groupedRankingScope.month || groupedRankingScope.year)) {
    const categoryDimension = inferTrendComparisonCategoryDimension(question, schema, dateColumn);
    if (categoryDimension && !groupBy.includes(categoryDimension.name)) {
      groupBy.push(categoryDimension.name);
    }
  }

  const filters = [];
  if (intent?.filters?.year && dateColumn) {
    const year = String(intent.filters.year);
    const nextYear = String(Number(year) + 1);
    filters.push(`${dateColumn.name} >= '${year}-01-01'`);
    filters.push(`${dateColumn.name} < '${nextYear}-01-01'`);
  }

  const phraseFilters = Array.isArray(intent?.filters?.conditions) ? intent.filters.conditions : [];
  const trendComparisonCategory = isTrendComparison
    ? inferTrendComparisonCategoryDimension(question, schema, dateColumn)
    : null;

  if (isTrendComparison && trendComparisonCategory && !groupBy.includes(trendComparisonCategory.name)) {
    groupBy.push(trendComparisonCategory.name);
  }

  phraseFilters.forEach(item => {
    if (!item || !item.column || item.value == null) return;
    const columnExists = schema.columns.some(column => column.name === item.column);
    if (!columnExists) return;

    // For broad trend comparisons ("compare with other/different types"), avoid collapsing
    // comparison to a single category via an inferred equality filter.
    const broadComparisonLanguage = /\bother\b|\bdifferent\b/.test(lower) && /\bcompare\b|\bacross\b/.test(lower);
    if (isTrendComparison && broadComparisonLanguage && trendComparisonCategory && item.column === trendComparisonCategory.name) {
      return;
    }

    const safeValue = escapeSqlString(item.value);
    filters.push(`${item.column} = '${safeValue}'`);
  });

  if (isCorrelation) {
    let rawMetrics = metrics.filter(name => {
      const column = schema.columns.find(item => item.name === name);
      return column && column.type === 'NUMBER';
    });

    if (rawMetrics.length < 2) {
      rawMetrics = findRequestedMetrics(question, schema, false)
        .map(column => column.name)
        .filter((name, index, arr) => arr.indexOf(name) === index)
        .slice(0, 2);
    } else {
      rawMetrics = rawMetrics.slice(0, 2);
    }

    rawMetrics.forEach(metric => {
      const predicate = `${metric} IS NOT NULL`;
      if (!filters.includes(predicate)) {
        filters.push(predicate);
      }
    });

    return {
      group_by: [],
      aggregations: {},
      raw_metrics: rawMetrics,
      filters,
      analysis_type: 'correlation',
      order_by: null,
      limit: 1000,
      time_grouping: null,
      no_aggregation: true
    };
  }

  let timeGrouping = null;
  const needsTimeGrouping = (intent?.analysis_type === 'trend' || isGroupedRanking) && dateColumn;
  if (needsTimeGrouping) {
    const yearly = lower.includes('yearly') || lower.includes('year over year') || /\byoy\b/.test(lower);
    const forceYear = groupedRankingScope.year && !groupedRankingScope.month;
    timeGrouping = {
      column: dateColumn.name,
      expression: (yearly || forceYear) ? `SUBSTR(${dateColumn.name}, 1, 4)` : `SUBSTR(${dateColumn.name}, 1, 7)`,
      alias: (yearly || forceYear) ? 'year' : 'month'
    };
    const dateIndex = groupBy.findIndex(column => column === dateColumn.name);
    if (dateIndex > -1) {
      groupBy.splice(dateIndex, 1);
    }
    if (!groupBy.includes(timeGrouping.alias)) {
      groupBy.push(timeGrouping.alias);
    }
  }

  let orderBy = null;
  const firstMetric = metrics[0] || null;
  if (firstMetric && (intent?.analysis_type === 'ranking' || intent?.analysis_type === 'comparison' || /\btop\b|\bhighest\b|\bbest\b|\brank\b/i.test(lower))) {
    orderBy = `total_${firstMetric} DESC`;
  }
  if (firstMetric && /\bbottom\b|\blowest\b|\bworst\b/i.test(lower)) {
    orderBy = `total_${firstMetric} ASC`;
  }

  const topMatch = lower.match(/\btop\s+(\d{1,3})\b/);
  const bottomMatch = lower.match(/\bbottom\s+(\d{1,3})\b/);
  const singularRanking = /\bhighest\b|\blowest\b|\bbest\b|\bworst\b/.test(lower);
  const defaultLimitText = (singularRanking && !isGroupedRanking) ? '1' : '20';
  const requestedLimit = Number((topMatch || bottomMatch || [null, defaultLimitText])[1]);
  const limit = Number.isFinite(requestedLimit) && requestedLimit > 0 ? requestedLimit : 20;

  if (isGroupedRanking && firstMetric) {
    const rankingDirection = /\bbottom\b|\blowest\b|\bworst\b/.test(lower) ? 'ASC' : 'DESC';
    if (timeGrouping && (groupedRankingScope.month || groupedRankingScope.year)) {
      orderBy = `${timeGrouping.alias} ASC, total_${firstMetric} ${rankingDirection}`;
    } else if (groupBy.length > 0) {
      orderBy = `${groupBy[0]} ASC, total_${firstMetric} ${rankingDirection}`;
    }
  }

  if (intent?.analysis_type === 'trend' && dateColumn) {
    orderBy = timeGrouping ? `${timeGrouping.alias} ASC` : `${dateColumn.name} ASC`;
  }

  return {
    group_by: groupBy,
    aggregations,
    filters,
    analysis_type: intent?.analysis_type || 'aggregation',
    order_by: orderBy,
    limit,
    time_grouping: timeGrouping
  };
}

function sql_generator(queryPlan, schema) {
  if (queryPlan?.no_aggregation && Array.isArray(queryPlan?.raw_metrics) && queryPlan.raw_metrics.length >= 2) {
    const selectClause = queryPlan.raw_metrics.slice(0, 2).join(', ');
    const whereClause = Array.isArray(queryPlan?.filters) && queryPlan.filters.length
      ? ` WHERE ${queryPlan.filters.join(' AND ')}`
      : '';
    const limitValue = Number.isFinite(queryPlan?.limit) ? queryPlan.limit : 1000;
    const limitClause = limitValue > 0 ? ` LIMIT ${limitValue}` : '';
    return {
      sql_query: `SELECT ${selectClause} FROM ${schema.tableName}${whereClause}${limitClause}`
    };
  }

  const selectParts = [];
  const groupByParts = [];

  if (queryPlan?.time_grouping) {
    selectParts.push(`${queryPlan.time_grouping.expression} AS ${queryPlan.time_grouping.alias}`);
    groupByParts.push(queryPlan.time_grouping.alias);
  }

  const explicitGroupBy = Array.isArray(queryPlan?.group_by) ? queryPlan.group_by : [];
  explicitGroupBy.forEach(column => {
    if (column !== 'month' && !groupByParts.includes(column)) {
      selectParts.push(column);
      groupByParts.push(column);
    }
  });

  const aggregations = queryPlan?.aggregations || {};
  const metricNames = Object.keys(aggregations);
  metricNames.forEach(metric => {
    const fn = String(aggregations[metric] || 'SUM').toUpperCase();
    const safeFn = ['SUM', 'AVG', 'COUNT', 'MAX', 'MIN'].includes(fn) ? fn : 'SUM';
    selectParts.push(`${safeFn}(${metric}) AS total_${metric}`);
  });

  const selectClause = selectParts.length ? selectParts.join(', ') : '*';
  const whereClause = Array.isArray(queryPlan?.filters) && queryPlan.filters.length
    ? ` WHERE ${queryPlan.filters.join(' AND ')}`
    : '';
  const groupByClause = groupByParts.length ? ` GROUP BY ${groupByParts.join(', ')}` : '';
  const orderByClause = queryPlan?.order_by ? ` ORDER BY ${queryPlan.order_by}` : '';
  const limitValue = Number.isFinite(queryPlan?.limit) ? queryPlan.limit : 20;
  const limitClause = limitValue > 0 ? ` LIMIT ${limitValue}` : '';

  return {
    sql_query: `SELECT ${selectClause} FROM ${schema.tableName}${whereClause}${groupByClause}${orderByClause}${limitClause}`
  };
}

function sql_validator(sqlOutput, schema, question, queryPlan) {
  const sqlQuery = sqlOutput?.sql_query || '';
  let validation = validateSqlAgainstSchema(sqlQuery, schema);
  if (!validation.ok) {
    return validation;
  }

  const plannedMetrics = queryPlan?.no_aggregation
    ? (Array.isArray(queryPlan?.raw_metrics) ? queryPlan.raw_metrics : [])
    : Object.keys(queryPlan?.aggregations || {});

  const coverage = validateSqlCoverage(question, schema, sqlQuery, {
    dimensions: queryPlan?.group_by || [],
    metrics: plannedMetrics,
    filters: (queryPlan?.filters || []).length ? { year: (question.match(/\b(20\d{2})\b/) || [])[1] } : {},
    analysis_type: queryPlan?.analysis_type || 'aggregation'
  });
  if (!coverage.ok) {
    return { ok: false, error: coverage.message };
  }

  const semanticCheck = performSqlDoubleCheck(question, schema, sqlQuery, {
    dimensions: queryPlan?.group_by || [],
    metrics: plannedMetrics,
    analysis_type: queryPlan?.analysis_type || 'aggregation'
  });
  if (!semanticCheck.ok) {
    return { ok: false, error: semanticCheck.error };
  }

  return { ok: true };
}

function repair_query_plan(queryPlan, schema, question) {
  const repaired = {
    ...(queryPlan || {}),
    group_by: Array.isArray(queryPlan?.group_by) ? [...queryPlan.group_by] : [],
    aggregations: { ...(queryPlan?.aggregations || {}) },
    filters: Array.isArray(queryPlan?.filters) ? [...queryPlan.filters] : []
  };

  // Ranking questions need ORDER BY for deterministic top/bottom results.
  const firstMetric = Object.keys(repaired.aggregations)[0];
  const lower = String(question || '').toLowerCase();
  if (!repaired.order_by && firstMetric && repaired.analysis_type === 'ranking') {
    repaired.order_by = /\bbottom\b|\blowest\b|\bworst\b/.test(lower)
      ? `total_${firstMetric} ASC`
      : `total_${firstMetric} DESC`;
  }

  return repaired;
}

function extractIntent(question, schema) {
  const extracted = intent_extractor(question, schema);
  return {
    dimensions: extracted.dimensions,
    metrics: extracted.metrics,
    filters: extracted.filters,
    analysis_type: extracted.analysis_type,
    aggregation_type: extracted.aggregation_type,
    metric: extracted.metrics[0] || 'none',
    time: (schema.columns.find(column => column.type === 'DATE' || column.name.toLowerCase().includes('date')) || {}).name || 'none',
    dimension: extracted.dimensions[0] || 'none',
    aggregation: extracted.aggregation_type
  };
}

function extractQueryPlan(question, schema) {
  const intent = intent_extractor(question, schema);
  return query_planner(intent, schema, question);
}

function selectRelevantColumns(question, schema) {
  const tokens = question.toLowerCase().match(/[a-z0-9_]+/g) || [];
  const scored = schema.columns.map(column => {
    const name = column.name.toLowerCase();
    let score = 0;
    tokens.forEach(token => {
      if (name.includes(token)) {
        score += 3;
      }
    });
    if (column.type === 'NUMBER') {
      score += 1;
    }
    if (column.type === 'DATE') {
      score += 1;
    }
    return { name: column.name, score };
  }).sort((left, right) => right.score - left.score);

  const selected = scored.filter(item => item.score > 0).slice(0, 8).map(item => item.name);
  return selected.length > 0 ? selected : schema.columns.slice(0, 8).map(column => column.name);
}

function chartHint(question, analysisType = '') {
  const lower = String(question || '').toLowerCase();
  const inferred = analysisType || inferAnalysisType(question);
  if (lower.includes('donut')) {
    return 'donut_chart';
  }
  if (lower.includes('doughnut')) {
    return 'donut_chart';
  }
  if (lower.includes('pie')) {
    return 'pie_chart';
  }
  if (lower.includes('polar area')) {
    return 'polar_area_chart';
  }
  if (lower.includes('radar')) {
    return 'radar_chart';
  }
  if (lower.includes('scatter')) {
    return 'scatter_plot';
  }
  if (lower.includes('bubble')) {
    return 'bubble_chart';
  }
  if (lower.includes('lollipop')) {
    return 'lollipop_chart';
  }
  if (lower.includes('horizontal bar')) {
    return 'horizontal_bar_chart';
  }
  if (lower.includes('bar')) {
    return 'bar_chart';
  }
  if (lower.includes('area')) {
    return 'area_chart';
  }
  if (lower.includes('step line') || lower.includes('step chart') || lower.includes('step trend')) {
    return 'step_line_chart';
  }
  if (lower.includes('line')) {
    return 'line_chart';
  }
  if (inferred === 'trend') {
    return 'line_chart';
  }
  if (inferred === 'comparison') {
    return 'bar_chart';
  }
  if (inferred === 'distribution') {
    return 'pie_chart';
  }
  if (inferred === 'correlation') {
    return 'scatter_plot';
  }
  if (inferred === 'ranking') {
    return 'horizontal_bar_chart';
  }
  if (lower.includes('list') || lower.includes('show all') || lower.includes('table')) {
    return 'table';
  }
  return 'bar_chart';
}

function buildDemoSql(question, schema) {
  const lower = question.toLowerCase();
  const table = schema.tableName;
  const metrics = findRequestedMetrics(question, schema);
  const metric = metrics[0] || null;
  const textColumn = findRequestedDimension(question, schema) || schema.columns.find(column => column.type === 'TEXT');
  const dateColumn = schema.columns.find(column => column.type === 'DATE' || column.name.toLowerCase().includes('date'));
  const yearMatch = lower.match(/\b(20\d{2})\b/);
  const yearFilter = yearMatch ? yearMatch[1] : null;
  const asksHighest = lower.includes('highest') || lower.includes('maximum') || lower.includes('max');
  const asksLowest = lower.includes('lowest') || lower.includes('minimum') || lower.includes('min');

  const extremaSql = buildExtremaSql(question, schema);
  if (extremaSql) {
    return extremaSql;
  }

  if (metrics.length > 1 && textColumn && (hasGroupingDimensionLanguage(question) || lower.includes('compare') || lower.includes(' vs ') || lower.includes(' and '))) {
    const selectMetrics = metrics
      .slice(0, 4)
      .map(metricCol => `SUM(${metricCol.name}) AS total_${metricCol.name}`)
      .join(', ');
    const orderMetric = `total_${metrics[0].name}`;
    const whereYear = yearFilter && dateColumn
      ? ` WHERE SUBSTR(${dateColumn.name}, 1, 4) = '${yearFilter}'`
      : '';
    return {
      sql_query: `SELECT ${textColumn.name}, ${selectMetrics} FROM ${table}${whereYear} GROUP BY ${textColumn.name} ORDER BY ${orderMetric} DESC LIMIT 20`,
      chart_type: 'bar_chart',
      mode: 'demo'
    };
  }

  if (metric && textColumn && (hasGroupingDimensionLanguage(question) || lower.includes('region') || lower.includes('category') || lower.includes('compare'))) {
    const whereYear = yearFilter && dateColumn
      ? ` WHERE SUBSTR(${dateColumn.name}, 1, 4) = '${yearFilter}'`
      : '';
    return {
      sql_query: `SELECT ${textColumn.name}, SUM(${metric.name}) AS total_value FROM ${table}${whereYear} GROUP BY ${textColumn.name} ORDER BY total_value DESC LIMIT 20`,
      chart_type: chartHint(question, inferAnalysisType(question)),
      mode: 'demo'
    };
  }

  if (metric && dateColumn && (lower.includes('monthly') || lower.includes('trend') || lower.includes('month') || lower.includes('time'))) {
    return {
      sql_query: `SELECT SUBSTR(${dateColumn.name}, 1, 7) AS month, SUM(${metric.name}) AS total_value FROM ${table} GROUP BY month ORDER BY month`,
      chart_type: 'line_chart',
      mode: 'demo'
    };
  }

  return {
    sql_query: `SELECT * FROM ${table} LIMIT 20`,
    chart_type: 'table',
    mode: 'demo'
  };
}

function extractRequestedAnswerCount(question) {
  const lower = String(question || '').toLowerCase();
  const match = lower.match(/\b(top|bottom|first|last)\s+(\d{1,3})\b/);
  if (!match) {
    return null;
  }
  const n = Number(match[2]);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function validateAnswerCoverage(question, executionResult) {
  const lower = String(question || '').toLowerCase();
  const rows = Array.isArray(executionResult?.rows) ? executionResult.rows : [];
  const columns = Array.isArray(executionResult?.columns)
    ? executionResult.columns.map(col => String(col).toLowerCase())
    : [];

  const asksHighest = lower.includes('highest') || lower.includes('maximum') || lower.includes('max');
  const asksLowest = lower.includes('lowest') || lower.includes('minimum') || lower.includes('min');

  if (asksHighest && asksLowest) {
    const hasHighest = columns.some(col => /(^|_)(highest|max)(_|$)/.test(col));
    const hasLowest = columns.some(col => /(^|_)(lowest|min)(_|$)/.test(col));
    if (!hasHighest || !hasLowest) {
      return {
        ok: false,
        message: 'Answer count mismatch: your question asks for both highest and lowest, but the generated result does not include both outputs.'
      };
    }
  }

  const requestedCount = extractRequestedAnswerCount(question);
  if (requestedCount != null && rows.length > requestedCount) {
    return {
      ok: false,
      message: `Answer count mismatch: your question asks for ${requestedCount} answers, but SQL returned ${rows.length}.`
    };
  }

  return { ok: true };
}

function validateSqlCoverage(question, schema, sqlQuery, queryPlan) {
  let parsed;
  try {
    parsed = parseSqlQuery(sqlQuery);
  } catch (error) {
    return { ok: false, message: `SQL parse failed during coverage check: ${error.message}` };
  }

  const requestedMetrics = Array.isArray(queryPlan?.metrics) ? queryPlan.metrics : [];
  const hasGrouping = parsed.groupByColumns.length > 0;
  const selectedMetricColumns = parsed.selectExpressions
    .filter(expr => expr.kind === 'column')
    .map(expr => expr.source.toLowerCase());
  if (requestedMetrics.length > 0) {
    const selectedMetricSources = parsed.selectExpressions
      .filter(expr => expr.kind === 'aggregate')
      .map(expr => expr.source.toLowerCase());

    const missingMetrics = requestedMetrics.filter(metricName => {
      const metricLower = String(metricName).toLowerCase();
      if (selectedMetricSources.includes(metricLower)) {
        return false;
      }
      // For non-grouped detail/ranking queries, metric can appear as a direct selected column.
      if (!hasGrouping && selectedMetricColumns.includes(metricLower)) {
        return false;
      }
      return true;
    });
    if (missingMetrics.length > 0) {
      return {
        ok: false,
        message: `SQL coverage mismatch: missing requested metric(s): ${missingMetrics.join(', ')}`
      };
    }
  }

  const wantsGrouping = /\bby\b/i.test(String(question || '')) || (queryPlan && queryPlan.analysis_type === 'comparison');
  if (wantsGrouping && parsed.groupByColumns.length === 0) {
    return {
      ok: false,
      message: 'SQL coverage mismatch: query asks for category comparison but SQL has no GROUP BY.'
    };
  }

  const requestedDimensions = Array.isArray(queryPlan?.dimensions) ? queryPlan.dimensions : [];
  if (requestedDimensions.length > 0 && wantsGrouping) {
    const missingDimensions = requestedDimensions.filter(dimension => !parsed.groupByColumns.includes(dimension));
    if (missingDimensions.length > 0) {
      return {
        ok: false,
        message: `SQL coverage mismatch: missing GROUP BY dimension(s): ${missingDimensions.join(', ')}`
      };
    }
  }

  const yearFilter = queryPlan?.filters?.year;
  if (yearFilter) {
    const lowerSql = String(sqlQuery || '').toLowerCase();
    const hasYearLiteral = lowerSql.includes(String(yearFilter));
    const dateColumn = schema.columns.find(column => column.type === 'DATE' || /date/i.test(column.name));
    const hasDateReference = dateColumn
      ? (lowerSql.includes(dateColumn.name.toLowerCase()) || parsed.whereConditions.some(cond => String(cond.column || '').toLowerCase() === dateColumn.name.toLowerCase()))
      : false;
    if (!hasYearLiteral || !hasDateReference) {
      return {
        ok: false,
        message: `SQL coverage mismatch: expected year filter ${yearFilter} on date field.`
      };
    }
  }

  return { ok: true };
}

function performSqlDoubleCheck(question, schema, sqlQuery, queryPlan) {
  const lower = String(question || '').toLowerCase();
  let parsed;
  try {
    parsed = parseSqlQuery(sqlQuery);
  } catch (error) {
    return { ok: false, error: `Double-check failed: SQL parse error: ${error.message}` };
  }

  const schemaByName = new Map(schema.columns.map(column => [column.name, column.type]));
  const hasAggregate = parsed.selectExpressions.some(expr => expr.kind === 'aggregate');
  const hasGrouping = parsed.groupByColumns.length > 0;
  const aggregateSources = parsed.selectExpressions
    .filter(expr => expr.kind === 'aggregate' && expr.source !== '*')
    .map(expr => expr.source);
  const selectedColumns = parsed.selectExpressions
    .filter(expr => expr.kind === 'column' && expr.source !== '*')
    .map(expr => expr.source);

  // 1) Ensure all requested metrics are present in aggregate expressions.
  const requestedMetrics = Array.isArray(queryPlan?.metrics) ? queryPlan.metrics : [];
  if (requestedMetrics.length > 0) {
    const missing = requestedMetrics.filter(metric => {
      if (aggregateSources.includes(metric)) {
        return false;
      }
      // Non-grouped ranking/detail query can use direct metric column selection.
      if (!hasGrouping && selectedColumns.includes(metric)) {
        return false;
      }
      return true;
    });
    if (missing.length > 0) {
      return { ok: false, error: `Double-check failed: missing requested metric aggregate(s): ${missing.join(', ')}` };
    }
  }

  // 2) If question asks "by ..." then ensure GROUP BY includes the resolved dimension.
  const requestedDimensions = Array.isArray(queryPlan?.dimensions) ? queryPlan.dimensions : [];
  const asksBy = hasGroupingDimensionLanguage(question);
  if (asksBy && requestedDimensions.length > 0) {
    const missingDim = requestedDimensions.filter(dimension => !parsed.groupByColumns.includes(dimension));
    if (missingDim.length > 0) {
      return { ok: false, error: `Double-check failed: missing GROUP BY dimension(s): ${missingDim.join(', ')}` };
    }
  }

  // 3) Prevent grouping by numeric metrics when aggregates are used.
  if (hasAggregate && parsed.groupByColumns.length > 0) {
    const invalidNumericGroupBy = parsed.groupByColumns.filter(column => {
      if (schemaByName.get(column) !== 'NUMBER') {
        return false;
      }
      return !/(^|_)id$/i.test(column);
    });
    if (invalidNumericGroupBy.length > 0) {
      return {
        ok: false,
        error: `Double-check failed: GROUP BY uses numeric column(s): ${invalidNumericGroupBy.join(', ')}. Use categorical dimensions.`
      };
    }
  }

  // 4) Ensure multi-metric comparison queries actually include multiple aggregate metrics.
  const isCorrelationQuery = queryPlan?.analysis_type === 'correlation' || queryPlan?.no_aggregation === true;
  const comparisonSignal = /\b(compare|comparison|vs|versus|along with|and)\b/.test(lower) || queryPlan?.analysis_type === 'comparison';
  if (!isCorrelationQuery && comparisonSignal && requestedMetrics.length >= 2) {
    const distinctAggregates = new Set(aggregateSources);
    if (distinctAggregates.size < 2) {
      return {
        ok: false,
        error: 'Double-check failed: comparison query requested multiple metrics but SQL includes fewer than 2 aggregated metrics.'
      };
    }
  }

  // 5) Ranking intent should include ORDER BY for meaningful top/bottom answers.
  const asksRanking = /\b(top|highest|max|best|bottom|lowest|min|rank)\b/.test(lower);
  if (asksRanking && !parsed.orderBy) {
    return {
      ok: false,
      error: 'Double-check failed: ranking intent detected but ORDER BY is missing.'
    };
  }

  return { ok: true };
}

function isComparisonQuestion(question, queryPlan) {
  const lower = String(question || '').toLowerCase();
  if (queryPlan && queryPlan.analysis_type === 'comparison') {
    return true;
  }
  return /\b(compare|comparison|vs|versus|along with)\b/.test(lower) || /\bby\b/.test(lower);
}

function pickChartForComplexQuestion(question, queryPlan, requestedChart, recommendedChart, allChartOptions) {
  const options = Array.isArray(allChartOptions) ? allChartOptions : [];
  const fallback = options.includes(recommendedChart) ? recommendedChart : (options[0] || 'table');
  const requestedValid = options.includes(requestedChart) ? requestedChart : null;

  const explicitChartRequested = (() => {
    const lower = String(question || '').toLowerCase();
    return [
      'donut', 'doughnut', 'pie', 'polar area', 'radar', 'scatter', 'bubble',
      'lollipop', 'horizontal bar', 'bar', 'area', 'step line', 'step chart', 'step trend', 'line'
    ].some(token => lower.includes(token));
  })();

  if (requestedValid && explicitChartRequested) {
    return requestedValid;
  }

  if (isComparisonQuestion(question, queryPlan)) {
    const forcedOrder = ['bar_chart', 'horizontal_bar_chart', 'ranked_table', 'table'];
    const forced = forcedOrder.find(chart => options.includes(chart));

    // Respect explicit user chart request if it is comparison-friendly.
    const comparisonFriendly = new Set(['bar_chart', 'horizontal_bar_chart', 'pie_chart', 'donut_chart', 'polar_area_chart', 'radar_chart', 'ranked_table', 'table']);
    if (requestedValid && comparisonFriendly.has(requestedValid)) {
      return requestedValid;
    }

    if (forced) {
      return forced;
    }
  }

  return requestedValid || fallback;
}

async function callGemini(prompt) {
  const response = await fetch(`https://generativelanguage.googleapis.com/v1beta/${GEMINI_MODEL}:generateContent?key=${geminiApiKey}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      contents: [
        {
          parts: [{ text: prompt }]
        }
      ]
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Gemini request failed: ${response.status} ${errorText}`);
  }

  const data = await response.json();
  return data.candidates?.[0]?.content?.parts?.map(part => part.text || '').join('') || '';
}

function extractJson(text) {
  const cleaned = text.trim().replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```$/i, '').trim();
  const start = cleaned.indexOf('{');
  const end = cleaned.lastIndexOf('}');
  if (start === -1 || end === -1 || end <= start) {
    throw new Error('AI did not return JSON.');
  }
  return JSON.parse(cleaned.slice(start, end + 1));
}

function normalizeAiResponse(rawObject, question) {
  const sql = (rawObject.sql_query || rawObject.sql || rawObject.query || '').toString().trim();
  const allowedCharts = new Set(CHART_TYPES);
  const rawChart = (rawObject.chart_type || rawObject.chart || '').toString().trim().toLowerCase();
  const chart = allowedCharts.has(rawChart) ? rawChart : chartHint(question, inferAnalysisType(question));

  if (!sql) {
    throw new Error('AI response missing sql_query.');
  }

  return {
    sql_query: sql.replace(/;+\s*$/, ''),
    chart_type: chart
  };
}

function getConversation(sessionId) {
  const key = sessionId || 'default';
  if (!conversations.has(key)) {
    conversations.set(key, []);
  }
  return conversations.get(key);
}

function pushConversationEntry(sessionId, entry) {
  const history = getConversation(sessionId);
  history.push(entry);
  if (history.length > MAX_HISTORY) {
    history.splice(0, history.length - MAX_HISTORY);
  }
}

function getQueryHistory(sessionId) {
  const key = sessionId || 'default';
  if (!queryHistoryStore.has(key)) {
    queryHistoryStore.set(key, []);
  }
  return queryHistoryStore.get(key);
}

function pushQueryHistoryEntry(sessionId, entry) {
  const history = getQueryHistory(sessionId);
  history.push(entry);
  if (history.length > MAX_QUERY_REPORT_HISTORY) {
    history.splice(0, history.length - MAX_QUERY_REPORT_HISTORY);
  }
}

function truncateCell(value, limit = 26) {
  const text = String(value == null ? '' : value);
  return text.length > limit ? `${text.slice(0, Math.max(0, limit - 1))}…` : text;
}

function summarizeDatasets(history) {
  const grouped = new Map();
  history.forEach(item => {
    const key = item.file_name || item.table_name || 'dataset';
    if (!grouped.has(key)) {
      grouped.set(key, {
        dataset: key,
        rows: Number(item.dataset_summary?.rows_analyzed) || 0,
        columns: Number(item.dataset_summary?.total_columns) || 0,
        metrics: new Set(item.dataset_summary?.detected_metrics || []),
        dimensions: new Set(item.dataset_summary?.detected_dimensions || []),
        timeColumns: new Set(item.dataset_summary?.time_columns || [])
      });
      return;
    }

    const current = grouped.get(key);
    current.rows = Math.max(current.rows, Number(item.dataset_summary?.rows_analyzed) || 0);
    current.columns = Math.max(current.columns, Number(item.dataset_summary?.total_columns) || 0);
    (item.dataset_summary?.detected_metrics || []).forEach(metric => current.metrics.add(metric));
    (item.dataset_summary?.detected_dimensions || []).forEach(dimension => current.dimensions.add(dimension));
    (item.dataset_summary?.time_columns || []).forEach(timeColumn => current.timeColumns.add(timeColumn));
  });

  return Array.from(grouped.values()).map(entry => ({
    ...entry,
    metrics: Array.from(entry.metrics),
    dimensions: Array.from(entry.dimensions),
    timeColumns: Array.from(entry.timeColumns)
  }));
}

function drawBarChart(doc, labels, values, left, top, width, height) {
  const safeLabels = labels.slice(0, 12);
  const safeValues = values.slice(0, 12).map(value => Number(value) || 0);
  const maxVal = Math.max(...safeValues, 1);
  const barGap = 6;
  const plotWidth = width - 40;
  const plotHeight = height - 34;
  const barWidth = Math.max(8, (plotWidth - ((safeValues.length - 1) * barGap)) / Math.max(safeValues.length, 1));
  const originX = left + 30;
  const originY = top + 8 + plotHeight;

  doc.save();
  doc.lineWidth(0.6).strokeColor('#8c94a8');
  doc.moveTo(originX, top + 8).lineTo(originX, originY).lineTo(originX + plotWidth, originY).stroke();

  safeValues.forEach((value, index) => {
    const x = originX + index * (barWidth + barGap);
    const barHeight = Math.max(1, Math.round((value / maxVal) * (plotHeight - 8)));
    doc.rect(x, originY - barHeight, barWidth, barHeight).fillAndStroke('#4c9ffe', '#2f6fcc');
    doc.fillColor('#3e4555').fontSize(7).text(truncateCell(safeLabels[index], 10), x - 2, originY + 2, { width: barWidth + 4, align: 'center' });
  });
  doc.restore();
}

function drawLineChart(doc, labels, values, left, top, width, height) {
  const safeLabels = labels.slice(0, 16);
  const safeValues = values.slice(0, 16).map(value => Number(value) || 0);
  const minVal = Math.min(...safeValues);
  const maxVal = Math.max(...safeValues, minVal + 1);
  const range = maxVal - minVal || 1;
  const originX = left + 24;
  const originY = top + height - 16;
  const plotWidth = width - 34;
  const plotHeight = height - 28;

  doc.save();
  doc.lineWidth(0.6).strokeColor('#8c94a8');
  doc.moveTo(originX, top + 8).lineTo(originX, originY).lineTo(originX + plotWidth, originY).stroke();

  const stepX = safeValues.length > 1 ? plotWidth / (safeValues.length - 1) : 0;
  doc.lineWidth(1.6).strokeColor('#1d7cf2');
  safeValues.forEach((value, index) => {
    const x = originX + (index * stepX);
    const y = originY - ((value - minVal) / range) * (plotHeight - 8);
    if (index === 0) {
      doc.moveTo(x, y);
    } else {
      doc.lineTo(x, y);
    }
  });
  if (safeValues.length > 1) {
    doc.stroke();
  }

  safeValues.forEach((value, index) => {
    const x = originX + (index * stepX);
    const y = originY - ((value - minVal) / range) * (plotHeight - 8);
    doc.circle(x, y, 2).fill('#1d7cf2');
    if (index % Math.ceil(Math.max(1, safeValues.length / 5)) === 0) {
      doc.fillColor('#3e4555').fontSize(7).text(truncateCell(safeLabels[index], 12), x - 14, originY + 2, { width: 28, align: 'center' });
    }
  });
  doc.restore();
}

function drawScatterChart(doc, points, left, top, width, height) {
  const safePoints = points.slice(0, 80).map(point => ({ x: Number(point.x) || 0, y: Number(point.y) || 0 }));
  const xs = safePoints.map(point => point.x);
  const ys = safePoints.map(point => point.y);
  const minX = Math.min(...xs, 0);
  const maxX = Math.max(...xs, minX + 1);
  const minY = Math.min(...ys, 0);
  const maxY = Math.max(...ys, minY + 1);
  const rangeX = maxX - minX || 1;
  const rangeY = maxY - minY || 1;
  const originX = left + 28;
  const originY = top + height - 16;
  const plotWidth = width - 38;
  const plotHeight = height - 28;

  doc.save();
  doc.lineWidth(0.6).strokeColor('#8c94a8');
  doc.moveTo(originX, top + 8).lineTo(originX, originY).lineTo(originX + plotWidth, originY).stroke();
  doc.fillColor('#1d7cf2');
  safePoints.forEach(point => {
    const x = originX + ((point.x - minX) / rangeX) * (plotWidth - 4);
    const y = originY - ((point.y - minY) / rangeY) * (plotHeight - 6);
    doc.circle(x, y, 2).fill();
  });
  doc.restore();
}

function drawChartPreview(doc, entry) {
  const chartData = entry.chart_data || null;
  const chartType = String(entry.chart || entry.chart_type || 'table');
  const left = doc.x;
  const top = doc.y;
  const width = 500;
  const height = 165;

  doc.rect(left, top, width, height).strokeColor('#d7dce8').lineWidth(0.8).stroke();
  doc.fillColor('#1f2533').font('Helvetica-Bold').fontSize(10).text(`Visualization (${chartType.replace(/_/g, ' ')})`, left + 10, top + 8);

  if (entry.chart_image && typeof entry.chart_image === 'string' && entry.chart_image.startsWith('data:image/')) {
    try {
      const imageBase64 = entry.chart_image.replace(/^data:image\/[a-zA-Z0-9.+-]+;base64,/, '');
      const imageBuffer = Buffer.from(imageBase64, 'base64');
      doc.image(imageBuffer, left + 10, top + 28, { fit: [width - 20, height - 38], align: 'center', valign: 'center' });
      doc.y = top + height + 6;
      return;
    } catch (_error) {
      // Fall back to the server-side preview below if the image payload is invalid.
    }
  }

  if (!chartData) {
    doc.fillColor('#6a748a').font('Helvetica').fontSize(9).text('No chart data available for this analysis.', left + 12, top + 32);
    doc.moveDown(12);
    return;
  }

  if (Array.isArray(chartData.points) && chartData.points.length > 0) {
    drawScatterChart(doc, chartData.points, left, top + 22, width, height - 24);
    doc.y = top + height + 6;
    return;
  }

  const labels = Array.isArray(chartData.labels) ? chartData.labels : [];
  const values = Array.isArray(chartData.values) ? chartData.values : [];
  if (labels.length && values.length) {
    const normalizedType = chartType.toLowerCase();
    if (normalizedType.includes('line') || normalizedType.includes('area') || normalizedType.includes('step')) {
      drawLineChart(doc, labels, values, left, top + 22, width, height - 24);
    } else {
      drawBarChart(doc, labels, values, left, top + 22, width, height - 24);
    }
    doc.y = top + height + 6;
    return;
  }

  if (chartData.multi_series && Array.isArray(chartData.values) && Array.isArray(chartData.labels)) {
    drawBarChart(doc, chartData.labels, chartData.values, left, top + 22, width, height - 24);
    doc.y = top + height + 6;
    return;
  }

  doc.fillColor('#6a748a').font('Helvetica').fontSize(9).text('Visualization preview unavailable for this chart payload.', left + 12, top + 32);
  doc.y = top + height + 6;
}

function renderResultTable(doc, entry) {
  const rows = Array.isArray(entry.result) ? entry.result : [];
  const columns = Array.isArray(entry.result_columns) && entry.result_columns.length
    ? entry.result_columns
    : (rows.length ? Object.keys(rows[0]) : []);

  doc.font('Helvetica-Bold').fontSize(11).fillColor('#1f2533').text('Result Table');
  doc.moveDown(0.2);

  if (!rows.length || !columns.length) {
    doc.font('Helvetica').fontSize(9).fillColor('#6a748a').text('No rows returned.');
    doc.moveDown(0.6);
    return;
  }

  const header = columns.map(column => truncateCell(column, 18)).join(' | ');
  doc.font('Helvetica-Bold').fontSize(8).fillColor('#243047').text(header);
  doc.moveDown(0.2);

  rows.slice(0, 12).forEach(row => {
    const line = columns.map(column => truncateCell(row[column], 18)).join(' | ');
    doc.font('Helvetica').fontSize(8).fillColor('#34415f').text(line);
  });

  if (rows.length > 12) {
    doc.moveDown(0.2);
    doc.font('Helvetica-Oblique').fontSize(8).fillColor('#6a748a').text(`Showing 12 of ${rows.length} rows.`);
  }
  doc.moveDown(0.6);
}

function writeReportPdf(res, historyEntries, contextInfo = {}) {
  const doc = new PDFDocument({ size: 'A4', margin: 42, bufferPages: true });
  const generatedAt = new Date();

  res.writeHead(200, {
    'Content-Type': 'application/pdf',
    'Content-Disposition': `attachment; filename="analysis-report-${generatedAt.toISOString().slice(0, 10)}.pdf"`
  });

  doc.pipe(res);

  doc.font('Helvetica-Bold').fontSize(24).fillColor('#10223f').text('Analytics Report', { align: 'center' });
  doc.moveDown(0.5);
  doc.font('Helvetica').fontSize(12).fillColor('#3a4a68').text('INTELLI-QUERY Session Export', { align: 'center' });
  doc.moveDown(0.8);
  doc.fontSize(10).fillColor('#596685').text(`Session: ${contextInfo.sessionId || 'default'}`, { align: 'center' });
  doc.text(`Generated: ${generatedAt.toISOString()}`, { align: 'center' });
  doc.text(`Analyses captured: ${historyEntries.length}`, { align: 'center' });

  doc.addPage();
  doc.font('Helvetica-Bold').fontSize(16).fillColor('#10223f').text('Dataset Summary');
  doc.moveDown(0.4);

  const datasetSummary = summarizeDatasets(historyEntries);
  if (!datasetSummary.length) {
    doc.font('Helvetica').fontSize(10).fillColor('#5f6b83').text('No dataset metadata available.');
  } else {
    datasetSummary.forEach(entry => {
      doc.font('Helvetica-Bold').fontSize(11).fillColor('#203055').text(`Dataset: ${entry.dataset}`);
      doc.font('Helvetica').fontSize(9).fillColor('#394662').text(`Rows analyzed: ${entry.rows || 0}`);
      doc.text(`Total columns: ${entry.columns || 0}`);
      doc.text(`Metrics: ${entry.metrics.length ? entry.metrics.join(', ') : 'None'}`);
      doc.text(`Dimensions: ${entry.dimensions.length ? entry.dimensions.join(', ') : 'None'}`);
      doc.text(`Time columns: ${entry.timeColumns.length ? entry.timeColumns.join(', ') : 'None'}`);
      doc.moveDown(0.6);
    });
  }

  historyEntries.forEach((entry, index) => {
    doc.addPage();
    doc.font('Helvetica-Bold').fontSize(15).fillColor('#10223f').text(`Analysis ${index + 1}`);
    doc.moveDown(0.2);

    doc.font('Helvetica-Bold').fontSize(11).fillColor('#1f2533').text('Question Asked');
    doc.font('Helvetica').fontSize(10).fillColor('#34415f').text(String(entry.question || 'N/A'));
    doc.moveDown(0.4);

    doc.font('Helvetica-Bold').fontSize(11).fillColor('#1f2533').text('Generated SQL Query');
    doc.font('Courier').fontSize(8.5).fillColor('#1f2533').text(String(entry.sql || 'N/A'), { width: 510 });
    doc.moveDown(0.4);

    doc.font('Helvetica-Bold').fontSize(11).fillColor('#1f2533').text('SQL Explanation');
    doc.font('Helvetica').fontSize(9.5).fillColor('#34415f').text(String(entry.explanation || 'N/A'));
    doc.moveDown(0.4);

    if (entry.narrative) {
      doc.font('Helvetica-Bold').fontSize(11).fillColor('#1f2533').text('Narrative Summary');
      doc.font('Helvetica').fontSize(9.5).fillColor('#34415f').text(String(entry.narrative));
      doc.moveDown(0.4);
    }

    renderResultTable(doc, entry);
    drawChartPreview(doc, entry);

    doc.font('Helvetica-Bold').fontSize(11).fillColor('#1f2533').text('AI Insight');
    doc.font('Helvetica').fontSize(9.5).fillColor('#34415f').text(String(entry.insight || 'N/A'));
    doc.moveDown(0.3);
    doc.font('Helvetica-Oblique').fontSize(8).fillColor('#6a748a').text(`Timestamp: ${entry.time || ''}`);
  });

  doc.end();
}

function buildSingleReportEntry(body) {
  const payload = body && body.payload && typeof body.payload === 'object' ? body.payload : null;
  if (!payload) {
    throw new Error('payload is required to generate the current query report.');
  }

  return {
    question: String(body.question || 'Current business question'),
    sql: String(payload.sql_query || ''),
    explanation: String(payload.sql_explanation || ''),
    result: Array.isArray(payload.result_rows) ? payload.result_rows : [],
    result_columns: Array.isArray(payload.result_columns) ? payload.result_columns : [],
    chart: String(payload.chart_type || payload.recommended_chart || 'table'),
    chart_type: String(payload.chart_type || payload.recommended_chart || 'table'),
    chart_data: payload.chart_data || null,
    chart_image: typeof body.chartImage === 'string' ? body.chartImage : '',
    insight: String(payload.insight || body.insight || ''),
    narrative: String(body.narrative || ''),
    time: new Date().toISOString(),
    file_name: payload.file_name || body.fileName || '',
    table_name: payload.table_name || '',
    dataset_summary: {
      dataset_name: payload.file_name || body.fileName || '',
      rows_analyzed: Number(payload.rows_loaded) || (Array.isArray(payload.result_rows) ? payload.result_rows.length : 0),
      total_columns: Array.isArray(payload.result_columns) ? payload.result_columns.length : 0,
      detected_metrics: Array.isArray(payload.schema_intelligence?.metric_candidates) ? payload.schema_intelligence.metric_candidates : [],
      detected_dimensions: Array.isArray(payload.schema_intelligence?.dimension_candidates) ? payload.schema_intelligence.dimension_candidates : [],
      time_columns: Array.isArray(payload.schema_intelligence?.date_columns) ? payload.schema_intelligence.date_columns : []
    }
  };
}

async function generateSql(question, schema, intent, selectedColumns, history, retryContext = null, parentContext = null, queryPlan = null) {
  if (!isAiEnabled()) {
    return buildDemoSql(question, schema);
  }

  const schemaBlock = schema.columns
    .filter(column => selectedColumns.includes(column.name))
    .map(column => `- ${column.name} (${column.type})`)
    .join('\n');

  const historyBlock = history.length === 0
    ? 'No conversation history.'
    : history.map((item, index) => `${index + 1}. Q: ${item.question}\n   SQL: ${item.sql_query}`).join('\n');

  const retryBlock = retryContext
    ? `\nPREVIOUS SQL FAILED:\n${retryContext.sql}\nERROR:\n${retryContext.error}\nRegenerate valid SQL.`
    : '';

  const parentBlock = parentContext
    ? `\nCURRENT RESULT CONTEXT (user is asking a follow-up about this result):\nParent SQL: ${parentContext.sql}\nResult columns: ${parentContext.columns.join(', ')}\nRow count: ${parentContext.rowCount}\nSample rows (first 3): ${JSON.stringify(parentContext.sample)}\nThe new query should refine, filter, or drill into this result using the same table.`
    : '';

  const fullSchemaBlock = schema.columns
    .map(column => `- ${column.name} (${column.type})`)
    .join('\n');
  const intelligence = getSchemaIntelligence(schema);
  const intelligenceBlock = [
    `- Numeric columns: ${(intelligence.numeric_columns || []).join(', ') || 'none'}`,
    `- Categorical columns: ${(intelligence.categorical_columns || []).join(', ') || 'none'}`,
    `- Date columns: ${(intelligence.date_columns || []).join(', ') || 'none'}`,
    `- Text columns: ${(intelligence.text_columns || []).join(', ') || 'none'}`,
    `- Metric candidates: ${(intelligence.metric_candidates || []).join(', ') || 'none'}`,
    `- Dimension candidates: ${(intelligence.dimension_candidates || []).join(', ') || 'none'}`
  ].join('\n');

  const prompt = [
    'You are a SQL generator for a conversational business intelligence dashboard.',
    'Return JSON only with keys: dimensions, metrics, filters, analysis_type, sql_query, chart_type.',
    'Use only one table and only the provided columns.',
    'Target SQL dialect: SQLite-like syntax with SELECT, WHERE, GROUP BY, ORDER BY, LIMIT, SUM/COUNT/AVG/MIN/MAX and SUBSTR.',
    'If user follow-up is contextual (for example "only North region"), use conversation history to update previous query intent.',
    'If the user asks for both highest and lowest, include both MAX and MIN in the same query response.',
    'Never use destructive SQL. Never use joins.',
    '',
    'CRITICAL DIMENSION vs METRIC RULES:',
    '- When the user asks "show X by Y": X is the METRIC (numeric column to aggregate), Y is the DIMENSION (categorical/text column to GROUP BY).',
    '- The GROUP BY column MUST be a TEXT or categorical column — NEVER group by a numeric metric column.',
    '- Example: "Show clicks by marketing channel" → GROUP BY Channel_Used, SUM(Clicks). NOT GROUP BY Clicks.',
    '- Example: "Show revenue by campaign type" → GROUP BY Campaign_Type, SUM(Revenue). NOT GROUP BY Revenue.',
    '- If the dimension word (after "by") does not exactly match a column name, find the closest TEXT column by meaning.',
    '- Never ignore metrics mentioned in the user query.',
    '- Never collapse multi-metric queries into a single metric query.',
    '- If user uses compare / vs / and / along with for metrics, include all requested metrics.',
    '',
    `User question: ${question}`,
    `Table name: ${schema.tableName}`,
    `Intent: metric=${intent.metric} (aggregate this), dimension=${intent.dimension} (GROUP BY this), time=${intent.time}, aggregation=${intent.aggregation}`,
    '',
    'Full table schema (all columns):',
    fullSchemaBlock,
    '',
    'Dataset intelligence:',
    intelligenceBlock,
    '',
    'Relevant columns for this query:',
    schemaBlock,
    'Structured query plan to satisfy:',
    JSON.stringify(queryPlan || {}, null, 2),
    'Conversation history:',
    historyBlock,
    parentBlock,
    'Chart options: line_chart, bar_chart, pie_chart, donut_chart, area_chart, scatter_plot, radar_chart, polar_area_chart, horizontal_bar_chart, lollipop_chart, ranked_table, table.',
    retryBlock
  ].join('\n');

  try {
    const rawText = await callGemini(prompt);
    return normalizeAiResponse(extractJson(rawText), question);
  } catch (error) {
    const fallback = buildDemoSql(question, schema);
    const reasonText = (error && error.message ? error.message : '').toLowerCase();
    if (reasonText.includes('429') || reasonText.includes('quota') || reasonText.includes('resource_exhausted')) {
      fallback.mode = 'demo_quota_fallback';
      return fallback;
    }
    fallback.mode = 'demo_api_fallback';
    return fallback;
  }
}

function validateSql(sqlQuery, tableName) {
  const upper = sqlQuery.toUpperCase().trim();
  if (!upper.startsWith('SELECT')) {
    return { ok: false, error: 'Only SELECT queries are allowed.' };
  }

  const blocked = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'INSERT', 'UPDATE', 'CREATE'];
  for (const keyword of blocked) {
    if (new RegExp(`\\b${keyword}\\b`, 'i').test(upper)) {
      return { ok: false, error: `Blocked keyword detected: ${keyword}` };
    }
  }

  if (!sqlQuery.toLowerCase().includes(tableName.toLowerCase())) {
    return { ok: false, error: `Expected table "${tableName}" not found in query.` };
  }

  return { ok: true };
}

function validateSqlAgainstSchema(sqlQuery, schema) {
  const base = validateSql(sqlQuery, schema.tableName);
  if (!base.ok) {
    return base;
  }

  let parsed;
  try {
    parsed = parseSqlQuery(sqlQuery);
  } catch (error) {
    return { ok: false, error: error.message };
  }

  const knownColumns = new Set(schema.columns.map(column => column.name));
  const aliases = new Set(parsed.selectExpressions.map(expr => expr.alias));

  const hasColumn = name => knownColumns.has(name);

  for (const expr of parsed.selectExpressions) {
    if (expr.kind === 'aggregate' && expr.source !== '*' && !hasColumn(expr.source)) {
      return { ok: false, error: `Unknown column in aggregate: ${expr.source}` };
    }
    if (expr.kind === 'column' && expr.source !== '*' && !hasColumn(expr.source)) {
      return { ok: false, error: `Unknown column in SELECT: ${expr.source}` };
    }
    if (expr.kind === 'computed' && !hasColumn(expr.source)) {
      return { ok: false, error: `Unknown column in computed expression: ${expr.source}` };
    }
  }

  for (const condition of parsed.whereConditions) {
    if (!hasColumn(condition.column)) {
      return { ok: false, error: `Unknown column in WHERE: ${condition.column}` };
    }
  }

  for (const group of parsed.groupByColumns) {
    if (!hasColumn(group) && !aliases.has(group)) {
      return { ok: false, error: `Unknown column in GROUP BY: ${group}` };
    }
  }

  if (parsed.orderBy) {
    const orderMatch = parsed.orderBy.match(/^([a-zA-Z0-9_]+)/);
    if (orderMatch) {
      const orderField = orderMatch[1];
      if (!hasColumn(orderField) && !aliases.has(orderField)) {
        return { ok: false, error: `Unknown column in ORDER BY: ${orderField}` };
      }
    }
  }

  return { ok: true };
}

function splitCommaAware(input) {
  const parts = [];
  let token = '';
  let depth = 0;

  for (let i = 0; i < input.length; i += 1) {
    const ch = input[i];
    if (ch === '(') {
      depth += 1;
      token += ch;
      continue;
    }
    if (ch === ')') {
      depth = Math.max(depth - 1, 0);
      token += ch;
      continue;
    }
    if (ch === ',' && depth === 0) {
      parts.push(token.trim());
      token = '';
      continue;
    }
    token += ch;
  }
  if (token.trim()) {
    parts.push(token.trim());
  }
  return parts;
}

function parseSelectExpression(rawExpr) {
  const aggregateMatch = rawExpr.match(/^(sum|count|avg|min|max)\s*\(\s*(\*|[a-zA-Z0-9_]+)\s*\)\s*(?:as\s+([a-zA-Z0-9_]+))?$/i);
  if (aggregateMatch) {
    const fn = aggregateMatch[1].toLowerCase();
    const source = aggregateMatch[2];
    const alias = aggregateMatch[3] || `${fn}_${source === '*' ? 'all' : source}`;
    return { kind: 'aggregate', fn, source, alias };
  }

  const substrMatch = rawExpr.match(/^substr\s*\(\s*([a-zA-Z0-9_]+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)\s*(?:as\s+([a-zA-Z0-9_]+))?$/i);
  if (substrMatch) {
    const source = substrMatch[1];
    const start = Number(substrMatch[2]);
    const length = Number(substrMatch[3]);
    const alias = substrMatch[4] || `${source}_substr`;
    return { kind: 'computed', source, start, length, alias };
  }

  const colMatch = rawExpr.match(/^([a-zA-Z0-9_]+|\*)\s*(?:as\s+([a-zA-Z0-9_]+))?$/i);
  if (colMatch) {
    return {
      kind: 'column',
      source: colMatch[1],
      alias: colMatch[2] || colMatch[1]
    };
  }

  throw new Error(`Unsupported SELECT expression: ${rawExpr}`);
}

function parseWhereClause(whereClause) {
  if (!whereClause) {
    return [];
  }

  return whereClause
    .split(/\s+and\s+/i)
    .map(chunk => chunk.trim())
    .filter(Boolean)
    .map(chunk => {
      const nullMatch = chunk.match(/^([a-zA-Z0-9_]+)\s+is\s+(not\s+)?null$/i);
      if (nullMatch) {
        return {
          column: nullMatch[1],
          operator: nullMatch[2] ? 'is not null' : 'is null',
          value: null
        };
      }

      const match = chunk.match(/^([a-zA-Z0-9_]+)\s*(=|!=|<>|>=|<=|>|<|like)\s*(.+)$/i);
      if (!match) {
        throw new Error(`Unsupported WHERE condition: ${chunk}`);
      }

      let value = match[3].trim();
      if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
        value = value.slice(1, -1);
      } else if (/^-?\d+(\.\d+)?$/.test(value)) {
        value = Number(value);
      }

      return {
        column: match[1],
        operator: match[2].toLowerCase(),
        value
      };
    });
}

function parseSqlQuery(sqlQuery) {
  const sql = sqlQuery.trim().replace(/;+\s*$/, '');
  const main = sql.match(/^select\s+([\s\S]+?)\s+from\s+([a-zA-Z0-9_]+)([\s\S]*)$/i);
  if (!main) {
    throw new Error('Only single-table SELECT statements are supported.');
  }

  const selectClause = main[1].trim();
  const tableName = main[2].trim();
  const tail = main[3] || '';

  const whereMatch = tail.match(/\swhere\s+([\s\S]+?)(?=\sgroup\s+by|\sorder\s+by|\slimit\s+|$)/i);
  const groupByMatch = tail.match(/\sgroup\s+by\s+([\s\S]+?)(?=\sorder\s+by|\slimit\s+|$)/i);
  const orderByMatch = tail.match(/\sorder\s+by\s+([\s\S]+?)(?=\slimit\s+|$)/i);
  const limitMatch = tail.match(/\slimit\s+(\d+)/i);

  const selectExpressions = splitCommaAware(selectClause).map(parseSelectExpression);

  return {
    tableName,
    selectExpressions,
    whereConditions: parseWhereClause(whereMatch ? whereMatch[1].trim() : ''),
    groupByColumns: groupByMatch ? splitCommaAware(groupByMatch[1]).map(item => item.trim()) : [],
    orderBy: orderByMatch ? orderByMatch[1].trim() : '',
    limit: limitMatch ? Number(limitMatch[1]) : null
  };
}

function applyWhere(rows, conditions) {
  if (!conditions.length) {
    return rows;
  }

  return rows.filter(row => conditions.every(condition => {
    const left = row[condition.column];
    const right = condition.value;

    switch (condition.operator) {
      case 'is null':
        return left === null || left === undefined;
      case 'is not null':
        return left !== null && left !== undefined;
      case '=': return left === right;
      case '!=':
      case '<>': return left !== right;
      case '>': return Number(left) > Number(right);
      case '<': return Number(left) < Number(right);
      case '>=': return Number(left) >= Number(right);
      case '<=': return Number(left) <= Number(right);
      case 'like': {
        const pattern = String(right).replace(/%/g, '.*');
        return new RegExp(`^${pattern}$`, 'i').test(String(left ?? ''));
      }
      default:
        return false;
    }
  }));
}

function computeExpressionValue(expr, row) {
  if (expr.kind === 'column') {
    return expr.source === '*' ? row : row[expr.source];
  }
  if (expr.kind === 'computed') {
    const source = row[expr.source];
    const asText = source == null ? '' : String(source);
    return asText.substring(expr.start - 1, expr.start - 1 + expr.length);
  }
  return null;
}

function aggregateRows(rows, expression, defaultValue = null) {
  const numericValues = expression.source === '*'
    ? rows.map(() => 1)
    : rows.map(row => row[expression.source]).filter(value => typeof value === 'number');

  switch (expression.fn) {
    case 'sum':
      return numericValues.reduce((acc, value) => acc + value, 0);
    case 'count':
      if (expression.source === '*') {
        return rows.length;
      }
      return rows.filter(row => row[expression.source] !== null && row[expression.source] !== undefined).length;
    case 'avg':
      if (!numericValues.length) {
        return null;
      }
      return numericValues.reduce((acc, value) => acc + value, 0) / numericValues.length;
    case 'min':
      if (!rows.length) {
        return defaultValue;
      }
      return rows.reduce((min, row) => {
        const value = row[expression.source];
        return min === null || value < min ? value : min;
      }, null);
    case 'max':
      if (!rows.length) {
        return defaultValue;
      }
      return rows.reduce((max, row) => {
        const value = row[expression.source];
        return max === null || value > max ? value : max;
      }, null);
    default:
      return defaultValue;
  }
}

function applyOrderBy(rows, orderBy) {
  if (!orderBy) {
    return rows;
  }

  const sortFields = String(orderBy)
    .split(',')
    .map(part => part.trim())
    .filter(Boolean)
    .map(part => {
      const match = part.match(/^([a-zA-Z0-9_]+)\s*(asc|desc)?$/i);
      if (!match) {
        return null;
      }
      return {
        field: match[1],
        direction: (match[2] || 'asc').toLowerCase() === 'desc' ? -1 : 1
      };
    })
    .filter(Boolean);

  if (!sortFields.length) {
    return rows;
  }

  return [...rows].sort((a, b) => {
    for (const sortField of sortFields) {
      const left = a[sortField.field];
      const right = b[sortField.field];
      if (left == null && right == null) {
        continue;
      }
      if (left == null) {
        return -1 * sortField.direction;
      }
      if (right == null) {
        return 1 * sortField.direction;
      }
      if (left === right) {
        continue;
      }
      return left > right ? sortField.direction : -sortField.direction;
    }
    return 0;
  });
}

function splitMultiValueCell(value) {
  if (value == null) {
    return [];
  }
  return String(value)
    .split(/[;,]/)
    .map(item => item.trim())
    .filter(Boolean);
}

function expandRowsForGroupedMultiValueDimensions(rows, groupByColumns, schema) {
  if (!Array.isArray(rows) || !rows.length || !Array.isArray(groupByColumns) || !groupByColumns.length) {
    return rows;
  }

  const splitEligibleColumns = groupByColumns.filter(columnName => {
    const schemaCol = schema.columns.find(col => col.name === columnName);
    if (!schemaCol || schemaCol.type !== 'TEXT') {
      return false;
    }
    const name = columnName.toLowerCase();
    return name.includes('channel');
  });

  if (!splitEligibleColumns.length) {
    return rows;
  }

  const expanded = [];
  rows.forEach(row => {
    let variants = [{ ...row }];

    splitEligibleColumns.forEach(columnName => {
      const next = [];
      variants.forEach(variant => {
        const parts = splitMultiValueCell(variant[columnName]);
        if (!parts.length) {
          next.push(variant);
          return;
        }
        const uniqueParts = [...new Set(parts)];
        uniqueParts.forEach(part => {
          next.push({ ...variant, [columnName]: part });
        });
      });
      variants = next;
    });

    expanded.push(...variants);
  });

  return expanded;
}

function executeSql(sqlQuery, dataset) {
  const parsed = parseSqlQuery(sqlQuery);
  if (parsed.tableName.toLowerCase() !== dataset.schema.tableName.toLowerCase()) {
    throw new Error(`SQL table '${parsed.tableName}' does not match dataset table '${dataset.schema.tableName}'.`);
  }

  const db = dataset.sqliteDb || buildSqliteDatabase(dataset.schema, dataset.rows);
  const queryResults = db.exec(sqlQuery);
  if (!queryResults.length) {
    return {
      rows: [],
      columns: parsed.selectExpressions.map(expr => expr.alias),
      row_count: 0
    };
  }

  const first = queryResults[0];
  const columns = first.columns || [];
  const results = (first.values || []).map(values => {
    const row = {};
    columns.forEach((column, index) => {
      row[column] = values[index];
    });
    return row;
  });
  return {
    rows: results,
    columns,
    row_count: results.length
  };
}

function buildChartData(rows) {
  if (!rows || rows.length === 0) {
    return null;
  }

  const columns = Object.keys(rows[0]);
  if (columns.length < 1) {
    return null;
  }

  const numericCols = columns.filter(col => rows.some(row => typeof row[col] === 'number'));
  const textCols = columns.filter(col => !numericCols.includes(col));

  // Relationship output (two numeric metrics) -> scatter-friendly x/y points.
  if (numericCols.length >= 2 && textCols.length === 0) {
    const xField = numericCols[0];
    const yField = numericCols[1];
    return {
      scatter: true,
      x_field: xField,
      y_field: yField,
      points: rows.map(row => ({
        x: Number(row[xField]) || 0,
        y: Number(row[yField]) || 0
      }))
    };
  }

  // Single row with 2+ numeric columns → comparison/multi-series bar
  if (rows.length === 1 && numericCols.length >= 2) {
    return {
      multi_series: true,
      labels: numericCols.map(c => c.replace(/_/g, ' ')),
      values: numericCols.map(c => Number(rows[0][c]) || 0),
      label_field: 'metric',
      value_field: 'value'
    };
  }

  // Standard: label column + one numeric value column
  const labelColumn = textCols[0] || columns[0];
  const valueColumn = numericCols[0] || columns[1];
  return {
    labels: rows.map(row => String(row[labelColumn])),
    values: rows.map(row => Number(row[valueColumn]) || 0),
    label_field: labelColumn,
    value_field: valueColumn
  };
}

function detectDataStructure(schema, rows, resultColumns) {
  if (!rows || rows.length === 0) {
    return {
      dimension: 'none',
      metric: 'none',
      type: 'tabular'
    };
  }

  const schemaByName = new Map(schema.columns.map(col => [col.name, col.type]));
  const numeric = resultColumns.filter(column => rows.some(row => typeof row[column] === 'number'));
  const dateLike = resultColumns.filter(column => {
    if (schemaByName.get(column) === 'DATE') {
      return true;
    }
    const normalized = String(column || '').toLowerCase().trim();
    return /^(date|month|year|time)$/.test(normalized)
      || /(^|_)(date|month|year|time)($|_)/.test(normalized);
  });
  const nonNumeric = resultColumns.filter(column => !numeric.includes(column));

  const metric = numeric[0] || 'none';
  const dimension = (dateLike[0] || nonNumeric[0] || 'none');

  let type = 'tabular';
  if (dateLike.length > 0 && numeric.length > 0) {
    type = 'time_series';
  } else if (numeric.length > 0 && nonNumeric.length > 0) {
    type = rows.length > 20 ? 'ranking' : 'categorical_comparison';
  } else if (numeric.length >= 2) {
    type = 'relationship';
  } else if (numeric.length === 1 && nonNumeric.length === 0) {
    type = 'distribution';
  }

  return { dimension, metric, type };
}

function listCompatibleChartTypes(dataStructure, rowCount) {
  const type = dataStructure.type;

  if (type === 'time_series') {
    return ['line_chart', 'area_chart', 'step_line_chart', 'bar_chart', 'radar_chart', 'table'];
  }
  if (type === 'relationship') {
    return ['scatter_plot', 'bubble_chart', 'radar_chart', 'table'];
  }
  if (type === 'distribution') {
    return ['pie_chart', 'donut_chart', 'polar_area_chart', 'radar_chart', 'table'];
  }
  if (type === 'ranking') {
    return ['horizontal_bar_chart', 'bar_chart', 'lollipop_chart', 'ranked_table', 'table'];
  }
  if (type === 'categorical_comparison') {
    if (rowCount > 12) {
      return ['horizontal_bar_chart', 'bar_chart', 'line_chart', 'lollipop_chart', 'table'];
    }
    return ['bar_chart', 'horizontal_bar_chart', 'pie_chart', 'donut_chart', 'radar_chart', 'polar_area_chart', 'lollipop_chart', 'table'];
  }
  return ['table', 'bar_chart', 'horizontal_bar_chart', 'line_chart', 'area_chart', 'pie_chart', 'donut_chart', 'radar_chart'];
}

function recommendChartOptions(dataStructure, rowCount) {
  return listCompatibleChartTypes(dataStructure, rowCount).slice(0, 4);
}

function buildInsight(rows, dataStructure) {
  if (!rows || rows.length === 0) {
    return 'No records matched this query. Try adjusting your filters.';
  }

  const { type, dimension, metric } = dataStructure;
  if (metric === 'none') {
    return `Returned ${rows.length} records. Consider adding a numeric metric for deeper insights.`;
  }

  const metricNumber = value => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  };

  const compareDimension = (left, right) => {
    const leftValue = left == null ? '' : String(left);
    const rightValue = right == null ? '' : String(right);
    const leftDate = Date.parse(leftValue);
    const rightDate = Date.parse(rightValue);
    if (Number.isFinite(leftDate) && Number.isFinite(rightDate)) {
      return leftDate - rightDate;
    }
    return leftValue.localeCompare(rightValue, undefined, { numeric: true, sensitivity: 'base' });
  };

  if (type === 'time_series' && rows.length >= 2) {
    const orderedRows = dimension && dimension !== 'none'
      ? [...rows].sort((left, right) => compareDimension(left[dimension], right[dimension]))
      : rows;
    const first = metricNumber(orderedRows[0][metric]) || 0;
    const last = metricNumber(orderedRows[orderedRows.length - 1][metric]) || 0;
    const trend = last >= first ? 'upward' : 'downward';
    return `${metric} shows a ${trend} trend over ${dimension}, moving from ${first.toLocaleString()} to ${last.toLocaleString()}.`;
  }

  if ((type === 'categorical_comparison' || type === 'ranking') && dimension !== 'none') {
    const sorted = [...rows]
      .map(row => ({ row, numericMetric: metricNumber(row[metric]) }))
      .filter(item => item.numericMetric !== null)
      .sort((left, right) => right.numericMetric - left.numericMetric);
    if (sorted.length > 0) {
      const top = sorted[0].row;
      const topValue = sorted[0].numericMetric;
      if (sorted.length > 1) {
        const second = sorted[1];
        const gap = topValue - second.numericMetric;
        return `${top[dimension]} has the highest ${metric} at ${topValue.toLocaleString()}, leading by ${gap.toLocaleString()} over ${second.row[dimension]}.`;
      }
      return `${top[dimension]} has the highest ${metric} at ${topValue.toLocaleString()}.`;
    }
  }

  const numericValues = rows.map(row => metricNumber(row[metric])).filter(value => value !== null);
  if (numericValues.length > 0) {
    const max = Math.max(...numericValues);
    const min = Math.min(...numericValues);
    return `${metric} ranges from ${min.toLocaleString()} to ${max.toLocaleString()} across ${rows.length} records.`;
  }

  return `Returned ${rows.length} records for analysis.`;
}

function explainSqlLocally(sqlQuery) {
  const sql = sqlQuery.toLowerCase();
  const pieces = [];

  if (sql.includes('group by')) {
    pieces.push('groups records to produce summaries');
  }
  if (sql.includes('sum(')) {
    pieces.push('calculates totals');
  }
  if (sql.includes('avg(')) {
    pieces.push('calculates averages');
  }
  if (sql.includes('count(')) {
    pieces.push('counts matching rows');
  }
  if (sql.includes('where')) {
    pieces.push('filters rows before aggregation');
  }
  if (sql.includes('order by')) {
    pieces.push('sorts the final result');
  }
  if (sql.includes('limit')) {
    pieces.push('limits how many rows are returned');
  }

  if (!pieces.length) {
    return 'This query selects data from the chosen dataset and returns it for visualization.';
  }
  return `This query ${pieces.join(', ')}.`;
}

function isIdLikeColumnName(name) {
  const normalized = String(name || '').toLowerCase().trim();
  if (!normalized) {
    return false;
  }
  return /^id$/.test(normalized)
    || /^id_/.test(normalized)
    || /_id$/.test(normalized)
    || /identifier|uuid|guid/.test(normalized);
}

function metricBusinessScore(name) {
  const normalized = String(name || '').toLowerCase();
  let score = 0;
  if (isIdLikeColumnName(normalized)) score -= 1000;
  if (/(revenue|sales|amount|profit|cost|price|income|gmv|value|total)/.test(normalized)) score += 120;
  if (/(conversion|click|impression|ctr|cpc|cpa|roi|rate|score|quantity|qty|units?)/.test(normalized)) score += 100;
  if (/(count|num|number)/.test(normalized)) score += 40;
  return score;
}

function pickPreferredMetric(schema, preferred = []) {
  const columns = Array.isArray(schema?.columns) ? schema.columns : [];
  const intelligence = getSchemaIntelligence(schema);
  const numericColumns = columns.filter(column => column.type === 'NUMBER').map(column => column.name);
  const metricCandidates = Array.isArray(intelligence?.metric_candidates) ? intelligence.metric_candidates : [];
  const merged = [...preferred, ...metricCandidates, ...numericColumns]
    .filter(Boolean)
    .map(name => String(name));
  const unique = [...new Set(merged)];
  if (!unique.length) {
    return columns[0]?.name || 'value';
  }

  unique.sort((a, b) => metricBusinessScore(b) - metricBusinessScore(a));
  return unique[0];
}

function pickPreferredDimension(schema, preferred = []) {
  const columns = Array.isArray(schema?.columns) ? schema.columns : [];
  const intelligence = getSchemaIntelligence(schema);
  const textColumns = columns.filter(column => column.type === 'TEXT').map(column => column.name);
  const dimensionCandidates = Array.isArray(intelligence?.dimension_candidates) ? intelligence.dimension_candidates : [];
  const merged = [...preferred, ...dimensionCandidates, ...textColumns]
    .filter(Boolean)
    .map(name => String(name));
  const unique = [...new Set(merged)];
  const nonId = unique.filter(name => !isIdLikeColumnName(name));
  if (nonId.length) {
    return nonId[0];
  }
  if (unique.length) {
    return unique[0];
  }
  return columns[0]?.name || 'category';
}

function normalizeDemoDifficulty(value) {
  const normalized = String(value || 'standard').trim().toLowerCase();
  return DEMO_DIFFICULTIES.includes(normalized) ? normalized : 'standard';
}

function fallbackDemoQueries(schema, count = DEMO_QUERY_COUNT, chartType = 'all', difficulty = 'standard') {
  const demoDifficulty = normalizeDemoDifficulty(difficulty);
  const metric = pickPreferredMetric(schema);
  const dimension = pickPreferredDimension(schema);
  const dateCol = schema.columns.find(column => column.type === 'DATE')?.name || schema.columns.find(column => column.name.toLowerCase().includes('date'))?.name || null;

  const simpleOptions = [
    { question: `Show total ${metric} by ${dimension}.`, chart_type: 'bar_chart' },
    { question: `Show ${metric} contribution by ${dimension} as a donut chart.`, chart_type: 'donut_chart' },
    { question: `Show ${metric} share by ${dimension}.`, chart_type: 'pie_chart' },
    { question: `Compare average ${metric} across ${dimension}.`, chart_type: 'bar_chart' },
    { question: `Show count of records by ${dimension}.`, chart_type: 'bar_chart' },
    { question: `List ${dimension} with ${metric} in a table.`, chart_type: 'table' }
  ];

  const standardOptions = [
    { question: `Which ${dimension} generated the highest total ${metric}?`, chart_type: 'horizontal_bar_chart' },
    { question: `What is the ${metric} breakdown across all ${dimension} categories?`, chart_type: 'bar_chart' },
    { question: `How is ${metric} distributed among different ${dimension}?`, chart_type: 'pie_chart' },
    { question: `Show ${metric} contribution by ${dimension} as a donut chart.`, chart_type: 'donut_chart' },
    { question: `Which ${dimension} has the lowest ${metric} performance?`, chart_type: 'horizontal_bar_chart' },
    { question: `Compare average ${metric} across all ${dimension} groups.`, chart_type: 'bar_chart' },
    { question: `What is the ${metric} performance radar for each ${dimension}?`, chart_type: 'radar_chart' },
    { question: `Show top 10 ${dimension} ranked by ${metric}.`, chart_type: 'ranked_table' },
    { question: `What are the top 5 and bottom 5 ${dimension} by ${metric}?`, chart_type: 'bar_chart' },
    { question: `Show count of records grouped by ${dimension}.`, chart_type: 'bar_chart' },
    { question: `How does ${metric} vary across ${dimension}? Show as lollipop chart.`, chart_type: 'lollipop_chart' },
    { question: `List all ${dimension} with their total ${metric} in a detailed table.`, chart_type: 'table' },
    { question: `Show scatter plot relationship between key numeric columns.`, chart_type: 'scatter_plot' }
  ];

  const complexOptions = [
    { question: `Compare total ${metric} and average ${metric} by ${dimension}.`, chart_type: 'bar_chart' },
    { question: `Show top 5 ${dimension} by ${metric} and compare with the bottom 5.`, chart_type: 'horizontal_bar_chart' },
    { question: `Show ${metric} contribution by ${dimension} as a donut chart with ranking context.`, chart_type: 'donut_chart' },
    { question: `Analyze the relationship between ${metric} and another numeric measure.`, chart_type: 'scatter_plot' },
    { question: `Show ranked table of ${dimension} by total ${metric} and count of records.`, chart_type: 'ranked_table' },
    { question: `Compare variance of ${metric} across ${dimension}.`, chart_type: 'lollipop_chart' },
    { question: `Show radar comparison of ${dimension} performance using ${metric}.`, chart_type: 'radar_chart' },
    { question: `Show bubble comparison of ${dimension} weighted by ${metric}.`, chart_type: 'bubble_chart' },
    { question: `List detailed rows for the top ${dimension} segments by ${metric}.`, chart_type: 'table' }
  ];

  let options = standardOptions;
  if (demoDifficulty === 'simple') {
    options = simpleOptions;
  } else if (demoDifficulty === 'complex') {
    options = [...standardOptions, ...complexOptions];
  }

  if (dateCol) {
    if (demoDifficulty === 'simple') {
      options.unshift({ question: `Show the ${metric} trend over ${dateCol}.`, chart_type: 'line_chart' });
    } else {
      options.unshift({ question: `How has ${metric} changed month by month? Show as step trend.`, chart_type: 'step_line_chart' });
      options.unshift({ question: `Show the cumulative ${metric} growth over time as an area chart.`, chart_type: 'area_chart' });
      options.unshift({ question: `What is the monthly ${metric} trend over time?`, chart_type: 'line_chart' });
    }
  }

  const chartSpecificTemplates = {
    line_chart: [
      `Show ${metric} trend by ${dimension}.`,
      `Show running ${metric} line ordered by ${dimension}.`,
      `Compare ${metric} movement across ${dimension}.`,
      `Show line view for average ${metric} by ${dimension}.`,
      `Show line comparison of top ${dimension} by ${metric}.`,
      `Show line chart for count of records by ${dimension}.`,
      `Show line pattern for total ${metric} by ${dimension}.`,
      `Show line trend for ${metric} grouped by ${dimension}.`
    ],
    area_chart: [
      `Show area view of total ${metric} by ${dimension}.`,
      `Show cumulative ${metric} area by ${dimension}.`,
      `Show stacked-style area for ${metric} across ${dimension}.`,
      `Compare area distribution of ${metric} by ${dimension}.`,
      `Show area trend of average ${metric} by ${dimension}.`,
      `Show area chart for count by ${dimension}.`,
      `Show filled trend of ${metric} over ${dimension}.`,
      `Show area contribution of ${dimension} to ${metric}.`
    ],
    step_line_chart: [
      `Show step trend of ${metric} by ${dimension}.`,
      `Show stepped comparison of ${metric} across ${dimension}.`,
      `Show step chart for average ${metric} by ${dimension}.`,
      `Show step transitions of ${metric} grouped by ${dimension}.`,
      `Show stepped pattern for count by ${dimension}.`,
      `Show step view of top ${dimension} by ${metric}.`,
      `Show step progression of ${metric} using ${dimension}.`,
      `Show step line profile for ${metric} by ${dimension}.`
    ],
    scatter_plot: [
      `Plot scatter relationship between key numeric columns by ${dimension}.`,
      `Show scatter of ${metric} versus another metric grouped by ${dimension}.`,
      `Show scatter clusters for ${dimension} using numeric values.`,
      `Compare outliers in ${metric} with a scatter view by ${dimension}.`,
      `Plot scatter points to inspect ${metric} spread by ${dimension}.`,
      `Show scatter distribution for numeric behavior across ${dimension}.`,
      `Use scatter to compare ${metric} intensity by ${dimension}.`,
      `Show scatter map of numeric variance grouped by ${dimension}.`
    ],
    bubble_chart: [
      `Show bubble chart for ${metric} by ${dimension} with size emphasis.`,
      `Compare ${dimension} using bubble size based on ${metric}.`,
      `Show bubble distribution of numeric intensity across ${dimension}.`,
      `Use bubble chart to highlight top ${dimension} contributors.`,
      `Show bubble comparison for average ${metric} by ${dimension}.`,
      `Plot bubble view of ${metric} spread grouped by ${dimension}.`,
      `Show bubble outlier check for ${metric} by ${dimension}.`,
      `Show weighted bubble profile for ${dimension} and ${metric}.`
    ]
  };

  let pool = options;
  if (chartType !== 'all') {
    pool = options.filter(item => item.chart_type === chartType);
    if (!pool.length && chartSpecificTemplates[chartType]) {
      pool = chartSpecificTemplates[chartType].map(question => ({
        question,
        chart_type: chartType
      }));
    }

    if (pool.length > 0 && pool.length < count) {
      const seeds = [
        `Show top ${dimension} by ${metric}.`,
        `Compare average ${metric} by ${dimension}.`,
        `Show distribution of ${metric} across ${dimension}.`,
        `Show ranked ${dimension} contribution to ${metric}.`,
        `Show count of records by ${dimension}.`,
        `Highlight highest and lowest ${metric} by ${dimension}.`,
        `Show segment performance for ${dimension} using ${metric}.`,
        `Show variance of ${metric} grouped by ${dimension}.`,
        `Show benchmark view of ${metric} by ${dimension}.`,
        `Show summary comparison for ${dimension} against ${metric}.`
      ];
      const existing = new Set(pool.map(item => item.question.toLowerCase()));
      for (const question of seeds) {
        if (pool.length >= count) break;
        if (existing.has(question.toLowerCase())) continue;
        pool.push({ question, chart_type: chartType });
        existing.add(question.toLowerCase());
      }
    }
  }

  if (!pool.length) {
    pool = chartType === 'all'
      ? options
      : Array.from({ length: count }, (_v, index) => ({
        question: `Show demo insight ${index + 1} for ${metric} by ${dimension}.`,
        chart_type: chartType
      }));
  }

  const normalized = [];
  while (normalized.length < count) {
    normalized.push(pool[normalized.length % pool.length]);
  }
  return normalized.slice(0, count);
}

function buildEmergencyDemoQueries(schema, count = 5, difficulty = 'standard') {
  const demoDifficulty = normalizeDemoDifficulty(difficulty);
  const metric = pickPreferredMetric(schema);
  const dimension = pickPreferredDimension(schema);
  const dateCol = (Array.isArray(schema?.columns) ? schema.columns : []).find(column => column.type === 'DATE')?.name
    || (Array.isArray(schema?.columns) ? schema.columns : []).find(column => String(column.name || '').toLowerCase().includes('date'))?.name
    || null;

  const base = demoDifficulty === 'simple'
    ? [
        { question: `Show total ${metric} by ${dimension}.`, chart_type: 'bar_chart' },
        { question: `Show ${metric} contribution by ${dimension} as a donut chart.`, chart_type: 'donut_chart' },
        { question: `Show ${metric} share by ${dimension}.`, chart_type: 'pie_chart' },
        { question: `Show count of records grouped by ${dimension}.`, chart_type: 'bar_chart' },
        { question: `List ${dimension} with ${metric} in a table.`, chart_type: 'table' }
      ]
    : [
        { question: `Which ${dimension} generated the highest total ${metric}?`, chart_type: 'horizontal_bar_chart' },
        { question: `What is the ${metric} breakdown across all ${dimension} categories?`, chart_type: 'bar_chart' },
        { question: `Compare average ${metric} across ${dimension} groups.`, chart_type: 'bar_chart' },
        { question: `Show count of records grouped by ${dimension}.`, chart_type: 'bar_chart' },
        { question: `List all ${dimension} with their total ${metric} in a table.`, chart_type: 'table' }
      ];

  if (dateCol) {
    base.unshift({ question: demoDifficulty === 'simple' ? `Show the ${metric} trend over ${dateCol}.` : `What is the monthly ${metric} trend over time?`, chart_type: 'line_chart' });
  }

  const unique = [];
  const seen = new Set();
  for (const item of base) {
    const key = String(item.question || '').toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    unique.push(item);
  }

  return unique.slice(0, Math.max(1, count));
}

function questionFingerprint(question) {
  return String(question || '')
    .toLowerCase()
    .replace(/demo\s*insight\s*\d+/g, 'demo_insight')
    .replace(/sample\s*insight\s*\d+/g, 'sample_insight')
    .replace(/\d+/g, '')
    .replace(/[^a-z\s]/g, ' ')
    .replace(/\b(show|list|please|the|a|an|for|of|to|as|chart|graph|view)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function buildDiverseQuestionSeeds(schema, chartType = 'all') {
  const columns = Array.isArray(schema?.columns) && schema.columns.length
    ? schema.columns
    : [
      { name: 'value', type: 'NUMBER' },
      { name: 'category', type: 'TEXT' }
    ];
  const metric = pickPreferredMetric(schema);
  const dimension = pickPreferredDimension(schema);
  const dateCol = columns.find(column => column.type === 'DATE')?.name || columns.find(column => column.name.toLowerCase().includes('date'))?.name || null;

  const base = [
    `Compare total ${metric} by ${dimension}.`,
    `Show top ${dimension} by ${metric}.`,
    `Show bottom ${dimension} by ${metric}.`,
    `Compare average ${metric} across ${dimension}.`,
    `Show count of records by ${dimension}.`,
    `Highlight variance in ${metric} grouped by ${dimension}.`,
    `Compare median-like spread of ${metric} by ${dimension}.`,
    `Show rank order of ${dimension} using ${metric}.`,
    `Show share contribution of ${dimension} to ${metric}.`,
    `Show distribution pattern for ${metric} by ${dimension}.`,
    `Show high-performing ${dimension} segments by ${metric}.`,
    `Show low-performing ${dimension} segments by ${metric}.`
  ];

  if (dateCol) {
    base.unshift(`Show trend of ${metric} over ${dateCol}.`);
    base.unshift(`Compare month-over-month ${metric} using ${dateCol}.`);
    base.unshift(`Show cumulative ${metric} progression by ${dateCol}.`);
  }

  const chartTypeSeeds = {
    line_chart: [
      `Show line trend of ${metric} by ${dimension}.`,
      `Show line pattern for average ${metric} across ${dimension}.`,
      `Show line comparison of top ${dimension} categories.`
    ],
    area_chart: [
      `Show area trend of ${metric} by ${dimension}.`,
      `Show filled area view of average ${metric} across ${dimension}.`,
      `Show cumulative area profile for ${metric} grouped by ${dimension}.`
    ],
    step_line_chart: [
      `Show stepped trend of ${metric} by ${dimension}.`,
      `Show step transitions for ${metric} across ${dimension}.`,
      `Show step comparison of count by ${dimension}.`
    ],
    scatter_plot: [
      `Plot scatter of numeric behavior grouped by ${dimension}.`,
      `Show scatter relation for ${metric} and another metric by ${dimension}.`,
      `Show scatter outlier map for ${metric}.`
    ],
    bubble_chart: [
      `Show bubble chart for ${dimension} weighted by ${metric}.`,
      `Compare ${dimension} with bubble sizes from ${metric}.`,
      `Show bubble outlier comparison for ${metric} by ${dimension}.`
    ],
    pie_chart: [
      `Show pie share of ${metric} by ${dimension}.`,
      `Show composition of ${metric} across ${dimension}.`,
      `Show proportional split of ${metric} by ${dimension}.`
    ],
    donut_chart: [
      `Show donut share of ${metric} by ${dimension}.`,
      `Show donut composition of ${metric} by ${dimension}.`,
      `Compare ring-share for ${metric} across ${dimension}.`
    ],
    radar_chart: [
      `Show radar comparison of ${dimension} by ${metric}.`,
      `Show radar profile for ${metric} across ${dimension}.`,
      `Show multi-axis radar for top ${dimension} segments.`
    ],
    horizontal_bar_chart: [
      `Show horizontal ranking of ${dimension} by ${metric}.`,
      `Compare top ${dimension} with horizontal bars for ${metric}.`,
      `Show side-by-side horizontal values of ${metric} by ${dimension}.`
    ],
    bar_chart: [
      `Show bar comparison of ${metric} by ${dimension}.`,
      `Show grouped bar style totals of ${metric} by ${dimension}.`,
      `Show bar ranking for ${dimension} using ${metric}.`
    ],
    lollipop_chart: [
      `Show lollipop comparison of ${metric} by ${dimension}.`,
      `Show lollipop rank view for ${dimension} by ${metric}.`,
      `Show lollipop spread of ${metric} across ${dimension}.`
    ],
    ranked_table: [
      `Show ranked table of ${dimension} by ${metric}.`,
      `Show top and bottom ${dimension} in ranked table format.`,
      `Show score-ranked ${dimension} list using ${metric}.`
    ],
    table: [
      `Show detailed table of ${dimension} and ${metric}.`,
      `List sample rows with ${dimension} and ${metric}.`,
      `Show tabular comparison of ${dimension} values.`
    ]
  };

  if (chartType !== 'all' && chartTypeSeeds[chartType]) {
    return [...chartTypeSeeds[chartType], ...base];
  }

  return [
    ...base,
    ...Object.keys(chartTypeSeeds).flatMap(type => chartTypeSeeds[type].slice(0, 1))
  ];
}

function diversifyDemoQueries(queries, schema, count = DEMO_QUERY_COUNT, chartType = 'all') {
  const normalized = [];
  const seen = new Set();
  const columns = Array.isArray(schema?.columns) && schema.columns.length
    ? schema.columns
    : [
      { name: 'value', type: 'NUMBER' },
      { name: 'category', type: 'TEXT' }
    ];

  for (const item of queries) {
    const question = (item.question || '').toString().trim();
    if (!question) {
      continue;
    }
    const key = questionFingerprint(question);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    normalized.push({
      question,
      chart_type: CHART_TYPES.includes(item.chart_type) ? item.chart_type : null
    });
  }

  const seeds = buildDiverseQuestionSeeds(schema, chartType);
  for (const question of seeds) {
    if (normalized.length >= count) break;
    const key = questionFingerprint(question);
    if (seen.has(key)) continue;
    seen.add(key);
    normalized.push({
      question,
      chart_type: chartType !== 'all' ? chartType : null
    });
  }

  let autoIndex = 1;
  while (normalized.length < count) {
    const question = `Compare scenario ${autoIndex} of ${columns[0]?.name || 'value'} across ${columns[1]?.name || 'category'}.`;
    autoIndex += 1;
    const key = questionFingerprint(question);
    if (seen.has(key)) continue;
    seen.add(key);
    normalized.push({ question, chart_type: chartType !== 'all' ? chartType : null });
  }

  return normalized.slice(0, count).map((item, index, list) => {
    let chart = chartType !== 'all'
      ? chartType
      : (item.chart_type || CHART_TYPES[index % CHART_TYPES.length]);
    if (chartType === 'all' && index > 0 && chart === list[index - 1]?.chart_type) {
      chart = CHART_TYPES[(index + 1) % CHART_TYPES.length];
    }
    return {
      question: item.question,
      chart_type: chart
    };
  });
}

async function generateDemoQueries(schema, count = DEMO_QUERY_COUNT, chartType = 'all', difficulty = 'standard') {
  const demoDifficulty = normalizeDemoDifficulty(difficulty);
  if (!isAiEnabled()) {
    return fallbackDemoQueries(schema, count, chartType, demoDifficulty);
  }

  const schemaBlock = schema.columns
    .map(column => `- ${column.name} (${column.type})`)
    .join('\n');

  const prompt = [
    'Create demo business questions for a conversational BI dashboard.',
    `Generate exactly ${count} questions for this table schema.`,
    `Difficulty level: ${demoDifficulty}.`,
    chartType === 'all'
      ? `Use mixed chart types across this list: ${CHART_TYPES.join(', ')}.`
      : `Use only this chart_type for every question: ${chartType}.`,
    'Return JSON only in this format:',
    `{"queries":[{"question":"...","chart_type":"${CHART_TYPES.join('|')}"}]}`,
    'Constraints:',
    '- Questions must be realistic and directly answerable with this single table.',
    '- Use varied business intents (trend, comparison, distribution, ranking, listing).',
    demoDifficulty === 'simple'
      ? '- Keep queries easy: one clear metric, one clear dimension, minimal ambiguity.'
      : (demoDifficulty === 'complex'
        ? '- Prefer richer questions using ranking, comparison, multiple metrics, or relationship analysis when valid.'
        : '- Keep a balanced mix of straightforward and moderately analytical questions.'),
    chartType === 'all'
      ? '- Ensure chart_type is distributed across multiple types from the allowed list.'
      : `- Every query must return chart_type exactly as ${chartType}.`,
    '- Do not include markdown.',
    `Table: ${schema.tableName}`,
    'Schema:',
    schemaBlock
  ].join('\n');

  try {
    const raw = await callGemini(prompt);
    const parsed = extractJson(raw);
    const queries = Array.isArray(parsed.queries) ? parsed.queries : [];
    if (!queries.length) {
      return fallbackDemoQueries(schema, count, chartType, demoDifficulty);
    }
    return diversifyDemoQueries(queries, schema, count, chartType);
  } catch (_error) {
    return fallbackDemoQueries(schema, count, chartType, demoDifficulty);
  }
}

function fallbackFollowUpDemoQueries(schema, parentContext, count = 6, chartType = 'all') {
  const parentColumns = Array.isArray(parentContext.columns) ? parentContext.columns : [];
  const schemaByName = new Map(schema.columns.map(column => [column.name, column.type]));
  const numeric = parentColumns.filter(column => schemaByName.get(column) === 'NUMBER');
  const dateLike = parentColumns.filter(column => schemaByName.get(column) === 'DATE' || /date|month|year|time/i.test(column));
  const textLike = parentColumns.filter(column => !numeric.includes(column) && !dateLike.includes(column));
  const metric = pickPreferredMetric(schema, numeric);
  const dimension = pickPreferredDimension(schema, textLike);
  const timeKey = dateLike[0] || schema.columns.find(column => column.type === 'DATE')?.name || null;

  const base = [
    { question: `Show top 5 ${dimension} from this result.`, chart_type: 'horizontal_bar_chart' },
    { question: `Filter this to only highest ${metric} records.`, chart_type: 'table' },
    { question: `Compare ${metric} share by ${dimension}.`, chart_type: 'donut_chart' },
    { question: `Show ranked table by ${metric}.`, chart_type: 'ranked_table' },
    { question: `Break this down with a bar chart for ${dimension}.`, chart_type: 'bar_chart' },
    { question: `Show only rows where ${metric} is above average.`, chart_type: 'table' }
  ];

  if (timeKey) {
    base.unshift({ question: `Show trend of ${metric} over ${timeKey} for this result.`, chart_type: 'line_chart' });
    base.unshift({ question: `Show area trend of ${metric} by ${timeKey}.`, chart_type: 'area_chart' });
  }

  const deduped = [];
  const seen = new Set();
  for (const item of base) {
    const key = item.question.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(item);
  }

  while (deduped.length < count) {
    deduped.push({
      question: `Drill down insight ${deduped.length + 1} for this result.`,
      chart_type: 'table'
    });
  }

  return diversifyDemoQueries(deduped, schema, count, chartType);
}

async function generateFollowUpDemoQueries(schema, parentContext, count = 6, chartType = 'all') {
  if (!isAiEnabled()) {
    return fallbackFollowUpDemoQueries(schema, parentContext, count, chartType);
  }

  const schemaBlock = schema.columns.map(column => `- ${column.name} (${column.type})`).join('\n');
  const parentSql = String(parentContext.sql || '');
  const parentColumns = Array.isArray(parentContext.columns) ? parentContext.columns : [];
  const sampleRows = Array.isArray(parentContext.sample) ? parentContext.sample.slice(0, 3) : [];

  const prompt = [
    'Create follow-up questions for an existing BI query result.',
    `Generate exactly ${count} follow-up questions that refine, filter, drill-down, rank, compare, or trend the current result.`,
    `Allowed chart types: ${CHART_TYPES.join(', ')}.`,
    chartType === 'all'
      ? 'Use mixed chart types across the list based on best fit.'
      : `Use only this chart_type for every question: ${chartType}.`,
    'Return JSON only in this format:',
    `{"queries":[{"question":"...","chart_type":"${CHART_TYPES.join('|')}"}]}`,
    'Constraints:',
    '- Must be follow-up style (context-dependent), not generic first-time questions.',
    '- Questions should be answerable from the same table.',
    chartType === 'all'
      ? '- Include mixed chart types where appropriate.'
      : `- Every query must return chart_type exactly as ${chartType}.`,
    `Current SQL: ${parentSql}`,
    `Current result columns: ${parentColumns.join(', ') || 'unknown'}`,
    `Current result sample rows: ${JSON.stringify(sampleRows)}`,
    `Table: ${schema.tableName}`,
    'Schema:',
    schemaBlock
  ].join('\n');

  try {
    const raw = await callGemini(prompt);
    const parsed = extractJson(raw);
    const queries = Array.isArray(parsed.queries) ? parsed.queries : [];
    if (!queries.length) {
      return fallbackFollowUpDemoQueries(schema, parentContext, count, chartType);
    }
    return diversifyDemoQueries(queries, schema, count, chartType);
  } catch (_error) {
    return fallbackFollowUpDemoQueries(schema, parentContext, count, chartType);
  }
}

async function validateDemoQueries(fileName, queries, count, chartType = 'all') {
  const list = Array.isArray(queries) ? queries : [];
  const validated = [];
  const seen = new Set();
  const maxAttempts = Math.min(40, Math.max(count * 3, 12));
  let attempts = 0;

  for (const item of list) {
    if (validated.length >= count) break;
    if (attempts >= maxAttempts) break;
    const question = String(item?.question || '').trim();
    if (!question) continue;
    const key = question.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    attempts += 1;

    try {
      const result = await runPipeline(
        fileName,
        question,
        `demo_main_validation_${Date.now()}_${attempts}`
      );
      const resultRows = Array.isArray(result?.result_rows) ? result.result_rows : [];
      if (!resultRows.length) {
        continue;
      }
      const compatibleCharts = Array.isArray(result?.all_chart_options) ? result.all_chart_options : [];
      const executedChart = CHART_TYPES.includes(result?.chart_type) ? result.chart_type : null;
      const compatibilityPool = [...new Set([
        ...compatibleCharts.filter(chart => CHART_TYPES.includes(chart)),
        ...(executedChart ? [executedChart] : []),
        'table'
      ])];

      if (chartType === 'all') {
        const requestedChart = CHART_TYPES.includes(item?.chart_type) ? item.chart_type : null;
        const selectedChart = (requestedChart && compatibilityPool.includes(requestedChart))
          ? requestedChart
          : (executedChart || compatibilityPool[0] || 'table');
        validated.push({ question, chart_type: selectedChart });
      } else {
        if (!compatibilityPool.includes(chartType)) {
          continue;
        }
        validated.push({ question, chart_type: chartType });
      }
    } catch (_error) {
      // Skip queries that fail to execute.
    }
  }

  return validated;
}

async function validateFollowUpDemoQueries(fileName, parentContext, queries, count, chartType = 'all') {
  const list = Array.isArray(queries) ? queries : [];
  const validated = [];
  const seen = new Set();
  const maxAttempts = Math.min(36, Math.max(count * 3, 12));
  let attempts = 0;

  for (const item of list) {
    if (validated.length >= count) break;
    if (attempts >= maxAttempts) break;
    const question = String(item?.question || '').trim();
    if (!question) continue;
    const key = question.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    attempts += 1;

    try {
      const result = await runPipeline(
        fileName,
        question,
        `demo_validation_${Date.now()}_${attempts}`,
        parentContext
      );
      const resultRows = Array.isArray(result?.result_rows) ? result.result_rows : [];
      if (!resultRows.length) {
        continue;
      }
      const compatibleCharts = Array.isArray(result?.all_chart_options) ? result.all_chart_options : [];
      const executedChart = CHART_TYPES.includes(result?.chart_type) ? result.chart_type : null;
      const compatibilityPool = [...new Set([
        ...compatibleCharts.filter(chart => CHART_TYPES.includes(chart)),
        ...(executedChart ? [executedChart] : []),
        'table'
      ])];

      if (chartType === 'all') {
        const requestedChart = CHART_TYPES.includes(item?.chart_type) ? item.chart_type : null;
        const selectedChart = (requestedChart && compatibilityPool.includes(requestedChart))
          ? requestedChart
          : (executedChart || compatibilityPool[0] || 'table');
        validated.push({
          question,
          chart_type: selectedChart
        });
      } else {
        if (!compatibilityPool.includes(chartType)) {
          continue;
        }
        validated.push({
          question,
          chart_type: chartType
        });
      }
    } catch (_error) {
      // Skip suggestions that cannot be executed safely.
    }
  }

  return validated;
}

async function explainSql(sqlQuery, question) {
  if (!isAiEnabled()) {
    return explainSqlLocally(sqlQuery);
  }

  const prompt = [
    'Explain this SQL in 1-2 simple sentences for a business user.',
    'No markdown and no bullet points.',
    `Question: ${question || 'N/A'}`,
    `SQL: ${sqlQuery}`
  ].join('\n');

  try {
    const text = await callGemini(prompt);
    const explanation = text.replace(/```/g, '').trim();
    return explanation || explainSqlLocally(sqlQuery);
  } catch (_error) {
    return explainSqlLocally(sqlQuery);
  }
}

function parseMultipartCsv(buffer, contentTypeHeader) {
  const boundaryMatch = contentTypeHeader.match(/boundary=([^;]+)/i);
  if (!boundaryMatch) {
    throw new Error('Invalid upload: missing multipart boundary.');
  }

  const boundary = boundaryMatch[1].trim();
  const body = buffer.toString('binary');
  const parts = body.split(`--${boundary}`).slice(1, -1);
  const fields = {};
  let parsedFile = null;

  for (const part of parts) {
    const trimmed = part.replace(/^\r\n/, '').replace(/\r\n$/, '');
    const separator = trimmed.indexOf('\r\n\r\n');
    if (separator === -1) {
      continue;
    }

    const rawHeaders = trimmed.slice(0, separator);
    const rawContent = trimmed.slice(separator + 4).replace(/\r\n$/, '');
    const disposition = rawHeaders.match(/content-disposition:\s*form-data;\s*name="([^"]+)"(?:;\s*filename="([^"]+)")?/i);
    if (!disposition) {
      continue;
    }

    const fieldName = disposition[1];
    const originalName = disposition[2] || '';
    if (fieldName !== 'file') {
      fields[fieldName] = Buffer.from(rawContent, 'binary').toString('utf8').trim();
      continue;
    }

    const safeName = path.basename(originalName).replace(/[^a-zA-Z0-9._-]/g, '_');
    if (!safeName.toLowerCase().endsWith('.csv')) {
      throw new Error('Only CSV files are accepted.');
    }

    parsedFile = {
      fileName: safeName,
      content: Buffer.from(rawContent, 'binary').toString('utf8'),
      fields
    };
  }

  if (parsedFile) {
    return parsedFile;
  }

  throw new Error('No file field found in upload payload.');
}

function preValidateQuery(question, schema) {
  const lower = String(question || '').toLowerCase().trim();
  const intelligence = getSchemaIntelligence(schema);
  const categoricalValueTokens = new Set();
  Object.values(intelligence?.categorical_value_map || {}).forEach(values => {
    (Array.isArray(values) ? values : []).forEach(rawValue => {
      const tokens = String(rawValue || '').toLowerCase().match(/[a-z0-9_]+/g) || [];
      tokens.forEach(token => {
        if (token.length >= 3) {
          categoricalValueTokens.add(token);
        }
      });
    });
  });

  // Reject completely empty or trivially short questions
  if (lower.length < 3) {
    return { ok: false, type: 'empty', message: 'Please type a question about your data.' };
  }

  // Extract meaningful words (strip stop words)
  const stopWords = new Set(['show', 'me', 'give', 'get', 'what', 'is', 'are', 'the', 'a', 'an', 'of', 'for',
    'in', 'on', 'at', 'to', 'by', 'and', 'or', 'with', 'from', 'all', 'how', 'many', 'much',
    'does', 'do', 'can', 'could', 'would', 'will', 'please', 'list', 'display', 'find', 'total',
    'number', 'count', 'sum', 'average', 'avg', 'top', 'bottom', 'highest', 'lowest', 'best', 'worst']);

  const queryWords = lower.match(/[a-z0-9]+/g) || [];
  const meaningfulWords = queryWords.filter(w => !stopWords.has(w) && w.length > 2);

  if (meaningfulWords.length === 0) {
    const available = schema.columns.map(c => c.name).join(', ');
    return {
      ok: false,
      type: 'no_meaningful_keywords',
      message: `Your question doesn't contain any recognizable data terms.\n\nAvailable columns: ${available}`
    };
  }

  // Check if at least one meaningful word maps to a known column (exact or partial)
  const columnNames = schema.columns.map(c => c.name.toLowerCase().replace(/_/g, ' '));
  const columnNamesRaw = schema.columns.map(c => c.name.toLowerCase());

  const hasColumnMatch = meaningfulWords.some(word =>
    columnNamesRaw.some(col => col.includes(word) || word.includes(col)) ||
    columnNames.some(col => col.includes(word) || word.includes(col)) ||
    COLUMN_SYNONYMS.some(([syn]) => syn.includes(word) || word.includes(syn)) ||
    Object.keys(METRIC_LANGUAGE_MAP).some(key => key.includes(word) || word.includes(key))
  );

  if (!hasColumnMatch) {
    return {
      ok: false,
      type: 'no_column_match',
      message: 'Invalid Query.\nThe requested field does not exist in this dataset.'
    };
  }

  const requestedFieldPhrases = [];
  const normalized = lower.replace(/[?]/g, ' ').replace(/\s+/g, ' ').trim();
  const prefixes = ['show ', 'compare ', 'analyze ', 'list ', 'get ', 'display ', 'find '];
  for (const prefix of prefixes) {
    if (normalized.startsWith(prefix)) {
      const body = normalized.slice(prefix.length);
      const beforeBy = body.split(/\b(?:grouped\s+by|for\s+each|per|across|by)\b/)[0].trim();
      if (beforeBy) {
        beforeBy
          .split(/\b(?:and|vs|versus|along with|,|with)\b/)
          .map(item => item.trim())
          .filter(Boolean)
          .forEach(item => requestedFieldPhrases.push(item));
      }
      break;
    }
  }

  const unresolved = requestedFieldPhrases.filter(phrase => {
    const cleaned = phrase.replace(/\b(total|average|avg|sum|count|maximum|max|minimum|min|of|the)\b/g, '').trim();
    if (!cleaned || cleaned.length < 3) {
      return false;
    }

    // Ignore connector fragments that are not field requests.
    if (/^(compare|comparison|compare it|it|other|others|with others)$/i.test(cleaned)) {
      return false;
    }

    const normalizedColumns = schema.columns.map(c => c.name.toLowerCase());
    const explicitColumnLike = /[_]/.test(cleaned) || /^column\s+[a-z0-9_]+$/i.test(cleaned);
    if (explicitColumnLike) {
      const direct = cleaned.toLowerCase().replace(/^column\s+/, '').trim();
      const existsDirect = normalizedColumns.includes(direct);
      const existsSpaceNormalized = normalizedColumns
        .map(name => name.replace(/_/g, ' '))
        .includes(direct.replace(/_/g, ' '));
      if (!existsDirect && !existsSpaceNormalized) {
        return true;
      }
    }

    const metricMatch = findRequestedMetrics(cleaned, schema, false);
    const dimensionMatch = resolveColumnByWord(cleaned, schema, false);
    if (metricMatch.length === 0 && !dimensionMatch) {
      return true;
    }

    // Token-level guard: if phrase contains unknown business terms, treat as invalid field request.
    const tokens = cleaned.match(/[a-z0-9_]+/g) || [];
    const ignore = new Set([
      'campaign', 'campaigns', 'channel', 'channels', 'type', 'types', 'segment', 'segments',
      'trend', 'trends', 'over', 'time', 'monthly', 'yearly', 'daily',
      'each', 'grouped', 'group', 'per',
      'top', 'bottom', 'rank', 'ranking', 'highest', 'lowest', 'show',
      'correlation', 'relationship', 'relation', 'between', 'affect', 'affects', 'impact', 'impacts',
      'compare', 'comparison', 'across', 'other', 'different', 'with'
    ]);
    const hasUnknownToken = tokens.some(token => {
      if (token.length < 4 || ignore.has(token)) {
        return false;
      }
      if (categoricalValueTokens.has(token)) {
        return false;
      }
      const tokenMetricMatch = findRequestedMetrics(token, schema, false).length > 0;
      const tokenDimensionMatch = !!resolveColumnByWord(token, schema, false);
      const synonymMatch = COLUMN_SYNONYMS.some(([syn]) => syn.includes(token) || token.includes(syn));
      const metricAliasMatch = Object.keys(METRIC_LANGUAGE_MAP).some(key => key.includes(token) || token.includes(key));
      return !tokenMetricMatch && !tokenDimensionMatch && !synonymMatch && !metricAliasMatch;
    });
    return hasUnknownToken;
  });

  if (unresolved.length > 0) {
    return {
      ok: false,
      type: 'unknown_requested_field',
      message: 'Invalid Query.\nThe requested field does not exist in this dataset.'
    };
  }

  const ambiguousSignals = ['performance', 'performing', 'overview', 'summary', 'health', 'status', 'quality'];
  const hasAmbiguousSignal = ambiguousSignals.some(token => lower.includes(token));
  const hasByClause = hasGroupingDimensionLanguage(question);
  const hasComparisonOperator = /\b(compare|vs|versus|along with|and)\b/.test(lower);
  const explicitDimensionMention = !!findRequestedDimension(question, schema);
  const numericColumns = schema.columns.filter(c => c.type === 'NUMBER').map(c => c.name);
  const metricMentioned = numericColumns.some(col => {
    const name = col.toLowerCase();
    return lower.includes(name) || lower.includes(name.replace(/_/g, ' '));
  });

  if (hasAmbiguousSignal && !metricMentioned && !hasByClause && !hasComparisonOperator && !explicitDimensionMention) {
    const dimensionCandidate = schema.columns.find(c => c.type === 'TEXT' && /campaign|channel|segment|type|category/i.test(c.name))
      || schema.columns.find(c => c.type === 'TEXT')
      || null;
    const preferredMetricOrder = ['Revenue', 'Conversions', 'ROI', 'Clicks', 'Leads'];
    const chosenMetrics = preferredMetricOrder
      .filter(name => numericColumns.includes(name))
      .concat(numericColumns.filter(name => !preferredMetricOrder.includes(name)))
      .slice(0, 3);

    const suggestions = chosenMetrics.map(metric => `- ${metric} by ${dimensionCandidate ? dimensionCandidate.name : 'category'}`);
    return {
      ok: false,
      type: 'ambiguous_query',
      message: `Did you mean:\n${suggestions.join('\n')}`
    };
  }

  return { ok: true };
}

async function runPipeline(fileName, question, sessionId, parentContext = null, context = null) {
  const validationTrace = [];
  const pushTrace = (stage, message, status = 'ok') => {
    validationTrace.push({
      stage,
      message,
      status,
      timestamp: new Date().toISOString()
    });
  };

  const dataset = await loadDataset(fileName, context || null);
  pushTrace('query_extracted', 'User question accepted and dataset loaded.');

  // Pre-validation disabled - SQL validator will catch invalid columns
  // Proceed directly to intent extraction
  pushTrace('pre_validation', 'Skipped (using post-generation SQL validation).');

  const intent = intent_extractor(question, dataset.schema);
  // Allow questions without explicit metrics - query planner will handle defaults
  pushTrace('intent_extractor', `Intent extracted: ${JSON.stringify(intent)}`);
  let queryPlan = query_planner(intent, dataset.schema, question);
  const selectedColumns = selectRelevantColumns(question, dataset.schema);
  pushTrace('intent_extractor', `Intent extracted: ${JSON.stringify(intent)}`);
  pushTrace('query_planner', `Query plan created: ${JSON.stringify(queryPlan)}`);

  let generated = {
    ...sql_generator(queryPlan, dataset.schema),
    chart_type: chartHint(question, intent.analysis_type),
    mode: 'structured_pipeline'
  };
  pushTrace('sql_generator', `SQL generated from plan: ${generated.sql_query}`);

  let validation = sql_validator(generated, dataset.schema, question, queryPlan);
  pushTrace(
    'sql_validator',
    validation.ok ? 'SQL passed schema, safety, and coverage validation.' : `SQL validation failed: ${validation.error}`,
    validation.ok ? 'ok' : 'error'
  );

  let execution = null;
  let repaired = false;

  const attemptExecution = () => {
    if (!validation.ok) {
      return;
    }
    try {
      execution = executeSql(generated.sql_query, dataset);
      pushTrace('query_execution', `SQL executed successfully with ${execution.rows.length} rows.`);
    } catch (error) {
      validation = { ok: false, error: `Execution failed: ${error.message}` };
      pushTrace('query_execution', validation.error, 'error');
    }
  };

  attemptExecution();

  let repairAttempt = 0;
  while (!validation.ok && repairAttempt < MAX_SQL_REPAIR_ATTEMPTS && !execution) {
    repairAttempt += 1;
    repaired = true;
    pushTrace('sql_validator', `Regenerating SQL from repaired plan (attempt ${repairAttempt}/${MAX_SQL_REPAIR_ATTEMPTS}).`);

    queryPlan = repair_query_plan(queryPlan, dataset.schema, question);
    pushTrace('query_planner', `Repaired plan: ${JSON.stringify(queryPlan)}`);

    generated = {
      ...sql_generator(queryPlan, dataset.schema),
      chart_type: chartHint(question, intent.analysis_type),
      mode: 'structured_pipeline_repair'
    };
    pushTrace('sql_generator', `Regenerated SQL: ${generated.sql_query}`);

    validation = sql_validator(generated, dataset.schema, question, queryPlan);
    pushTrace(
      'sql_validator',
      validation.ok ? `Regenerated SQL passed validation on attempt ${repairAttempt}.` : `Regenerated SQL failed on attempt ${repairAttempt}: ${validation.error}`,
      validation.ok ? 'ok' : 'error'
    );

    attemptExecution();
  }

  if (!execution) {
    pushTrace('fallback_sql_generator', `Primary pipeline failed: ${validation.error || 'unknown error'}. Falling back to deterministic SQL.`, 'warning');
    generated = {
      ...buildDemoSql(question, dataset.schema),
      mode: 'deterministic_fallback'
    };
    pushTrace('fallback_sql_generator', `Fallback SQL generated: ${generated.sql_query}`);
    validation = sql_validator(generated, dataset.schema, question, queryPlan);
    pushTrace(
      'sql_validator',
      validation.ok ? 'Fallback SQL passed validation.' : `Fallback SQL failed validation: ${validation.error}`,
      validation.ok ? 'ok' : 'error'
    );
    if (validation.ok) {
      try {
        execution = executeSql(generated.sql_query, dataset);
        repaired = true;
        pushTrace('query_execution', `Fallback SQL executed successfully with ${execution.rows.length} rows.`);
      } catch (error) {
        pushTrace('query_execution', `Fallback execution failed: ${error.message}`, 'error');
      }
    }
  }

  if (!execution) {
    throw new Error(validation.error || 'Failed to execute generated SQL.');
  }

  const answerCoverage = validateAnswerCoverage(question, execution);
  if (!answerCoverage.ok) {
    pushTrace('answer_coverage_check', answerCoverage.message, 'warning');
  } else {
    pushTrace('answer_coverage_check', 'Requested answer count matches generated result.');
  }

  const explanation = await explainSql(generated.sql_query, question);
  const chartData = buildChartData(execution.rows);
  const dataStructure = detectDataStructure(dataset.schema, execution.rows, execution.columns);
  const allChartOptions = listCompatibleChartTypes(dataStructure, execution.rows.length);
  const chartOptions = recommendChartOptions(dataStructure, execution.rows.length);
  const recommendedChart = chartOptions[0] || 'table';
  const requestedChart = generated.chart_type || chartHint(question, intent.analysis_type);
  const finalChart = pickChartForComplexQuestion(question, queryPlan, requestedChart, recommendedChart, allChartOptions);
  pushTrace(
    'chart_generator',
    requestedChart === finalChart
      ? `Chart "${finalChart}" is valid for this result.`
      : `Requested chart "${requestedChart}" adjusted to "${finalChart}" based on result structure.`
  );

  const insight = buildInsight(execution.rows, dataStructure);
  pushTrace('insight_generator', 'Insight generated from final dataset result.');
  pushTrace('preparing_response', 'Final response payload prepared.');

  const response = {
    file_name: dataset.fileName,
    table_name: dataset.schema.tableName,
    rows_loaded: dataset.rows.length,
    truncated_rows: dataset.truncated,
    schema_intelligence: dataset.schema.intelligence || null,
    intent,
    intent_extractor_output: {
      dimensions: intent.dimensions || [],
      metrics: intent.metrics || [],
      filters: intent.filters || {},
      analysis_type: intent.analysis_type || '',
      aggregation_type: intent.aggregation_type || ''
    },
    selected_columns: selectedColumns,
    sql_query: generated.sql_query,
    sql_generator_output: {
      sql_query: generated.sql_query
    },
    sql_explanation: explanation,
    chart_type: finalChart,
    chart_data: chartData,
    recommended_chart: recommendedChart,
    chart_options: chartOptions,
    all_chart_options: allChartOptions,
    insight,
    data_structure: dataStructure,
    result_columns: execution.columns,
    result_rows: execution.rows,
    validation_status: repaired ? 'repaired_and_executed' : 'executed',
    sql_validator_output: {
      status: 'passed',
      repair_attempts: repairAttempt
    },
    validation_trace: validationTrace,
    validation_error: null,
    mode: generated.mode || 'gemini',
    query_plan: queryPlan,
    query_planner_output: {
      group_by: queryPlan.group_by || [],
      aggregations: queryPlan.aggregations || {},
      filters: queryPlan.filters || []
    },
    sql_repair_attempts: repairAttempt
  };

  pushConversationEntry(sessionId, {
    question,
    sql_query: response.sql_query,
    chart_type: response.chart_type,
    timestamp: new Date().toISOString()
  });

  pushQueryHistoryEntry(sessionId, {
    question,
    sql: response.sql_query,
    explanation: response.sql_explanation,
    result: response.result_rows,
    result_columns: response.result_columns,
    chart: response.chart_type,
    chart_data: response.chart_data,
    insight: response.insight,
    time: new Date().toISOString(),
    file_name: dataset.fileName,
    table_name: dataset.schema.tableName,
    dataset_summary: {
      dataset_name: dataset.fileName,
      rows_analyzed: dataset.rows.length,
      total_columns: dataset.schema.columns.length,
      detected_metrics: (dataset.schema.intelligence?.metric_candidates || []),
      detected_dimensions: (dataset.schema.intelligence?.dimension_candidates || []),
      time_columns: (dataset.schema.intelligence?.date_columns || [])
    }
  });

  return response;
}

const server = http.createServer(async (req, res) => {
  const parsedUrl = new URL(req.url, `http://localhost:${PORT}`);
  const pathname = parsedUrl.pathname;

  if (req.method === 'GET' && pathname === '/') {
    sendFile(res, path.join(__dirname, 'index.html'), 'text/html');
    return;
  }

  if (req.method === 'GET' && pathname === '/style.css') {
    sendFile(res, path.join(__dirname, 'style.css'), 'text/css');
    return;
  }

  if (req.method === 'POST' && pathname === '/api/auth/register') {
    try {
      const body = await collectJsonBody(req);
      const name = (body.name || '').toString().trim();
      const email = (body.email || '').toString().trim().toLowerCase();
      const password = (body.password || '').toString();
      const role = (body.role || 'viewer').toString().trim().toLowerCase() || 'viewer';

      if (!name || !email || !password) {
        throw new Error('name, email and password are required.');
      }
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        throw new Error('Please provide a valid email address.');
      }
      if (password.length < 8) {
        throw new Error('Password must be at least 8 characters long.');
      }

      const users = readUsers();
      if (users.some(user => user.email === email)) {
        throw new Error('Account already exists for this email.');
      }

      const password_hash = await bcrypt.hash(password, 10);
      const user = {
        id: 'usr_' + Math.random().toString(36).slice(2, 10),
        name,
        email,
        password_hash,
        role,
        created_at: new Date().toISOString()
      };

      users.push(user);
      writeUsers(users);

      const token = createAuthToken(user);
      sendJson(res, 201, {
        message: 'Account created successfully.',
        token,
        user: sanitizeUser(user)
      });
    } catch (error) {
      const handled = toApiError(error, 'Could not create account. Please verify details and try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'POST' && pathname === '/api/auth/login') {
    try {
      const body = await collectJsonBody(req);
      const email = (body.email || '').toString().trim().toLowerCase();
      const password = (body.password || '').toString();

      if (!email || !password) {
        throw new Error('email and password are required.');
      }

      const users = readUsers();
      const user = users.find(item => item.email === email);
      if (!user) {
        throw new Error('Authentication required: invalid email or password.');
      }

      const ok = await bcrypt.compare(password, user.password_hash || '');
      if (!ok) {
        throw new Error('Authentication required: invalid email or password.');
      }

      const token = createAuthToken(user);
      sendJson(res, 200, {
        message: 'Login successful.',
        token,
        user: sanitizeUser(user)
      });
    } catch (error) {
      const handled = toApiError(error, 'Could not login. Please try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'GET' && pathname === '/api/files') {
    try {
      const auth = getOptionalAuth(req);
      const sessionId = extractSessionId(req, parsedUrl);
      sendJson(res, 200, { files: getDatasetFiles({ auth, sessionId }) });
    } catch (error) {
      const handled = toApiError(error, 'Could not load datasets. Please try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'GET' && pathname === '/api/schema') {
    try {
      const auth = getOptionalAuth(req);
      const sessionId = extractSessionId(req, parsedUrl);
      const fileName = parsedUrl.searchParams.get('file');
      if (!fileName) {
        sendJson(res, 400, { error: 'file query parameter is required.' });
        return;
      }
      const dataset = await loadDataset(fileName, { auth, sessionId });
      sendJson(res, 200, {
        file_name: dataset.fileName,
        table_name: dataset.schema.tableName,
        columns: dataset.schema.columns,
        schema_intelligence: dataset.schema.intelligence || null,
        preview_rows: dataset.rows.slice(0, 3)
      });
    } catch (error) {
      const handled = toApiError(error, 'Could not read schema. Please verify the dataset and try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'GET' && pathname === '/api/demo-queries') {
    try {
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, private');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('Expires', '0');
      const auth = getOptionalAuth(req);
      const sessionId = extractSessionId(req, parsedUrl);
      const fileName = parsedUrl.searchParams.get('file');
      if (!fileName) {
        sendJson(res, 400, { error: 'file query parameter is required.' });
        return;
      }
      const dataset = await loadDataset(fileName, { auth, sessionId });
      const rawCount = parseInt(parsedUrl.searchParams.get('count') || '8', 10);
      const count = Math.min(20, Math.max(1, Number.isFinite(rawCount) ? rawCount : 8));
      const rawChartType = (parsedUrl.searchParams.get('chart_type') || 'all').trim();
      const rawDifficulty = (parsedUrl.searchParams.get('difficulty') || 'standard').trim().toLowerCase();
      const chartType = rawChartType === 'all' ? 'all' : (CHART_TYPES.includes(rawChartType) ? rawChartType : 'all');
      const difficulty = normalizeDemoDifficulty(rawDifficulty);

      // Generate extra candidates so validation can filter freely.
      const candidates = await generateDemoQueries(dataset.schema, count * 2, chartType, difficulty);
      let queries = await validateDemoQueries(fileName, candidates, count, chartType);

      // If not enough passed validation, run a deterministic fallback generation
      // and validate those too. Never append unvalidated suggestions.
      if (queries.length < count) {
        const needed = count - queries.length;
        const usedKeys = new Set(queries.map(q => q.question.toLowerCase()));
        const fallbackCandidates = fallbackDemoQueries(dataset.schema, Math.max(needed * 3, needed + 4), chartType, difficulty)
          .filter(item => item && item.question && !usedKeys.has(String(item.question).toLowerCase()));

        if (fallbackCandidates.length > 0) {
          const fallbackValidated = await validateDemoQueries(fileName, fallbackCandidates, needed, chartType);
          queries = [...queries, ...fallbackValidated];
        }
      }

      if (!queries.length) {
        queries = buildEmergencyDemoQueries(dataset.schema, Math.min(6, count), difficulty);
      }

      sendJson(res, 200, {
        file_name: dataset.fileName,
        table_name: dataset.schema.tableName,
        difficulty,
        queries
      });
    } catch (error) {
      const handled = toApiError(error, 'Could not generate demo queries right now. Please try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'GET' && pathname === '/api/history') {
    const sessionId = parsedUrl.searchParams.get('sessionId') || 'default';
    sendJson(res, 200, { sessionId, history: getConversation(sessionId) });
    return;
  }

  if ((req.method === 'GET' || req.method === 'POST') && (pathname === '/generate-report' || pathname === '/api/generate-report')) {
    try {
      const auth = getOptionalAuth(req);
      if (req.method === 'POST') {
        const body = await collectJsonBody(req);
        const requestedSessionId = extractSessionId(req, parsedUrl, body.sessionId || '');
        const sessionId = requestedSessionId || (auth && auth.sub ? `user_${auth.sub}` : 'default');
        const reportEntry = buildSingleReportEntry(body);
        writeReportPdf(res, [reportEntry], { sessionId });
        return;
      }

      const requestedSessionId = extractSessionId(req, parsedUrl, parsedUrl.searchParams.get('sessionId') || '');
      const sessionId = requestedSessionId || (auth && auth.sub ? `user_${auth.sub}` : 'default');
      const reportHistory = getQueryHistory(sessionId);

      if (!reportHistory.length) {
        sendJson(res, 404, { error: 'No query history found for this session.' });
        return;
      }

      writeReportPdf(res, reportHistory, { sessionId });
    } catch (error) {
      const handled = toApiError(error, 'Could not generate report. Please try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'POST' && (pathname === '/api/follow-up-demo-queries' || pathname === '/api/v3/follow-up-demo-queries')) {
    try {
      const auth = pathname === '/api/v3/follow-up-demo-queries' ? requireAuth(req) : null;
      const body = await collectJsonBody(req);
      if (!body.fileName) {
        sendJson(res, 400, { error: 'fileName is required.' });
        return;
      }
      if (!body.parentContext || !body.parentContext.sql) {
        sendJson(res, 400, { error: 'parentContext.sql is required for follow-up demo queries.' });
        return;
      }

      const sessionId = extractSessionId(req, parsedUrl, body.sessionId || (auth && auth.sub ? `user_${auth.sub}` : ''));
      const dataset = await loadDataset(body.fileName, { auth, sessionId });
      const rawCount = parseInt(body.count || '6', 10);
      const count = Math.min(12, Math.max(2, Number.isFinite(rawCount) ? rawCount : 6));
      const rawChartType = (body.chart_type || 'all').toString().trim();
      const chartType = rawChartType === 'all' ? 'all' : (CHART_TYPES.includes(rawChartType) ? rawChartType : 'all');
      const parentContext = {
        sql: String(body.parentContext.sql || ''),
        columns: Array.isArray(body.parentContext.columns) ? body.parentContext.columns : [],
        rowCount: Number(body.parentContext.rowCount) || 0,
        sample: Array.isArray(body.parentContext.sample) ? body.parentContext.sample.slice(0, 3) : []
      };

      const generatedQueries = await generateFollowUpDemoQueries(dataset.schema, parentContext, count * 2, chartType);
      let queries = await validateFollowUpDemoQueries(dataset.fileName, parentContext, generatedQueries, count, chartType);
      if (queries.length < count) {
        const fallbackQueries = fallbackFollowUpDemoQueries(dataset.schema, parentContext, count * 2, chartType);
        const fallbackValidated = await validateFollowUpDemoQueries(dataset.fileName, parentContext, fallbackQueries, count - queries.length, chartType);
        queries = [...queries, ...fallbackValidated];
      }

      sendJson(res, 200, {
        file_name: dataset.fileName,
        table_name: dataset.schema.tableName,
        parent_sql: parentContext.sql,
        queries,
        auth_user: auth ? { id: auth.sub, email: auth.email, role: auth.role } : null
      });
    } catch (error) {
      const handled = toApiError(error, 'Could not generate follow-up demo queries right now. Please try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'POST' && (pathname === '/api/upload' || pathname === '/upload')) {
    try {
      const auth = getOptionalAuth(req);
      const sessionId = extractSessionId(req, parsedUrl);
      const uploadedFile = await runUploadMiddleware(req, res);
      if (!uploadedFile) {
        sendJson(res, 400, { error: 'Dataset file is required.' });
        return;
      }

      const registry = cleanupUploadRegistry();
      const filePath = path.join(DATA_DIR, uploadedFile.filename);

      if (auth && auth.sub) {
        registry[uploadedFile.filename] = {
          visibility: 'user',
          ownerUserId: auth.sub,
          createdAt: new Date().toISOString()
        };
        writeUploadRegistry(registry);
      } else {
        if (!sessionId) {
          throw new Error('sessionId is required for guest upload.');
        }
        const fileBuffer = fs.readFileSync(filePath);
        temporaryUploads.set(uploadedFile.filename, {
          contentBase64: fileBuffer.toString('base64'),
          uploadedAt: Date.now(),
          ownerSessionId: sessionId
        });
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath);
        }
      }

      datasetCache.clear();
      const dataset = await loadDataset(uploadedFile.filename, { auth, sessionId });
      sendJson(res, 200, {
        message: 'Dataset uploaded successfully',
        file_name: uploadedFile.filename,
        table_name: dataset.schema.tableName,
        rows: dataset.rows.length,
        columns: dataset.schema.columns.map(column => column.name),
        schema_intelligence: dataset.schema.intelligence || null,
        rows_loaded: dataset.rows.length,
        temporary: !auth
      });
    } catch (error) {
      const handled = toApiError(error, 'Upload failed. Please verify the dataset file and try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'POST' && pathname === '/api/explain') {
    try {
      const body = await collectJsonBody(req);
      if (!body.sqlQuery) {
        sendJson(res, 400, { error: 'sqlQuery is required.' });
        return;
      }
      const explanation = await explainSql(body.sqlQuery, body.question || '');
      sendJson(res, 200, { explanation });
    } catch (error) {
      const handled = toApiError(error, 'Could not generate explanation right now.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'POST' && pathname === '/api/query') {
    try {
      const body = await collectJsonBody(req);
      if (!body.fileName || !body.question) {
        sendJson(res, 400, { error: 'fileName and question are required.' });
        return;
      }
      const sessionId = (body.sessionId || 'default').toString();
      const parentContext = body.parentContext && typeof body.parentContext === 'object' ? body.parentContext : null;
      const context = { auth: null, sessionId };
      ensureDatasetAccessible(body.fileName, context);
      const result = await runPipeline(body.fileName, body.question, sessionId, parentContext, context);
      sendJson(res, 200, result);
    } catch (error) {
      const errorMsg = error && error.message ? String(error.message) : String(error);
      console.error(`[api/query] Error: ${errorMsg}`);
      if (error && error.stack) console.error(error.stack);
      const handled = toApiError(error, 'Could not build your dashboard for this query. Please try again.');
      console.error(`[api/query] Returning status ${handled.statusCode}: ${handled.message}`);
      sendJson(res, handled.statusCode, { error: handled.message, debug_trace: error && error.validationTrace ? error.validationTrace : null });
    }
    return;
  }

  if (req.method === 'POST' && (pathname === '/api/follow-up' || pathname === '/api/v3/follow-up')) {
    try {
      const auth = pathname === '/api/v3/follow-up' ? requireAuth(req) : null;
      const body = await collectJsonBody(req);
      if (!body.fileName || !body.question) {
        sendJson(res, 400, { error: 'fileName and question are required.' });
        return;
      }
      if (!body.parentContext || !body.parentContext.sql) {
        sendJson(res, 400, { error: 'parentContext.sql is required for follow-up queries.' });
        return;
      }
      const sessionId = (body.sessionId || (auth && auth.sub) || 'default').toString();
      const parentContext = {
        sql: String(body.parentContext.sql || ''),
        columns: Array.isArray(body.parentContext.columns) ? body.parentContext.columns : [],
        rowCount: Number(body.parentContext.rowCount) || 0,
        sample: Array.isArray(body.parentContext.sample) ? body.parentContext.sample.slice(0, 3) : []
      };
      const context = { auth, sessionId };
      ensureDatasetAccessible(body.fileName, context);
      const result = await runPipeline(body.fileName, body.question, sessionId, parentContext, context);
      if (auth) {
        result.auth_user = { id: auth.sub, email: auth.email, role: auth.role };
      }
      sendJson(res, 200, result);
    } catch (error) {
      const handled = toApiError(error, 'Could not process follow-up query. Please try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'POST' && pathname === '/api/v3/query') {
    try {
      const auth = requireAuth(req);
      const body = await collectJsonBody(req);
      if (!body.fileName || !body.question) {
        sendJson(res, 400, { error: 'fileName and question are required.' });
        return;
      }
      const sessionId = (body.sessionId || auth.sub || 'default').toString();
      const context = { auth, sessionId };
      ensureDatasetAccessible(body.fileName, context);
      const result = await runPipeline(body.fileName, body.question, sessionId, null, context);
      sendJson(res, 200, {
        ...result,
        auth_user: {
          id: auth.sub,
          email: auth.email,
          role: auth.role
        }
      });
    } catch (error) {
      const handled = toApiError(error, 'Could not process Version 3 query. Please try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'GET' && pathname === '/api/preview') {
    try {
      const auth = getOptionalAuth(req);
      const sessionId = extractSessionId(req, parsedUrl);
      const fileName = parsedUrl.searchParams.get('file') || '';
      if (!fileName) {
        sendJson(res, 400, { error: 'file parameter required.' });
        return;
      }
      const dataset = await loadDataset(fileName, { auth, sessionId });
      const previewRows = dataset.rows.slice(0, 50);
      sendJson(res, 200, { columns: dataset.schema.columns.map(c => c.name), rows: previewRows });
    } catch (error) {
      const handled = toApiError(error, 'Preview is unavailable for this dataset. Please verify and try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'POST' && pathname === '/api/settings') {
    try {
      const body = await collectJsonBody(req);
      const hasApiKeyField = Object.prototype.hasOwnProperty.call(body, 'apiKey');
      const hasDemoModeField = Object.prototype.hasOwnProperty.call(body, 'demoMode');
      const key = hasApiKeyField ? (body.apiKey || '').toString().trim() : '';
      const demoMode = hasDemoModeField ? Boolean(body.demoMode) : adminDemoMode;

      if (!hasApiKeyField && !hasDemoModeField) {
        sendJson(res, 400, { error: 'No settings payload provided.' });
        return;
      }

      const envPath = path.join(__dirname, '.env');
      let envContent = fs.existsSync(envPath) ? fs.readFileSync(envPath, 'utf8') : '';

      if (hasApiKeyField) {
        if (!key || key.length < 10) {
          sendJson(res, 400, { error: 'Invalid API key.' });
          return;
        }
        geminiApiKey = key;
        process.env.GEMINI_API_KEY = key;
        if (/^GEMINI_API_KEY=/m.test(envContent)) {
          envContent = envContent.replace(/^GEMINI_API_KEY=.*/m, `GEMINI_API_KEY=${key}`);
        } else {
          envContent = envContent.trimEnd() + `\nGEMINI_API_KEY=${key}\n`;
        }
      }

      adminDemoMode = demoMode;
      process.env.DEMO_MODE = demoMode ? 'true' : 'false';
      if (/^DEMO_MODE=/m.test(envContent)) {
        envContent = envContent.replace(/^DEMO_MODE=.*/m, `DEMO_MODE=${process.env.DEMO_MODE}`);
      } else {
        envContent = envContent.trimEnd() + `\nDEMO_MODE=${process.env.DEMO_MODE}\n`;
      }

      fs.writeFileSync(envPath, envContent, 'utf8');
      const masked = hasConfiguredApiKey() ? (geminiApiKey.slice(0, 6) + '...' + geminiApiKey.slice(-4)) : null;
      sendJson(res, 200, { ok: true, masked, demoMode: adminDemoMode, configured: hasConfiguredApiKey() });
    } catch (error) {
      const handled = toApiError(error, 'Could not save settings. Please try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
    }
    return;
  }

  if (req.method === 'GET' && pathname === '/api/settings') {
    const hasKey = hasConfiguredApiKey();
    const masked = hasKey ? (geminiApiKey.slice(0, 6) + '...' + geminiApiKey.slice(-4)) : null;
    sendJson(res, 200, { configured: hasKey, masked, demoMode: adminDemoMode });
    return;
  }

  sendJson(res, 404, { error: 'Not found.' });
});

process.on('uncaughtException', error => {
  console.error('[uncaughtException]', error && error.stack ? error.stack : error);
});

process.on('unhandledRejection', reason => {
  console.error('[unhandledRejection]', reason);
});

initSqlJs()
  .then(SQL => {
    SQL_MODULE = SQL;
    migrateLegacyPublicTempDatasets();
    server.listen(PORT, () => {
      console.log(`Compact BI demo running at http://localhost:${PORT}`);
      console.log(`Drop dataset files into: ${DATA_DIR}`);
    });
  })
  .catch(error => {
    console.error('[sql.js init failed]', error && error.stack ? error.stack : error);
    process.exit(1);
  });
