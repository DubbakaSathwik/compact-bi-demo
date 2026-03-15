const http = require('http');
const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

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
const conversations = new Map();
const temporaryUploads = new Map();

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

if (!fs.existsSync(USERS_FILE)) {
  fs.writeFileSync(USERS_FILE, '[]', 'utf8');
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
  if (raw === '') {
    return null;
  }
  if (/^-?\d+(\.\d+)?$/.test(raw)) {
    return Number(raw);
  }
  return raw;
}

function parseCsv(text) {
  const cleaned = unwrapEmbeddedCsv(text);
  const lines = cleaned.split(/\r?\n/).filter(line => line.trim() !== '');
  if (lines.length < 2) {
    throw new Error('CSV file must contain a header row and at least one data row.');
  }

  // Skip noisy wrapper rows and pick the first row that looks like real headers.
  let headerIndex = 0;
  for (let i = 0; i < Math.min(lines.length, 20); i += 1) {
    const lineLower = lines[i].toLowerCase();
    if (lineLower.includes('monthly_income') || (lineLower.includes('age') && lineLower.includes('gender'))) {
      headerIndex = i;
      break;
    }

    const cells = parseCsvLine(lines[i]).map(cell => cell.trim());
    const nonEmpty = cells.filter(Boolean);
    const numericLike = nonEmpty.filter(cell => /^-?\d+(\.\d+)?$/.test(cell)).length;
    const alphaLike = nonEmpty.filter(cell => /[a-zA-Z]/.test(cell)).length;

    if (nonEmpty.length >= 3 && alphaLike >= 2 && numericLike / nonEmpty.length < 0.5) {
      headerIndex = i;
      break;
    }
  }

  let headerCells = parseCsvLine(lines[headerIndex]);
  if (headerCells.length === 1 && headerCells[0].includes(',')) {
    let expanded = headerCells[0].trim();
    if ((expanded.startsWith('"') && expanded.endsWith('"')) || (expanded.startsWith("'") && expanded.endsWith("'"))) {
      expanded = expanded.slice(1, -1);
    }
    headerCells = expanded.split(',').map(cell => cell.trim());
  }

  const headers = headerCells.map((header, index) => {
    const cleanedHeader = header.includes('>') ? header.slice(header.lastIndexOf('>') + 1) : header;
    const sanitized = cleanedHeader.replace(/[^a-zA-Z0-9_]/g, '_').trim();
    return sanitized || `column_${index + 1}`;
  });

  const rows = [];
  for (let index = headerIndex + 1; index < lines.length && rows.length < MAX_ROWS; index += 1) {
    const values = parseCsvLine(lines[index]);
    const row = {};
    headers.forEach((header, columnIndex) => {
      row[header] = normalizeValue(values[columnIndex] || '');
    });
    rows.push(row);
  }

  return {
    headers,
    rows,
    truncated: lines.length - 1 > MAX_ROWS
  };
}

function inferColumnType(values) {
  const filtered = values.filter(value => value !== null && value !== undefined && value !== '');
  if (filtered.length === 0) {
    return 'TEXT';
  }
  const numeric = filtered.every(value => typeof value === 'number');
  if (numeric) {
    return 'NUMBER';
  }
  const dateLike = filtered.every(value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value));
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
      type: inferColumnType(values)
    };
  });

  return {
    tableName: path.basename(fileName, path.extname(fileName)).replace(/[^a-zA-Z0-9_]/g, '_') || 'dataset',
    columns
  };
}

function cleanupTemporaryUploads() {
  const now = Date.now();
  for (const [fileName, entry] of temporaryUploads.entries()) {
    if (!entry || !entry.uploadedAt || now - entry.uploadedAt > TEMP_UPLOAD_TTL_MS) {
      temporaryUploads.delete(fileName);
    }
  }
}

function reserveUploadFileName(originalFileName) {
  const safe = path.basename(originalFileName || 'upload.csv');
  const ext = path.extname(safe).toLowerCase() === '.csv' ? '.csv' : '.csv';
  const base = path.basename(safe, path.extname(safe)).replace(/[^a-zA-Z0-9_\-]/g, '_') || 'upload';

  const diskExists = fs.existsSync(path.join(DATA_DIR, base + ext));
  const tempExists = temporaryUploads.has(base + ext);
  if (!diskExists && !tempExists) {
    return base + ext;
  }

  return base + '_temp_' + Date.now() + ext;
}

function getCsvFiles() {
  cleanupTemporaryUploads();
  const diskFiles = fs.readdirSync(DATA_DIR).filter(fileName => fileName.toLowerCase().endsWith('.csv'));
  const tempFiles = Array.from(temporaryUploads.keys());
  return [...tempFiles, ...diskFiles.filter(fileName => !temporaryUploads.has(fileName))];
}

function loadDataset(fileName) {
  const safeName = path.basename(fileName);
  cleanupTemporaryUploads();

  const temporaryEntry = temporaryUploads.get(safeName);
  if (temporaryEntry && typeof temporaryEntry.content === 'string') {
    const parsedTemp = parseCsv(temporaryEntry.content);
    const schemaTemp = inferSchema(parsedTemp.headers, parsedTemp.rows, safeName);
    return { fileName: safeName, ...parsedTemp, schema: schemaTemp };
  }

  const filePath = path.join(DATA_DIR, safeName);
  if (!fs.existsSync(filePath)) {
    throw new Error('Selected CSV file was not found in the data folder.');
  }
  const text = fs.readFileSync(filePath, 'utf8');
  const parsed = parseCsv(text);
  const schema = inferSchema(parsed.headers, parsed.rows, safeName);
  return { fileName: safeName, ...parsed, schema };
}

function findQuestionMatchedColumn(question, schema, predicate = () => true) {
  const lower = String(question || '').toLowerCase();
  return schema.columns.find(column => predicate(column) && lower.includes(String(column.name || '').toLowerCase())) || null;
}

function findRequestedMetric(question, schema) {
  return findQuestionMatchedColumn(question, schema, column => column.type === 'NUMBER')
    || schema.columns.find(column => ['revenue', 'sales', 'quantity', 'price', 'amount', 'count', 'cost', 'profit', 'income', 'salary', 'spend'].some(token => column.name.toLowerCase().includes(token)))
    || schema.columns.find(column => column.type === 'NUMBER')
    || null;
}

function findRequestedDimension(question, schema) {
  const lower = String(question || '').toLowerCase();
  const byMatch = lower.match(/\bby\s+([a-zA-Z0-9_]+)/);
  if (byMatch) {
    const requested = byMatch[1];
    const exact = schema.columns.find(column => String(column.name || '').toLowerCase() === requested);
    if (exact) {
      return exact;
    }
  }

  return findQuestionMatchedColumn(question, schema, column => true)
    || schema.columns.find(column => column.type === 'TEXT')
    || null;
}

function buildExtremaSql(question, schema) {
  const lower = String(question || '').toLowerCase();
  const table = schema.tableName;
  const metric = findRequestedMetric(question, schema);
  const dimension = findRequestedDimension(question, schema);
  const asksHighest = lower.includes('highest') || lower.includes('maximum') || lower.includes('max');
  const asksLowest = lower.includes('lowest') || lower.includes('minimum') || lower.includes('min');

  if (!metric) {
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

function extractIntent(question, schema) {
  const lower = question.toLowerCase();
  const metric = findRequestedMetric(question, schema);
  const time = schema.columns.find(column => column.type === 'DATE' || column.name.toLowerCase().includes('date')) || null;
  const dimension = findRequestedDimension(question, schema);

  return {
    metric: metric ? metric.name : 'none',
    time: time ? time.name : 'none',
    dimension: dimension ? dimension.name : 'none',
    aggregation: lower.includes('monthly') || lower.includes('month') ? 'monthly' : lower.includes('total') || lower.includes('sum') ? 'sum' : lower.includes('top') ? 'ranking' : 'unspecified'
  };
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

function chartHint(question) {
  const lower = question.toLowerCase();
  if (lower.includes('trend') || lower.includes('monthly') || lower.includes('over time') || lower.includes('date')) {
    return 'line_chart';
  }
  if (lower.includes('share') || lower.includes('percentage') || lower.includes('distribution')) {
    return 'pie_chart';
  }
  if (lower.includes('list') || lower.includes('show all') || lower.includes('table')) {
    return 'table';
  }
  return 'bar_chart';
}

function buildDemoSql(question, schema) {
  const lower = question.toLowerCase();
  const table = schema.tableName;
  const metric = findRequestedMetric(question, schema);
  const textColumn = findRequestedDimension(question, schema) || schema.columns.find(column => column.type === 'TEXT');
  const dateColumn = schema.columns.find(column => column.type === 'DATE' || column.name.toLowerCase().includes('date'));
  const asksHighest = lower.includes('highest') || lower.includes('maximum') || lower.includes('max');
  const asksLowest = lower.includes('lowest') || lower.includes('minimum') || lower.includes('min');

  const extremaSql = buildExtremaSql(question, schema);
  if (extremaSql) {
    return extremaSql;
  }

  if (metric && textColumn && (lower.includes('by ') || lower.includes('region') || lower.includes('category') || lower.includes('compare'))) {
    return {
      sql_query: `SELECT ${textColumn.name}, SUM(${metric.name}) AS total_value FROM ${table} GROUP BY ${textColumn.name} ORDER BY total_value DESC LIMIT 20`,
      chart_type: chartHint(question),
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
  if (requestedCount != null && rows.length !== requestedCount) {
    return {
      ok: false,
      message: `Answer count mismatch: your question asks for ${requestedCount} answers, but SQL returned ${rows.length}.`
    };
  }

  return { ok: true };
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
  const chart = allowedCharts.has(rawChart) ? rawChart : chartHint(question);

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

async function generateSql(question, schema, intent, selectedColumns, history, retryContext = null, parentContext = null) {
  const forcedExtremaSql = buildExtremaSql(question, schema);
  if (forcedExtremaSql) {
    return forcedExtremaSql;
  }
    
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

  const prompt = [
    'You are a SQL generator for a conversational business intelligence dashboard.',
    'Return JSON only with keys: sql_query, chart_type.',
    'Use only one table and only the provided columns.',
    'Target SQL dialect: SQLite-like syntax with SELECT, WHERE, GROUP BY, ORDER BY, LIMIT, SUM/COUNT/AVG/MIN/MAX and SUBSTR.',
    'If user follow-up is contextual (for example "only North region"), use conversation history to update previous query intent.',
    'If the user asks for both highest and lowest, include both MAX and MIN in the same query response.',
    'Never use destructive SQL. Never use joins.',
    `User question: ${question}`,
    `Table name: ${schema.tableName}`,
    `Intent summary: metric=${intent.metric}, time=${intent.time}, dimension=${intent.dimension}, aggregation=${intent.aggregation}`,
    'Relevant schema:',
    schemaBlock,
    'Conversation history:',
    historyBlock,
    parentBlock,
    'Chart options: line_chart, bar_chart, pie_chart, table.',
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

  const match = orderBy.match(/^([a-zA-Z0-9_]+)\s*(asc|desc)?$/i);
  if (!match) {
    return rows;
  }

  const field = match[1];
  const direction = (match[2] || 'asc').toLowerCase() === 'desc' ? -1 : 1;

  return [...rows].sort((a, b) => {
    const left = a[field];
    const right = b[field];
    if (left == null && right == null) {
      return 0;
    }
    if (left == null) {
      return -1 * direction;
    }
    if (right == null) {
      return 1 * direction;
    }
    if (left === right) {
      return 0;
    }
    return left > right ? direction : -direction;
  });
}

function executeSql(sqlQuery, dataset) {
  const parsed = parseSqlQuery(sqlQuery);
  if (parsed.tableName.toLowerCase() !== dataset.schema.tableName.toLowerCase()) {
    throw new Error(`SQL table '${parsed.tableName}' does not match dataset table '${dataset.schema.tableName}'.`);
  }

  const filteredRows = applyWhere(dataset.rows, parsed.whereConditions);
  const hasAggregate = parsed.selectExpressions.some(expr => expr.kind === 'aggregate');
  const groupBy = parsed.groupByColumns;

  let results = [];

  if (hasAggregate || groupBy.length > 0) {
    const groups = new Map();

    filteredRows.forEach(row => {
      const computedValues = {};
      parsed.selectExpressions.forEach(expr => {
        if (expr.kind === 'column' || expr.kind === 'computed') {
          computedValues[expr.alias] = computeExpressionValue(expr, row);
        }
      });

      const keyParts = groupBy.map(column => {
        if (computedValues[column] !== undefined) {
          return computedValues[column];
        }
        return row[column];
      });
      const key = JSON.stringify(keyParts);

      if (!groups.has(key)) {
        groups.set(key, { rows: [], sample: row, computedSample: computedValues });
      }

      const bucket = groups.get(key);
      bucket.rows.push(row);
      if (!bucket.computedSample || Object.keys(bucket.computedSample).length === 0) {
        bucket.computedSample = computedValues;
      }
    });

    groups.forEach(group => {
      const outRow = {};
      parsed.selectExpressions.forEach(expr => {
        if (expr.kind === 'aggregate') {
          outRow[expr.alias] = aggregateRows(group.rows, expr, null);
        } else if (expr.kind === 'column' || expr.kind === 'computed') {
          if (group.computedSample && group.computedSample[expr.alias] !== undefined) {
            outRow[expr.alias] = group.computedSample[expr.alias];
          } else {
            outRow[expr.alias] = computeExpressionValue(expr, group.sample);
          }
        }
      });
      results.push(outRow);
    });
  } else {
    results = filteredRows.map(row => {
      if (parsed.selectExpressions.length === 1 && parsed.selectExpressions[0].kind === 'column' && parsed.selectExpressions[0].source === '*') {
        return { ...row };
      }

      const projected = {};
      parsed.selectExpressions.forEach(expr => {
        projected[expr.alias] = computeExpressionValue(expr, row);
      });
      return projected;
    });
  }

  results = applyOrderBy(results, parsed.orderBy);
  if (parsed.limit !== null) {
    results = results.slice(0, parsed.limit);
  }

  const columns = results.length ? Object.keys(results[0]) : parsed.selectExpressions.map(expr => expr.alias);
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
  const dateLike = resultColumns.filter(column => schemaByName.get(column) === 'DATE' || /date|month|year|time/i.test(column));
  const nonNumeric = resultColumns.filter(column => !numeric.includes(column));

  const metric = numeric[0] || 'none';
  const dimension = (dateLike[0] || nonNumeric[0] || 'none');

  let type = 'tabular';
  if (dateLike.length > 0 && numeric.length > 0) {
    type = 'time_series';
  } else if (numeric.length >= 2) {
    type = 'relationship';
  } else if (numeric.length > 0 && nonNumeric.length > 0) {
    type = rows.length > 20 ? 'ranking' : 'categorical_comparison';
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

  if (type === 'time_series' && rows.length >= 2) {
    const first = Number(rows[0][metric]) || 0;
    const last = Number(rows[rows.length - 1][metric]) || 0;
    const trend = last >= first ? 'upward' : 'downward';
    return `${metric} shows a ${trend} trend over ${dimension}, moving from ${first.toLocaleString()} to ${last.toLocaleString()}.`;
  }

  if ((type === 'categorical_comparison' || type === 'ranking') && dimension !== 'none') {
    const sorted = [...rows]
      .filter(row => typeof row[metric] === 'number')
      .sort((left, right) => right[metric] - left[metric]);
    if (sorted.length > 0) {
      const top = sorted[0];
      return `${top[dimension]} has the highest ${metric} at ${Number(top[metric]).toLocaleString()}.`;
    }
  }

  const numericValues = rows.map(row => Number(row[metric])).filter(value => Number.isFinite(value));
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

function fallbackDemoQueries(schema, count = DEMO_QUERY_COUNT, chartType = 'all') {
  const metric = schema.columns.find(column => column.type === 'NUMBER')?.name || schema.columns[0]?.name || 'value';
  const dimension = schema.columns.find(column => column.type === 'TEXT')?.name || schema.columns[0]?.name || 'category';
  const dateCol = schema.columns.find(column => column.type === 'DATE')?.name || schema.columns.find(column => column.name.toLowerCase().includes('date'))?.name || null;

  const options = [
    { question: `Show total ${metric} by ${dimension}.`, chart_type: 'bar_chart' },
    { question: `Show top 10 ${dimension} by ${metric}.`, chart_type: 'horizontal_bar_chart' },
    { question: `Show ${dimension} share by ${metric}.`, chart_type: 'pie_chart' },
    { question: `Show ${dimension} share by ${metric} as donut.`, chart_type: 'donut_chart' },
    { question: `Show ${dimension} performance radar for ${metric}.`, chart_type: 'radar_chart' },
    { question: `List recent rows with ${dimension} and ${metric}.`, chart_type: 'table' },
    { question: `Compare average ${metric} across ${dimension}.`, chart_type: 'bar_chart' },
    { question: `Compare ${metric} points as lollipop by ${dimension}.`, chart_type: 'lollipop_chart' },
    { question: `Show distribution of ${metric} by ${dimension}.`, chart_type: 'pie_chart' },
    { question: `Show maximum and minimum ${metric} by ${dimension}.`, chart_type: 'table' },
    { question: `Show count of records by ${dimension}.`, chart_type: 'bar_chart' },
    { question: `Show ranked table of ${dimension} by ${metric}.`, chart_type: 'ranked_table' },
    { question: `Plot relationship points between metrics.`, chart_type: 'scatter_plot' }
  ];

  if (dateCol) {
    options.unshift({ question: `Show monthly trend of ${metric} using ${dateCol}.`, chart_type: 'line_chart' });
    options.unshift({ question: `Show monthly ${metric} area trend using ${dateCol}.`, chart_type: 'area_chart' });
    options.unshift({ question: `Show monthly step trend of ${metric} using ${dateCol}.`, chart_type: 'step_line_chart' });
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
  const metric = columns.find(column => column.type === 'NUMBER')?.name || columns[0]?.name || 'value';
  const dimension = columns.find(column => column.type === 'TEXT')?.name || columns[0]?.name || 'category';
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

async function generateDemoQueries(schema, count = DEMO_QUERY_COUNT, chartType = 'all') {
  if (!isAiEnabled()) {
    return fallbackDemoQueries(schema, count, chartType);
  }

  const schemaBlock = schema.columns
    .map(column => `- ${column.name} (${column.type})`)
    .join('\n');

  const prompt = [
    'Create demo business questions for a conversational BI dashboard.',
    `Generate exactly ${count} questions for this table schema.`,
    chartType === 'all'
      ? `Use mixed chart types across this list: ${CHART_TYPES.join(', ')}.`
      : `Use only this chart_type for every question: ${chartType}.`,
    'Return JSON only in this format:',
    `{"queries":[{"question":"...","chart_type":"${CHART_TYPES.join('|')}"}]}`,
    'Constraints:',
    '- Questions must be realistic and directly answerable with this single table.',
    '- Use varied business intents (trend, comparison, distribution, ranking, listing).',
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
      return fallbackDemoQueries(schema, count, chartType);
    }
    return diversifyDemoQueries(queries, schema, count, chartType);
  } catch (_error) {
    return fallbackDemoQueries(schema, count, chartType);
  }
}

function fallbackFollowUpDemoQueries(schema, parentContext, count = 6) {
  const parentColumns = Array.isArray(parentContext.columns) ? parentContext.columns : [];
  const schemaByName = new Map(schema.columns.map(column => [column.name, column.type]));
  const numeric = parentColumns.filter(column => schemaByName.get(column) === 'NUMBER');
  const dateLike = parentColumns.filter(column => schemaByName.get(column) === 'DATE' || /date|month|year|time/i.test(column));
  const textLike = parentColumns.filter(column => !numeric.includes(column) && !dateLike.includes(column));
  const metric = numeric[0] || schema.columns.find(column => column.type === 'NUMBER')?.name || 'value';
  const dimension = textLike[0] || schema.columns.find(column => column.type === 'TEXT')?.name || 'category';
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

  return deduped.slice(0, count);
}

async function generateFollowUpDemoQueries(schema, parentContext, count = 6) {
  if (!isAiEnabled()) {
    return fallbackFollowUpDemoQueries(schema, parentContext, count);
  }

  const schemaBlock = schema.columns.map(column => `- ${column.name} (${column.type})`).join('\n');
  const parentSql = String(parentContext.sql || '');
  const parentColumns = Array.isArray(parentContext.columns) ? parentContext.columns : [];
  const sampleRows = Array.isArray(parentContext.sample) ? parentContext.sample.slice(0, 3) : [];

  const prompt = [
    'Create follow-up questions for an existing BI query result.',
    `Generate exactly ${count} follow-up questions that refine, filter, drill-down, rank, compare, or trend the current result.`,
    `Allowed chart types: ${CHART_TYPES.join(', ')}.`,
    'Return JSON only in this format:',
    `{"queries":[{"question":"...","chart_type":"${CHART_TYPES.join('|')}"}]}`,
    'Constraints:',
    '- Must be follow-up style (context-dependent), not generic first-time questions.',
    '- Questions should be answerable from the same table.',
    '- Include mixed chart types where appropriate.',
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
      return fallbackFollowUpDemoQueries(schema, parentContext, count);
    }
    return diversifyDemoQueries(queries, schema, count, 'all');
  } catch (_error) {
    return fallbackFollowUpDemoQueries(schema, parentContext, count);
  }
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

async function runPipeline(fileName, question, sessionId, parentContext = null) {
  const validationTrace = [];
  const pushTrace = (stage, message, status = 'ok') => {
    validationTrace.push({
      stage,
      message,
      status,
      timestamp: new Date().toISOString()
    });
  };

  const dataset = loadDataset(fileName);
  pushTrace('query_extracted', 'User question accepted and dataset loaded.');

  const intent = extractIntent(question, dataset.schema);
  const selectedColumns = selectRelevantColumns(question, dataset.schema);
  const history = getConversation(sessionId).slice(-4);

  pushTrace('sending_to_ai', 'Sending question and schema context to SQL generator.');
  let generated = await generateSql(question, dataset.schema, intent, selectedColumns, history, null, parentContext);
  pushTrace('ai_sql_received', `SQL candidate received (${generated.mode || 'gemini'}).`);

  let validation = validateSqlAgainstSchema(generated.sql_query, dataset.schema);
  pushTrace(
    'rechecking_ai_sql',
    validation.ok ? 'AI SQL passed schema and safety checks.' : `AI SQL check failed: ${validation.error}`,
    validation.ok ? 'ok' : 'error'
  );

  let execution = null;
  let repaired = false;

  if (validation.ok) {
    try {
      execution = executeSql(generated.sql_query, dataset);
      pushTrace('query_executed', `SQL executed successfully with ${execution.rows.length} rows.`);
    } catch (error) {
      validation = { ok: false, error: `Execution failed: ${error.message}` };
      pushTrace('query_execution_failed', validation.error, 'error');
    }
  }

  if (!validation.ok && isAiEnabled()) {
    pushTrace('repairing_sql', 'Requesting SQL regeneration after failed validation.');
    generated = await generateSql(question, dataset.schema, intent, selectedColumns, history, {
      sql: generated.sql_query,
      error: validation.error
    }, parentContext);
    validation = validateSqlAgainstSchema(generated.sql_query, dataset.schema);
    pushTrace(
      'rechecking_ai_sql_after_repair',
      validation.ok ? 'Repaired SQL passed schema and safety checks.' : `Repaired SQL failed: ${validation.error}`,
      validation.ok ? 'ok' : 'error'
    );
    if (validation.ok) {
      execution = executeSql(generated.sql_query, dataset);
      pushTrace('query_executed', `Repaired SQL executed successfully with ${execution.rows.length} rows.`);
    }
    repaired = true;
  }

  if (!execution) {
    throw new Error(validation.error || 'Failed to execute generated SQL.');
  }

  const answerCoverage = validateAnswerCoverage(question, execution);
  if (!answerCoverage.ok) {
    pushTrace('answer_coverage_check', answerCoverage.message, 'error');
    throw new Error(answerCoverage.message);
  }
  pushTrace('answer_coverage_check', 'Requested answer count matches generated result.');

  const explanation = await explainSql(generated.sql_query, question);
  const chartData = buildChartData(execution.rows);
  const dataStructure = detectDataStructure(dataset.schema, execution.rows, execution.columns);
  const allChartOptions = listCompatibleChartTypes(dataStructure, execution.rows.length);
  const chartOptions = recommendChartOptions(dataStructure, execution.rows.length);
  const recommendedChart = chartOptions[0] || 'table';
  const requestedChart = generated.chart_type || chartHint(question);
  const finalChart = allChartOptions.includes(requestedChart) ? requestedChart : recommendedChart;
  pushTrace(
    'checking_valid_charts',
    requestedChart === finalChart
      ? `Chart "${finalChart}" is valid for this result.`
      : `Requested chart "${requestedChart}" adjusted to "${finalChart}" based on result structure.`
  );

  const insight = buildInsight(execution.rows, dataStructure);
  pushTrace('preparing_response', 'Final response payload prepared.');

  const response = {
    file_name: dataset.fileName,
    table_name: dataset.schema.tableName,
    rows_loaded: dataset.rows.length,
    truncated_rows: dataset.truncated,
    intent,
    selected_columns: selectedColumns,
    sql_query: generated.sql_query,
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
    validation_trace: validationTrace,
    validation_error: null,
    mode: generated.mode || 'gemini'
  };

  pushConversationEntry(sessionId, {
    question,
    sql_query: response.sql_query,
    chart_type: response.chart_type,
    timestamp: new Date().toISOString()
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
    sendJson(res, 200, { files: getCsvFiles() });
    return;
  }

  if (req.method === 'GET' && pathname === '/api/schema') {
    try {
      const fileName = parsedUrl.searchParams.get('file');
      if (!fileName) {
        sendJson(res, 400, { error: 'file query parameter is required.' });
        return;
      }
      const dataset = loadDataset(fileName);
      sendJson(res, 200, {
        file_name: dataset.fileName,
        table_name: dataset.schema.tableName,
        columns: dataset.schema.columns,
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
      const fileName = parsedUrl.searchParams.get('file');
      if (!fileName) {
        sendJson(res, 400, { error: 'file query parameter is required.' });
        return;
      }
      const dataset = loadDataset(fileName);
      const rawCount = parseInt(parsedUrl.searchParams.get('count') || '8', 10);
      const count = Math.min(20, Math.max(1, Number.isFinite(rawCount) ? rawCount : 8));
      const rawChartType = (parsedUrl.searchParams.get('chart_type') || 'all').trim();
      const chartType = rawChartType === 'all' ? 'all' : (CHART_TYPES.includes(rawChartType) ? rawChartType : 'all');
      const queries = await generateDemoQueries(dataset.schema, count, chartType);
      sendJson(res, 200, {
        file_name: dataset.fileName,
        table_name: dataset.schema.tableName,
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

      const dataset = loadDataset(body.fileName);
      const rawCount = parseInt(body.count || '6', 10);
      const count = Math.min(12, Math.max(2, Number.isFinite(rawCount) ? rawCount : 6));
      const parentContext = {
        sql: String(body.parentContext.sql || ''),
        columns: Array.isArray(body.parentContext.columns) ? body.parentContext.columns : [],
        rowCount: Number(body.parentContext.rowCount) || 0,
        sample: Array.isArray(body.parentContext.sample) ? body.parentContext.sample.slice(0, 3) : []
      };

      const queries = await generateFollowUpDemoQueries(dataset.schema, parentContext, count);
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

  if (req.method === 'POST' && pathname === '/api/upload') {
    try {
      const contentType = req.headers['content-type'] || '';
      if (!contentType.toLowerCase().startsWith('multipart/form-data')) {
        sendJson(res, 400, { error: 'Use multipart/form-data for file upload.' });
        return;
      }

      const raw = await collectRawBody(req);
      const parsed = parseMultipartCsv(raw, contentType);
      const saveToDatabaseRaw = parsed.fields && typeof parsed.fields.saveToDatabase === 'string'
        ? parsed.fields.saveToDatabase
        : '';
      const saveToDatabase = /^(true|1|yes|on)$/i.test(saveToDatabaseRaw);
      const uploadFileName = reserveUploadFileName(parsed.fileName);

      if (saveToDatabase) {
        fs.writeFileSync(path.join(DATA_DIR, uploadFileName), parsed.content, 'utf8');
      } else {
        temporaryUploads.set(uploadFileName, {
          content: parsed.content,
          uploadedAt: Date.now()
        });
      }

      const dataset = loadDataset(uploadFileName);
      sendJson(res, 200, {
        message: saveToDatabase ? 'CSV uploaded and saved to data storage.' : 'CSV uploaded for temporary session use.',
        file_name: uploadFileName,
        table_name: dataset.schema.tableName,
        rows_loaded: dataset.rows.length,
        columns: dataset.schema.columns,
        temporary: !saveToDatabase,
        persisted: saveToDatabase
      });
    } catch (error) {
      const handled = toApiError(error, 'Upload failed. Please verify the CSV file and try again.');
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
      const result = await runPipeline(body.fileName, body.question, sessionId, parentContext);
      sendJson(res, 200, result);
    } catch (error) {
      const handled = toApiError(error, 'Could not build your dashboard for this query. Please try again.');
      sendJson(res, handled.statusCode, { error: handled.message });
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
      const result = await runPipeline(body.fileName, body.question, sessionId, parentContext);
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
      const result = await runPipeline(body.fileName, body.question, sessionId);
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
      const fileName = parsedUrl.searchParams.get('file') || '';
      if (!fileName) {
        sendJson(res, 400, { error: 'file parameter required.' });
        return;
      }
      const dataset = loadDataset(fileName);
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

server.listen(PORT, () => {
  console.log(`Compact BI demo running at http://localhost:${PORT}`);
  console.log(`Drop CSV files into: ${DATA_DIR}`);
});
