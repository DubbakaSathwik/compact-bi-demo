const http = require('http');
const fs = require('fs');
const path = require('path');

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
const GEMINI_MODEL = 'models/gemini-2.5-flash';
const MAX_ROWS = 5000;
const MAX_UPLOAD_BYTES = 15 * 1024 * 1024;
const MAX_HISTORY = 8;
const DEMO_QUERY_COUNT = 8;
const CHART_TYPES = ['bar_chart', 'line_chart', 'pie_chart', 'table'];
const conversations = new Map();

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
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

function getCsvFiles() {
  return fs.readdirSync(DATA_DIR).filter(fileName => fileName.toLowerCase().endsWith('.csv'));
}

function loadDataset(fileName) {
  const safeName = path.basename(fileName);
  const filePath = path.join(DATA_DIR, safeName);
  if (!fs.existsSync(filePath)) {
    throw new Error('Selected CSV file was not found in the data folder.');
  }
  const text = fs.readFileSync(filePath, 'utf8');
  const parsed = parseCsv(text);
  const schema = inferSchema(parsed.headers, parsed.rows, safeName);
  return { fileName: safeName, ...parsed, schema };
}

function extractIntent(question, schema) {
  const lower = question.toLowerCase();
  const metric = schema.columns.find(column => ['revenue', 'sales', 'quantity', 'price', 'amount', 'count', 'cost', 'profit', 'income', 'salary', 'spend'].some(token => column.name.toLowerCase().includes(token))) || schema.columns.find(column => column.type === 'NUMBER') || null;
  const time = schema.columns.find(column => column.type === 'DATE' || column.name.toLowerCase().includes('date')) || null;
  const dimension = schema.columns.find(column => lower.includes(column.name.toLowerCase())) || schema.columns.find(column => column.type === 'TEXT') || null;

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
  const metric = schema.columns.find(column => ['revenue', 'sales', 'quantity', 'price', 'amount', 'profit', 'cost', 'income', 'salary', 'spend'].some(token => column.name.toLowerCase().includes(token))) || schema.columns.find(column => column.type === 'NUMBER');
  const textColumn = schema.columns.find(column => column.type === 'TEXT');
  const dateColumn = schema.columns.find(column => column.type === 'DATE' || column.name.toLowerCase().includes('date'));

  if (metric && (lower.includes('highest') || lower.includes('maximum') || lower.includes('max'))) {
    return {
      sql_query: `SELECT MAX(${metric.name}) AS highest_${metric.name} FROM ${table}`,
      chart_type: 'table',
      mode: 'demo'
    };
  }

  if (metric && (lower.includes('lowest') || lower.includes('minimum') || lower.includes('min'))) {
    return {
      sql_query: `SELECT MIN(${metric.name}) AS lowest_${metric.name} FROM ${table}`,
      chart_type: 'table',
      mode: 'demo'
    };
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
  const allowedCharts = new Set(['line_chart', 'bar_chart', 'pie_chart', 'table']);
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

async function generateSql(question, schema, intent, selectedColumns, history, retryContext = null) {
  if (!geminiApiKey || geminiApiKey === 'PASTE_GEMINI_API_KEY_HERE') {
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

  const prompt = [
    'You are a SQL generator for a conversational business intelligence dashboard.',
    'Return JSON only with keys: sql_query, chart_type.',
    'Use only one table and only the provided columns.',
    'Target SQL dialect: SQLite-like syntax with SELECT, WHERE, GROUP BY, ORDER BY, LIMIT, SUM/COUNT/AVG/MIN/MAX and SUBSTR.',
    'If user follow-up is contextual (for example "only North region"), use conversation history to update previous query intent.',
    'Never use destructive SQL. Never use joins.',
    `User question: ${question}`,
    `Table name: ${schema.tableName}`,
    `Intent summary: metric=${intent.metric}, time=${intent.time}, dimension=${intent.dimension}, aggregation=${intent.aggregation}`,
    'Relevant schema:',
    schemaBlock,
    'Conversation history:',
    historyBlock,
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

function fallbackDemoQueries(schema, count = DEMO_QUERY_COUNT) {
  const metric = schema.columns.find(column => column.type === 'NUMBER')?.name || schema.columns[0]?.name || 'value';
  const dimension = schema.columns.find(column => column.type === 'TEXT')?.name || schema.columns[0]?.name || 'category';
  const dateCol = schema.columns.find(column => column.type === 'DATE')?.name || schema.columns.find(column => column.name.toLowerCase().includes('date'))?.name || null;

  const options = [
    { question: `Show total ${metric} by ${dimension}.`, chart_type: 'bar_chart' },
    { question: `Show top 10 ${dimension} by ${metric}.`, chart_type: 'bar_chart' },
    { question: `Show ${dimension} share by ${metric}.`, chart_type: 'pie_chart' },
    { question: `List recent rows with ${dimension} and ${metric}.`, chart_type: 'table' },
    { question: `Compare average ${metric} across ${dimension}.`, chart_type: 'bar_chart' },
    { question: `Show distribution of ${metric} by ${dimension}.`, chart_type: 'pie_chart' },
    { question: `Show maximum and minimum ${metric} by ${dimension}.`, chart_type: 'table' },
    { question: `Show count of records by ${dimension}.`, chart_type: 'bar_chart' }
  ];

  if (dateCol) {
    options[1] = { question: `Show monthly trend of ${metric} using ${dateCol}.`, chart_type: 'line_chart' };
    options[6] = { question: `Show monthly ${metric} summary using ${dateCol}.`, chart_type: 'line_chart' };
  }

  return options.slice(0, count);
}

function diversifyDemoQueries(queries, count = DEMO_QUERY_COUNT) {
  const normalized = [];
  const seen = new Set();

  for (const item of queries) {
    const question = (item.question || '').toString().trim();
    if (!question) {
      continue;
    }
    const key = question.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    normalized.push({
      question,
      chart_type: CHART_TYPES.includes(item.chart_type) ? item.chart_type : null
    });
  }

  while (normalized.length < count) {
    normalized.push({
      question: `Show sample insight ${normalized.length + 1}.`,
      chart_type: null
    });
  }

  return normalized.slice(0, count).map((item, index, list) => {
    let chart = item.chart_type || CHART_TYPES[index % CHART_TYPES.length];
    if (index > 0 && chart === list[index - 1]?.chart_type) {
      chart = CHART_TYPES[(index + 1) % CHART_TYPES.length];
    }
    return {
      question: item.question,
      chart_type: chart
    };
  });
}

async function generateDemoQueries(schema, count = DEMO_QUERY_COUNT) {
  if (!geminiApiKey || geminiApiKey === 'PASTE_GEMINI_API_KEY_HERE') {
    return fallbackDemoQueries(schema, count);
  }

  const schemaBlock = schema.columns
    .map(column => `- ${column.name} (${column.type})`)
    .join('\n');

  const prompt = [
    'Create demo business questions for a conversational BI dashboard.',
    `Generate exactly ${count} questions for this table schema.`,
    'Return JSON only in this format:',
    '{"queries":[{"question":"...","chart_type":"bar_chart|line_chart|pie_chart|table"}]}',
    'Constraints:',
    '- Questions must be realistic and directly answerable with this single table.',
    '- Use varied business intents (trend, comparison, distribution, ranking, listing).',
    '- Ensure chart_type is distributed across all four chart types.',
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
      return fallbackDemoQueries(schema, count);
    }
    return diversifyDemoQueries(queries, count);
  } catch (_error) {
    return fallbackDemoQueries(schema, count);
  }
}

async function explainSql(sqlQuery, question) {
  if (!geminiApiKey || geminiApiKey === 'PASTE_GEMINI_API_KEY_HERE') {
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
      continue;
    }

    const safeName = path.basename(originalName).replace(/[^a-zA-Z0-9._-]/g, '_');
    if (!safeName.toLowerCase().endsWith('.csv')) {
      throw new Error('Only CSV files are accepted.');
    }

    return {
      fileName: safeName,
      content: Buffer.from(rawContent, 'binary').toString('utf8')
    };
  }

  throw new Error('No file field found in upload payload.');
}

async function runPipeline(fileName, question, sessionId) {
  const dataset = loadDataset(fileName);
  const intent = extractIntent(question, dataset.schema);
  const selectedColumns = selectRelevantColumns(question, dataset.schema);
  const history = getConversation(sessionId).slice(-4);

  let generated = await generateSql(question, dataset.schema, intent, selectedColumns, history);
  let validation = validateSql(generated.sql_query, dataset.schema.tableName);
  let execution = null;
  let repaired = false;

  if (validation.ok) {
    try {
      execution = executeSql(generated.sql_query, dataset);
    } catch (error) {
      validation = { ok: false, error: `Execution failed: ${error.message}` };
    }
  }

  if (!validation.ok && geminiApiKey && geminiApiKey !== 'PASTE_GEMINI_API_KEY_HERE') {
    generated = await generateSql(question, dataset.schema, intent, selectedColumns, history, {
      sql: generated.sql_query,
      error: validation.error
    });
    validation = validateSql(generated.sql_query, dataset.schema.tableName);
    if (validation.ok) {
      execution = executeSql(generated.sql_query, dataset);
    }
    repaired = true;
  }

  if (!execution) {
    throw new Error(validation.error || 'Failed to execute generated SQL.');
  }

  const explanation = await explainSql(generated.sql_query, question);
  const chartData = buildChartData(execution.rows);

  const response = {
    file_name: dataset.fileName,
    table_name: dataset.schema.tableName,
    rows_loaded: dataset.rows.length,
    truncated_rows: dataset.truncated,
    intent,
    selected_columns: selectedColumns,
    sql_query: generated.sql_query,
    sql_explanation: explanation,
    chart_type: generated.chart_type || chartHint(question),
    chart_data: chartData,
    result_columns: execution.columns,
    result_rows: execution.rows,
    validation_status: repaired ? 'repaired_and_executed' : 'executed',
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
      sendJson(res, 400, { error: error.message });
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
      const queries = await generateDemoQueries(dataset.schema, count);
      sendJson(res, 200, {
        file_name: dataset.fileName,
        table_name: dataset.schema.tableName,
        queries
      });
    } catch (error) {
      sendJson(res, 500, { error: error.message });
    }
    return;
  }

  if (req.method === 'GET' && pathname === '/api/history') {
    const sessionId = parsedUrl.searchParams.get('sessionId') || 'default';
    sendJson(res, 200, { sessionId, history: getConversation(sessionId) });
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
      const targetPath = path.join(DATA_DIR, parsed.fileName);
      fs.writeFileSync(targetPath, parsed.content, 'utf8');

      const dataset = loadDataset(parsed.fileName);
      sendJson(res, 200, {
        message: 'CSV uploaded successfully.',
        file_name: parsed.fileName,
        table_name: dataset.schema.tableName,
        rows_loaded: dataset.rows.length,
        columns: dataset.schema.columns
      });
    } catch (error) {
      sendJson(res, 400, { error: error.message });
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
      sendJson(res, 500, { error: error.message });
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
      const result = await runPipeline(body.fileName, body.question, sessionId);
      sendJson(res, 200, result);
    } catch (error) {
      sendJson(res, 500, { error: error.message });
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
      sendJson(res, 500, { error: error.message });
    }
    return;
  }

  if (req.method === 'POST' && pathname === '/api/settings') {
    try {
      const body = await collectJsonBody(req);
      const key = (body.apiKey || '').toString().trim();
      if (!key || key.length < 10) {
        sendJson(res, 400, { error: 'Invalid API key.' });
        return;
      }
      geminiApiKey = key;
      process.env.GEMINI_API_KEY = key;
      const envPath = path.join(__dirname, '.env');
      let envContent = fs.existsSync(envPath) ? fs.readFileSync(envPath, 'utf8') : '';
      if (/^GEMINI_API_KEY=/m.test(envContent)) {
        envContent = envContent.replace(/^GEMINI_API_KEY=.*/m, `GEMINI_API_KEY=${key}`);
      } else {
        envContent = envContent.trimEnd() + `\nGEMINI_API_KEY=${key}\n`;
      }
      fs.writeFileSync(envPath, envContent, 'utf8');
      const masked = key.slice(0, 6) + '...' + key.slice(-4);
      sendJson(res, 200, { ok: true, masked });
    } catch (error) {
      sendJson(res, 500, { error: error.message });
    }
    return;
  }

  if (req.method === 'GET' && pathname === '/api/settings') {
    const hasKey = geminiApiKey && geminiApiKey !== 'PASTE_GEMINI_API_KEY_HERE';
    const masked = hasKey ? (geminiApiKey.slice(0, 6) + '...' + geminiApiKey.slice(-4)) : null;
    sendJson(res, 200, { configured: hasKey, masked });
    return;
  }

  sendJson(res, 404, { error: 'Not found.' });
});

server.listen(PORT, () => {
  console.log(`Compact BI demo running at http://localhost:${PORT}`);
  console.log(`Drop CSV files into: ${DATA_DIR}`);
});
