import express from 'express';

const app = express();

app.use(express.json({ limit: '5mb' }));
app.use(express.urlencoded({ extended: true, limit: '5mb' }));

const PORT = Number(process.env.PORT || 3000);

const GRAPH_API_VERSION = process.env.GRAPH_API_VERSION || 'v25.0';
const META_ACCESS_TOKEN = process.env.META_ACCESS_TOKEN || '';
const WHATSAPP_PHONE_NUMBER_ID = process.env.WHATSAPP_PHONE_NUMBER_ID || '1118555908004589';
const WABA_ID = process.env.WABA_ID || '1251319566775221';
const META_VERIFY_TOKEN = process.env.META_VERIFY_TOKEN || 'change_this_verify_token';
const MANAGER_WHATSAPP = normalizePhone(process.env.MANAGER_WHATSAPP || '77076669955');

const WHATSAPP_TEMPLATE_LANG = process.env.WHATSAPP_TEMPLATE_LANG || 'ru';

const RO_SECRET = process.env.RO_SECRET || '';
const REMONLINE_API_KEY = process.env.REMONLINE_API_KEY || '';
const REMONLINE_API_BASE_URL = (process.env.REMONLINE_API_BASE_URL || 'https://api.roapp.io/v2').replace(/\/+$/, '');

const READY_STATUS_IDS = parseIdList(process.env.READY_STATUS_IDS || '363629');
const CLOSED_STATUS_IDS = parseIdList(process.env.CLOSED_STATUS_IDS || '363632');
const ACCEPTED_STATUS_IDS = parseIdList(process.env.ACCEPTED_STATUS_IDS || '');
const INTERNAL_STATUS_IDS = parseIdList(process.env.INTERNAL_STATUS_IDS || '3045928');

const RECEIPT_TEMPLATE_NAME = process.env.RECEIPT_TEMPLATE_NAME || 'order_closed_receipt';
const RECEIPT_SEARCH_ENABLED = String(process.env.RECEIPT_SEARCH_ENABLED || 'true').toLowerCase() !== 'false';
const RECEIPT_RETRY_MS = parseRetryList(process.env.RECEIPT_RETRY_MS || '0,10000,30000,60000,180000,600000');

const GRAPH_BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;

const receiptJobs = new Set();
const receiptSent = new Set();

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function parseIdList(value) {
  return new Set(
    String(value || '')
      .split(',')
      .map((v) => v.trim())
      .filter(Boolean)
  );
}

function parseRetryList(value) {
  return String(value || '')
    .split(',')
    .map((v) => Number(String(v).trim()))
    .filter((v) => Number.isFinite(v) && v >= 0);
}

function normalizePhone(value) {
  if (!value) return '';

  let digits = String(value).replace(/\D/g, '');

  if (digits.length === 11 && digits.startsWith('8')) {
    digits = `7${digits.slice(1)}`;
  }

  if (digits.length === 10) {
    digits = `7${digits}`;
  }

  return digits;
}

function prettyPhone(value) {
  const phone = normalizePhone(value);
  return phone ? `+${phone}` : 'не указан';
}

function pick(obj, paths, fallback = '') {
  for (const path of paths) {
    const parts = path.split('.');
    let cur = obj;

    for (const part of parts) {
      if (cur == null) break;
      cur = cur[part];
    }

    if (cur !== undefined && cur !== null && String(cur).trim() !== '') {
      return cur;
    }
  }

  return fallback;
}

function looksLikePhone(value) {
  const raw = String(value ?? '').trim();
  if (!raw) return '';

  const full = raw.match(/(?:\+?7|8)[\s\-()]*\d{3}[\s\-()]*\d{3}[\s\-()]*\d{2}[\s\-()]*\d{2}/);
  if (full) return normalizePhone(full[0]);

  const compact = raw.replace(/\D/g, '');

  if (/^7\d{10}$/.test(compact)) return normalizePhone(compact);
  if (/^8\d{10}$/.test(compact)) return normalizePhone(compact);
  if (/^\d{10}$/.test(compact)) return normalizePhone(compact);

  return '';
}

function looksLikeOrderNumber(value) {
  const text = String(value || '').trim();
  if (!text) return false;

  return /^[A-Za-zА-Яа-я]\d{3,}$/.test(text) || /^\d{6,}$/.test(text);
}

function cleanClientName(value) {
  const text = String(value || '').trim();

  if (!text) return '';
  if (looksLikePhone(text)) return '';
  if (looksLikeOrderNumber(text)) return '';
  if (/^(заявка|форма|обращение|новое обращение)$/i.test(text)) return '';

  return text;
}

function titleCaseName(value) {
  const text = cleanClientName(value);
  if (!text) return '';

  return text
    .split(/\s+/)
    .map((part) => part ? part[0].toUpperCase() + part.slice(1) : part)
    .join(' ');
}

function findFirstPhone(obj) {
  const direct = pick(obj, [
    'client.phone',
    'client.mobile',
    'client.phone_number',
    'client.phoneNumber',
    'client.whatsapp',
    'client.phone.0',
    'client.phones.0.phone',
    'client.phones.0.number',
    'client.phones.0.value',

    'metadata.client.phone',
    'metadata.client.mobile',
    'metadata.client.phone_number',
    'metadata.client.phoneNumber',
    'metadata.client.phone.0',
    'metadata.client.phones.0.phone',
    'metadata.client.phones.0.number',
    'metadata.client.phones.0.value',

    'data.client.phone',
    'data.client.mobile',
    'data.client.phone_number',
    'data.client.phoneNumber',
    'data.client.phone.0',
    'data.client.phones.0.phone',
    'data.client.phones.0.number',
    'data.client.phones.0.value',

    'order.client.phone',
    'order.client.mobile',
    'order.client.phone_number',
    'order.client.phoneNumber',
    'order.client.phone.0',
    'order.client.phones.0.phone',
    'order.client.phones.0.number',
    'order.client.phones.0.value',

    'ro_api_order.client.phone',
    'ro_api_order.client.mobile',
    'ro_api_order.client.phone_number',
    'ro_api_order.client.phoneNumber',
    'ro_api_order.client.phone.0',
    'ro_api_order.client.phones.0.phone',
    'ro_api_order.client.phones.0.number',
    'ro_api_order.client.phones.0.value',

    'ro_api_order.data.client.phone',
    'ro_api_order.data.client.mobile',
    'ro_api_order.data.client.phone_number',
    'ro_api_order.data.client.phoneNumber',
    'ro_api_order.data.client.phone.0',
    'ro_api_order.data.client.phones.0.phone',
    'ro_api_order.data.client.phones.0.number',
    'ro_api_order.data.client.phones.0.value',

    'phone',
    'mobile',
    'telephone',
    'phone_number',
    'phoneNumber',
    'whatsapp',
    'client_phone',
    'customer_phone'
  ]);

  const normalizedDirect = looksLikePhone(direct);
  if (normalizedDirect) return normalizedDirect;

  const candidates = [];
  const seen = new Set();

  function walk(node, path = '', depth = 0) {
    if (node == null || depth > 14) return;

    if (typeof node === 'string' || typeof node === 'number') {
      const found = looksLikePhone(node);
      const keyLooksPhone = /phone|mobile|tel|contact|whatsapp|wa|номер|телефон|домашний/i.test(path);

      if (found && (keyLooksPhone || found.length === 11)) {
        if (!seen.has(found)) {
          seen.add(found);
          candidates.push(found);
        }
      }

      return;
    }

    if (Array.isArray(node)) {
      node.forEach((item, i) => walk(item, `${path}.${i}`, depth + 1));
      return;
    }

    if (typeof node === 'object') {
      for (const [key, value] of Object.entries(node)) {
        walk(value, path ? `${path}.${key}` : key, depth + 1);
      }
    }
  }

  walk(obj);

  return candidates[0] || '';
}

function findFirstId(obj, paths) {
  const direct = pick(obj, paths, '');
  if (direct !== '') return String(direct);
  return '';
}

function getRoOrderId(payload) {
  return findFirstId(payload, [
    'metadata.order.id',
    'order.id',
    'data.order.id',
    'context.order.id',
    'metadata.order_id',
    'object.id',
    'object_id',
    'context.object_id',
    'data.object_id',
    'rel_obj.id',
    'ro_api_order.id',
    'ro_api_order.data.id',
    'ro_api_order.order.id',
    'ro_api_order.data.order.id'
  ]);
}

function getRoLeadId(payload) {
  return findFirstId(payload, [
    'metadata.lead.id',
    'lead.id',
    'data.lead.id',
    'context.lead.id',
    'metadata.lead_id',
    'object.id',
    'object_id',
    'context.object_id',
    'data.object_id'
  ]);
}

function getRoClientId(payload) {
  return findFirstId(payload, [
    'metadata.client.id',
    'client.id',
    'customer.id',
    'data.client.id',
    'data.customer.id',
    'order.client.id',
    'metadata.order.client.id',
    'data.order.client.id',
    'context.client.id',
    'metadata.client_id',
    'ro_api_order.client.id',
    'ro_api_order.client_id',
    'ro_api_order.data.client.id',
    'ro_api_order.data.client_id'
  ]);
}

function getNewStatusId(payload) {
  return String(pick(payload, [
    'metadata.new.id',
    'metadata.new.status.id',
    'new_status.id',
    'status.id',
    'metadata.status.id',
    'order.status.id',
    'data.status.id',
    'data.order.status.id',
    'ro_api_order.status.id',
    'ro_api_order.data.status.id',
    'ro_api_order.order.status.id',
    'ro_api_order.data.order.status.id'
  ], '')).trim();
}

function getEventName(payload) {
  return String(pick(payload, [
    'event',
    'event_name',
    'type',
    'action',
    'topic',
    'hook',
    'webhook_event',
    'data.event',
    'data.type',
    'object.event'
  ], '')).toLowerCase();
}

function compactPayloadPreview(payload) {
  try {
    return JSON.stringify(payload).slice(0, 12000);
  } catch {
    return '[payload stringify failed]';
  }
}

function unwrapRoResponse(data) {
  if (!data || typeof data !== 'object') return data;

  if (data.data && typeof data.data === 'object' && !Array.isArray(data.data)) return data.data;
  if (data.result && typeof data.result === 'object') return data.result;
  if (data.item && typeof data.item === 'object') return data.item;

  return data;
}

async function roApiGetRaw(path) {
  if (!REMONLINE_API_KEY) {
    throw new Error('REMONLINE_API_KEY is empty');
  }

  const url = `${REMONLINE_API_BASE_URL}/${path.replace(/^\/+/, '')}`;

  const res = await fetch(url, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${REMONLINE_API_KEY}`,
      Accept: 'application/json'
    }
  });

  const text = await res.text();
  let data;

  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }

  if (!res.ok) {
    const err = new Error(data?.message || data?.error?.message || `RO API error ${res.status}`);
    err.status = res.status;
    err.data = data;
    err.url = url;
    throw err;
  }

  return data;
}

async function roApiGet(path) {
  const raw = await roApiGetRaw(path);
  return unwrapRoResponse(raw);
}

async function tryRoApiPaths(paths, label, options = {}) {
  let lastErr = null;

  for (const path of paths) {
    try {
      const data = options.raw ? await roApiGetRaw(path) : await roApiGet(path);
      log(`RO API ${label} success:`, path);
      return data;
    } catch (err) {
      lastErr = err;
      log(`RO API ${label} failed:`, path, err.status || '', err.message);
    }
  }

  if (lastErr) throw lastErr;
  return null;
}

async function fetchRoOrder(orderId) {
  if (!orderId) return null;

  return tryRoApiPaths([
    `/orders/${orderId}`,
    `/orders/${orderId}?include=client`,
    `/orders/${orderId}?expand=client`,
    `/orders/${orderId}?with=client`
  ], `order ${orderId}`, { raw: true });
}

async function fetchRoOrderPublicUrl(orderId) {
  if (!orderId) return null;
  return roApiGetRaw(`/orders/${orderId}/public-url`);
}

async function enrichRoOrderPayloadWithApi(payload) {
  const orderId = getRoOrderId(payload);
  const clientId = getRoClientId(payload);

  log('Order webhook: trying RO API first.', {
    orderId: orderId || null,
    clientId: clientId || null
  });

  let orderData = null;

  if (orderId) {
    try {
      orderData = await fetchRoOrder(orderId);
      const phoneFromOrder = findFirstPhone(orderData);

      if (phoneFromOrder) {
        return {
          payload: { ...payload, ro_api_order: orderData },
          phone: phoneFromOrder,
          source: 'ro_api_order'
        };
      }
    } catch (err) {
      log('RO API order lookup failed finally:', err.status || '', err.message);
    }
  }

  const fallbackPhone = findFirstPhone(payload);

  if (fallbackPhone) {
    log('WARNING: RO API phone not found, fallback to webhook phone:', fallbackPhone);

    return {
      payload: { ...payload, ro_api_order: orderData },
      phone: fallbackPhone,
      source: 'webhook_fallback'
    };
  }

  return {
    payload: { ...payload, ro_api_order: orderData },
    phone: '',
    source: 'not_found'
  };
}

function getClientName(payload) {
  const firstName = pick(payload, [
    'ro_api_order.client.first_name',
    'ro_api_order.data.client.first_name',
    'ro_api_order.first_name',
    'ro_api_order.data.first_name',

    'metadata.client.first_name',
    'client.first_name',
    'data.client.first_name',
    'metadata.order.client.first_name',
    'order.client.first_name'
  ], '');

  const fullName = pick(payload, [
    'ro_api_order.client.name',
    'ro_api_order.client.fullname',
    'ro_api_order.client.full_name',
    'ro_api_order.data.client.name',
    'ro_api_order.data.client.fullname',
    'ro_api_order.data.client.full_name',
    'ro_api_order.name',
    'ro_api_order.fullname',
    'ro_api_order.full_name',
    'ro_api_order.data.name',
    'ro_api_order.data.fullname',
    'ro_api_order.data.full_name',

    'metadata.client.fullname',
    'metadata.client.full_name',
    'metadata.client.name',
    'client.fullname',
    'client.full_name',
    'client.name',
    'data.client.fullname',
    'data.client.full_name',
    'data.client.name',
    'customer.fullname',
    'customer.name',
    'client_name',
    'customer_name',
    'name'
  ], '');

  return titleCaseName(firstName) || titleCaseName(fullName) || 'Клиент';
}

function getOrderNumber(payload) {
  return String(pick(payload, [
    'metadata.order.name',
    'order.name',
    'data.order.name',

    'ro_api_order.number',
    'ro_api_order.data.number',
    'ro_api_order.name',
    'ro_api_order.data.name',
    'ro_api_order.order.number',
    'ro_api_order.order.name',
    'ro_api_order.data.order.number',
    'ro_api_order.data.order.name',

    'metadata.order.number',
    'order.number',
    'data.order.number',

    'metadata.order.id',
    'order.id',
    'order_id',
    'number',
    'id',
    'data.order.id',
    'data.id',
    'ro_api_order.id',
    'ro_api_order.data.id'
  ], 'Без номера')).trim();
}

function getOrderStatus(payload) {
  const statusId = getNewStatusId(payload);

  if (READY_STATUS_IDS.has(statusId)) return 'Готов к выдаче';
  if (CLOSED_STATUS_IDS.has(statusId)) return 'Выдан / закрыт';
  if (ACCEPTED_STATUS_IDS.has(statusId)) return 'Принят';

  return String(pick(payload, [
    'status.name',
    'status.title',
    'status',
    'new_status',
    'new_status.name',
    'order.status.name',
    'order.status.title',
    'order.status',
    'data.status.name',
    'data.status.title',
    'data.status',
    'data.order.status.name',
    'data.order.status.title',
    'metadata.new.name',
    'metadata.new.title',
    'metadata.new.status.name',
    'metadata.new.status.title',
    'ro_api_order.status.name',
    'ro_api_order.status.title',
    'ro_api_order.status',
    'ro_api_order.data.status.name',
    'ro_api_order.data.status.title',
    'ro_api_order.data.status'
  ], 'Статус изменён')).trim();
}

function parseMoneyValue(value) {
  if (value === null || value === undefined) return null;

  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return null;
    return value;
  }

  const text = String(value).trim();
  if (!text) return null;

  const normalized = text
    .replace(/\s/g, '')
    .replace(/[₸〒тгKZTkzt]/g, '')
    .replace(',', '.');

  if (!/^-?\d+(\.\d+)?$/.test(normalized)) return null;

  const num = Number(normalized);
  if (!Number.isFinite(num)) return null;

  return num;
}

function formatMoney(num) {
  if (num === null || num === undefined || !Number.isFinite(num)) return '';

  const rounded = Math.round(num * 100) / 100;

  if (Math.abs(rounded - Math.round(rounded)) < 0.001) {
    return String(Math.round(rounded));
  }

  return rounded.toFixed(2).replace('.', ',');
}

function getOrderAmountDebug(payload) {
  const paths = [
    'ro_api_order.total',
    'ro_api_order.data.total',
    'ro_api_order.order.total',
    'ro_api_order.data.order.total',

    'ro_api_order.sum',
    'ro_api_order.data.sum',
    'ro_api_order.order.sum',
    'ro_api_order.data.order.sum',

    'ro_api_order.amount',
    'ro_api_order.data.amount',
    'ro_api_order.order.amount',
    'ro_api_order.data.order.amount',

    'ro_api_order.payed',
    'ro_api_order.data.payed',
    'ro_api_order.order.payed',
    'ro_api_order.data.order.payed',

    'amount',
    'total',
    'price',
    'sum',
    'order.total',
    'order.amount',
    'order.price',
    'order.sum',
    'metadata.order.total',
    'metadata.order.amount',
    'metadata.order.price',
    'metadata.order.sum',
    'data.total',
    'data.amount',
    'data.price',
    'data.sum'
  ];

  const candidates = [];

  for (const path of paths) {
    const raw = pick(payload, [path], '');
    const parsed = parseMoneyValue(raw);

    if (parsed !== null && parsed >= 0 && parsed < 100000000) {
      candidates.push({
        path,
        value: parsed,
        raw
      });
    }
  }

  const best = candidates.find((item) => item.value > 0) || candidates[0];

  if (best) {
    return {
      value: best.value,
      formatted: `${formatMoney(best.value)} ₸`,
      source: best.path,
      raw: best.raw,
      candidates
    };
  }

  return {
    value: null,
    formatted: 'уточняется',
    source: 'not_found',
    raw: null,
    candidates
  };
}

function extractUrlsFromText(value) {
  const text = String(value || '');
  const matches = text.match(/https?:\/\/[^\s"'<>]+/g) || [];

  return matches.map((url) => url.replace(/[),.;]+$/g, ''));
}

function receiptUrlLooksGood(url) {
  return /roapp\.page|cabinet\.kofd\.kz|kofd|ofd|webkassa|receipt|check|cheque|fiscal|pdf|consumer|qr/i.test(String(url || ''));
}

function scoreReceiptUrl(url) {
  const value = String(url || '');

  if (/cabinet\.kofd\.kz\/consumer/i.test(value)) return 0;
  if (/kofd/i.test(value)) return 1;
  if (/webkassa/i.test(value)) return 2;
  if (/fiscal|receipt|check|cheque/i.test(value)) return 3;
  if (/roapp\.page/i.test(value)) return 4;

  return 10;
}

function findReceiptCandidates(obj) {
  const candidates = [];
  const seen = new Set();

  function addCandidate(url, path, raw) {
    if (!url) return;

    const cleanUrl = String(url).replace(/[),.;]+$/g, '');

    if (!/^https?:\/\//i.test(cleanUrl)) return;
    if (!receiptUrlLooksGood(cleanUrl)) return;
    if (seen.has(cleanUrl)) return;

    seen.add(cleanUrl);

    candidates.push({
      url: cleanUrl,
      path,
      raw: String(raw || '').slice(0, 500)
    });
  }

  function walk(node, path = '', depth = 0) {
    if (node == null || depth > 12) return;

    if (typeof node === 'string' || typeof node === 'number') {
      for (const url of extractUrlsFromText(node)) {
        addCandidate(url, path, node);
      }

      return;
    }

    if (Array.isArray(node)) {
      node.forEach((item, i) => walk(item, `${path}.${i}`, depth + 1));
      return;
    }

    if (typeof node === 'object') {
      for (const [key, value] of Object.entries(node)) {
        walk(value, path ? `${path}.${key}` : key, depth + 1);
      }
    }
  }

  walk(obj);

  return candidates.sort((a, b) => {
    const scoreA = scoreReceiptUrl(a.url);
    const scoreB = scoreReceiptUrl(b.url);

    if (scoreA !== scoreB) return scoreA - scoreB;
    return String(a.path).localeCompare(String(b.path));
  });
}

async function findReceiptLink(orderId, baseOrderData = null) {
  const sourcesChecked = [];
  const candidates = [];

  function addSource(path, ok, error = '') {
    sourcesChecked.push({
      path,
      ok,
      error: String(error || '').slice(0, 300)
    });
  }

  function addCandidatesFrom(sourcePath, data) {
    const found = findReceiptCandidates(data);

    for (const item of found) {
      candidates.push({
        ...item,
        sourcePath
      });
    }
  }

  if (baseOrderData) {
    addSource('base_ro_api_order', true);
    addCandidatesFrom('base_ro_api_order', baseOrderData);
  }

  try {
    const publicUrlData = await fetchRoOrderPublicUrl(orderId);

    addSource(`/orders/${orderId}/public-url`, true);
    addCandidatesFrom(`/orders/${orderId}/public-url`, publicUrlData);
  } catch (err) {
    addSource(`/orders/${orderId}/public-url`, false, `${err.status || ''} ${err.message || ''}`);
    log('RO API public-url failed:', err.status || '', err.message);
  }

  if (!baseOrderData) {
    try {
      const orderData = await fetchRoOrder(orderId);

      addSource(`/orders/${orderId}`, true);
      addCandidatesFrom(`/orders/${orderId}`, orderData);
    } catch (err) {
      addSource(`/orders/${orderId}`, false, `${err.status || ''} ${err.message || ''}`);
      log('RO API order fallback for receipt failed:', err.status || '', err.message);
    }
  }

  const unique = [];
  const seen = new Set();

  for (const item of candidates) {
    if (seen.has(item.url)) continue;

    seen.add(item.url);
    unique.push(item);
  }

  unique.sort((a, b) => {
    const scoreA = scoreReceiptUrl(a.url);
    const scoreB = scoreReceiptUrl(b.url);

    if (scoreA !== scoreB) return scoreA - scoreB;
    return String(a.sourcePath).localeCompare(String(b.sourcePath));
  });

  return {
    link: unique[0]?.url || '',
    candidates: unique,
    sourcesChecked
  };
}

function scheduleReceiptSearch({ orderId, clientPhone, clientName, orderNumber, baseOrderData }) {
  if (!RECEIPT_SEARCH_ENABLED) {
    log('Receipt search disabled by RECEIPT_SEARCH_ENABLED=false');
    return;
  }

  if (!orderId) {
    log('Receipt search skipped: no orderId');
    return;
  }

  const key = String(orderId);

  if (receiptSent.has(key)) {
    log('Receipt already sent, skip:', key);
    return;
  }

  if (receiptJobs.has(key)) {
    log('Receipt search already scheduled, skip:', key);
    return;
  }

  receiptJobs.add(key);

  log('Receipt search scheduled:', {
    orderId: key,
    orderNumber,
    retries: RECEIPT_RETRY_MS
  });

  RECEIPT_RETRY_MS.forEach((ms, index) => {
    const timer = setTimeout(() => {
      attemptReceiptSend({
        orderId: key,
        clientPhone,
        clientName,
        orderNumber,
        baseOrderData,
        attempt: index + 1,
        isLast: index === RECEIPT_RETRY_MS.length - 1
      }).catch((err) => {
        console.error('Receipt attempt fatal error:', err.data || err.message || err);
      });
    }, ms);

    if (timer.unref) timer.unref();
  });
}

async function attemptReceiptSend({ orderId, clientPhone, clientName, orderNumber, baseOrderData, attempt, isLast }) {
  if (receiptSent.has(orderId)) return;

  log('Receipt search attempt:', {
    orderId,
    orderNumber,
    attempt
  });

  const result = await findReceiptLink(orderId, baseOrderData);

  if (!result.link) {
    log('Receipt link not found yet:', {
      orderId,
      orderNumber,
      attempt,
      candidates: result.candidates.length,
      sourcesChecked: result.sourcesChecked
    });

    if (isLast) {
      receiptJobs.delete(orderId);
      log('Receipt search finished without link:', {
        orderId,
        orderNumber
      });
    }

    return;
  }

  log('Receipt link found:', {
    orderId,
    orderNumber,
    receiptUrl: result.link,
    attempt
  });

  try {
    await sendTemplate(clientPhone, RECEIPT_TEMPLATE_NAME, [
      clientName,
      orderNumber,
      result.link
    ]);

    receiptSent.add(orderId);
    receiptJobs.delete(orderId);

    log('order_closed_receipt sent', clientPhone, {
      clientName,
      orderNumber,
      orderId,
      receiptUrl: result.link
    });
  } catch (err) {
    console.error('order_closed_receipt send error:', err.data || err.message || err);

    if (isLast) {
      receiptJobs.delete(orderId);
      log('Receipt link found but template was not sent after last attempt:', {
        orderId,
        orderNumber,
        receiptUrl: result.link
      });
    }
  }
}

function isLeadCreated(payload) {
  const event = getEventName(payload);
  const objectType = String(pick(payload, ['context.object_type'], '')).toLowerCase();

  return (
    event.includes('lead.created') ||
    event.includes('lead') ||
    event.includes('appeal') ||
    event.includes('request') ||
    objectType === 'lead'
  );
}

function isRepairRequestCreated(payload) {
  const event = getEventName(payload);
  const text = JSON.stringify(payload || {}).toLowerCase();

  return (
    isLeadCreated(payload) ||
    text.includes('обращен') ||
    text.includes('заявк') ||
    text.includes('lead') ||
    text.includes('appeal')
  ) && (
    event.includes('create') ||
    event.includes('created') ||
    event.includes('new') ||
    event.includes('add') ||
    text.includes('created') ||
    text.includes('создан')
  );
}

function isOrderCreated(payload) {
  const event = getEventName(payload);
  const objectType = String(pick(payload, ['context.object_type'], '')).toLowerCase();

  return (
    event.includes('order.created') ||
    (event.includes('order') && event.includes('created')) ||
    (objectType === 'order' && event.includes('created'))
  );
}

function isOrderReady(payload) {
  const statusId = getNewStatusId(payload);
  const status = getOrderStatus(payload).toLowerCase();

  return (
    READY_STATUS_IDS.has(statusId) ||
    status.includes('готов') ||
    status.includes('выдач')
  );
}

function isOrderClosed(payload) {
  const statusId = getNewStatusId(payload);
  const status = getOrderStatus(payload).toLowerCase();

  return (
    CLOSED_STATUS_IDS.has(statusId) ||
    status.includes('закрыт') ||
    status.includes('выдан') ||
    status.includes('completed') ||
    status.includes('closed')
  );
}

function isOrderAccepted(payload) {
  const statusId = getNewStatusId(payload);
  const event = getEventName(payload);
  const status = getOrderStatus(payload).toLowerCase();

  return (
    isOrderCreated(payload) ||
    ACCEPTED_STATUS_IDS.has(statusId) ||
    (event.includes('order') && event.includes('create')) ||
    status.includes('принят') ||
    status.includes('нов')
  );
}

function isOrderRelatedPayload(payload) {
  const event = getEventName(payload);
  const objectType = String(pick(payload, ['context.object_type'], '')).toLowerCase();

  return (
    event.includes('order') ||
    event.includes('status') ||
    objectType === 'order' ||
    JSON.stringify(payload || {}).toLowerCase().includes('"order"')
  );
}

function collectFormFields(obj) {
  const fields = [];

  function primitive(value) {
    return typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean';
  }

  function add(label, value, path) {
    if (value === undefined || value === null) return;

    const text = String(value).trim();
    if (!text) return;

    fields.push({
      label: String(label || '').trim(),
      value: text,
      path: String(path || '').trim()
    });
  }

  function walk(node, path = '', depth = 0) {
    if (node == null || depth > 12) return;

    if (primitive(node)) {
      const last = path.split('.').pop() || path;
      add(last, node, path);
      return;
    }

    if (Array.isArray(node)) {
      node.forEach((item, i) => walk(item, `${path}.${i}`, depth + 1));
      return;
    }

    if (typeof node === 'object') {
      const label = pick(node, [
        'label',
        'name',
        'title',
        'key',
        'field',
        'question',
        'caption'
      ], '');

      const value = pick(node, [
        'value',
        'text',
        'answer',
        'content',
        'val'
      ], '');

      if (label && value) {
        add(label, value, path);
      }

      for (const [key, val] of Object.entries(node)) {
        walk(val, path ? `${path}.${key}` : key, depth + 1);
      }
    }
  }

  walk(obj);

  return fields;
}

function findFieldValue(payload, regexList) {
  const fields = collectFormFields(payload);

  for (const regex of regexList) {
    const found = fields.find((field) => {
      const haystack = `${field.label} ${field.path}`;
      return regex.test(haystack);
    });

    if (found?.value) return found.value;
  }

  return '';
}

function findFormTextInPayload(payload) {
  const fields = collectFormFields(payload);

  const found = fields.find((field) => (
    /Тип устройства\s*:/i.test(field.value) ||
    /Неисправность\s*:/i.test(field.value) ||
    /Комментарий\s*:/i.test(field.value) ||
    /FormID\s*:/i.test(field.value)
  ));

  return found?.value || '';
}

function extractRepairSubjectFromText(value) {
  const text = String(value || '').trim();
  if (!text) return '';

  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const device = lines.find((line) => /^Тип устройства\s*:/i.test(line));
  const problem = lines.find((line) => /^Неисправность\s*:/i.test(line));
  const comment = lines.find((line) => /^Комментарий\s*:/i.test(line));

  const useful = [device, problem, comment].filter(Boolean);

  if (useful.length) {
    return useful.join('\n');
  }

  const cleaned = lines
    .filter((line) => !/^Форма\s*:/i.test(line))
    .filter((line) => !/^FormID\s*:/i.test(line))
    .filter((line) => !/^Страница\s*:/i.test(line))
    .filter((line) => !/^Прикрепить фото\s*:/i.test(line))
    .join('\n')
    .trim();

  return cleaned || text;
}

function getSiteClientName(payload) {
  const direct = pick(payload, [
    'name',
    'clientName',
    'client_name',
    'fullname',
    'full_name',
    'first_name',
    'client.name',
    'client.fullname',
    'client.full_name',
    'data.name',
    'data.clientName',
    'data.client_name',
    'fields.name',
    'fields.clientName'
  ], '');

  const fromField = findFieldValue(payload, [
    /(^|\.|\s)(имя|ваше имя|клиент|client_name|clientName|fullname|full_name)($|\.|\s)/i
  ]);

  return titleCaseName(direct) || titleCaseName(fromField) || 'Клиент';
}

function getSiteRepairSubject(payload) {
  const formText = findFormTextInPayload(payload);

  if (formText) {
    const extracted = extractRepairSubjectFromText(formText);
    if (extracted) return extracted;
  }

  const device = findFieldValue(payload, [
    /тип.*устрой/i,
    /устройство/i,
    /инструмент/i,
    /device/i,
    /tool/i
  ]);

  const problem = findFieldValue(payload, [
    /неисправ/i,
    /проблем/i,
    /problem/i,
    /issue/i,
    /malfunction/i
  ]);

  const comment = findFieldValue(payload, [
    /коммент/i,
    /сообщ/i,
    /описан/i,
    /comment/i,
    /message/i,
    /description/i
  ]);

  const lines = [];

  if (device) lines.push(`Тип устройства: ${device}`);
  if (problem) lines.push(`Неисправность: ${problem}`);
  if (comment) lines.push(`Комментарий: ${comment}`);

  if (lines.length) return lines.join('\n');

  const direct = pick(payload, [
    'comment',
    'message',
    'description',
    'text',
    'problem',
    'data.comment',
    'data.message',
    'data.description',
    'data.text'
  ], '');

  if (direct) return extractRepairSubjectFromText(direct);

  return 'Новая заявка с сайта';
}

function getRoLeadClientName(payload) {
  return titleCaseName(pick(payload, [
    'metadata.client.fullname',
    'metadata.client.full_name',
    'metadata.client.name',
    'metadata.client.first_name',
    'client.fullname',
    'client.full_name',
    'client.name',
    'client.first_name',
    'data.client.fullname',
    'data.client.full_name',
    'data.client.name',
    'data.client.first_name'
  ], ''));
}

function getRoLeadSubject(payload) {
  const text = findFormTextInPayload(payload);

  if (text) {
    const extracted = extractRepairSubjectFromText(text);
    if (extracted) return extracted;
  }

  const direct = pick(payload, [
    'metadata.lead.comment',
    'metadata.lead.description',
    'metadata.lead.text',
    'metadata.lead.message',
    'lead.comment',
    'lead.description',
    'lead.text',
    'lead.message',
    'data.lead.comment',
    'data.lead.description',
    'data.lead.text',
    'data.lead.message',
    'comment',
    'description',
    'message',
    'text'
  ], '');

  if (direct && !/^\d+$/.test(String(direct).trim())) {
    return extractRepairSubjectFromText(direct);
  }

  const leadName = String(pick(payload, [
    'metadata.lead.name',
    'lead.name',
    'data.lead.name'
  ], '')).trim();

  if (leadName && !/^\d+$/.test(leadName)) {
    return leadName;
  }

  return '';
}

async function sendRepairRequestAlert({ clientName, clientPhone, subject }) {
  const normalizedPhone = normalizePhone(clientPhone);
  const waLink = normalizedPhone ? `https://wa.me/${normalizedPhone}` : 'Телефон не найден';

  await sendTemplate(MANAGER_WHATSAPP, 'new_repair_request_alert', [
    clientName || 'Клиент',
    normalizedPhone ? prettyPhone(normalizedPhone) : 'не указан',
    subject || 'Новая заявка с сайта',
    waLink
  ]);

  log('Repair request alert sent to manager', {
    clientName,
    clientPhone: normalizedPhone || '',
    hasPhone: Boolean(normalizedPhone)
  });
}

async function handleSiteRepairRequest(payload) {
  const clientName = getSiteClientName(payload);
  const clientPhone = findFirstPhone(payload);
  const subject = getSiteRepairSubject(payload);

  log('Site repair request received:', {
    clientName,
    clientPhone: clientPhone || '',
    subject
  });

  await sendRepairRequestAlert({
    clientName,
    clientPhone,
    subject
  });

  return {
    ok: true,
    clientName,
    clientPhone,
    subject
  };
}

async function handleRoRepairRequest(payload) {
  const clientName = getRoLeadClientName(payload) || getClientName(payload);
  const clientPhone = findFirstPhone(payload);
  const subject = getRoLeadSubject(payload);

  if (!clientPhone && !subject) {
    log('RO lead has no phone and no useful comment. Skipped. Use /site-repair-request for full website form data.');
    log('RO lead payload preview:', compactPayloadPreview(payload));
    return {
      ok: true,
      skipped: true,
      reason: 'no phone and no useful comment'
    };
  }

  await sendRepairRequestAlert({
    clientName,
    clientPhone,
    subject: subject || 'Новое обращение в РемОнлайн'
  });

  return {
    ok: true,
    skipped: false,
    clientName,
    clientPhone,
    subject
  };
}

async function metaRequest(path, body) {
  if (!META_ACCESS_TOKEN) throw new Error('META_ACCESS_TOKEN is empty');

  const url = `${GRAPH_BASE}/${path.replace(/^\//, '')}`;

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${META_ACCESS_TOKEN}`,
      'Content-Type': 'application/json; charset=utf-8'
    },
    body: JSON.stringify(body)
  });

  const text = await res.text();
  let data;

  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }

  if (!res.ok) {
    const err = new Error(data?.error?.message || `Meta API error ${res.status}`);
    err.status = res.status;
    err.data = data;
    throw err;
  }

  return data;
}

async function sendWhatsAppText(to, text) {
  const body = {
    messaging_product: 'whatsapp',
    to: normalizePhone(to),
    type: 'text',
    text: {
      preview_url: true,
      body: text
    }
  };

  return metaRequest(`${WHATSAPP_PHONE_NUMBER_ID}/messages`, body);
}

async function sendTemplate(to, name, parameters = []) {
  const bodyParams = parameters.map((text) => ({
    type: 'text',
    text: String(text ?? '')
  }));

  const body = {
    messaging_product: 'whatsapp',
    to: normalizePhone(to),
    type: 'template',
    template: {
      name,
      language: {
        code: WHATSAPP_TEMPLATE_LANG
      },
      components: bodyParams.length
        ? [
            {
              type: 'body',
              parameters: bodyParams
            }
          ]
        : []
    }
  };

  return metaRequest(`${WHATSAPP_PHONE_NUMBER_ID}/messages`, body);
}

function autoReplyText() {
  return [
    'Здравствуйте. Это автоматический номер MasterProServis.kz для уведомлений по заказам.',
    '',
    'Пожалуйста, не пишите сюда. Для связи с менеджером напишите в основной WhatsApp:',
    `https://wa.me/${MANAGER_WHATSAPP}`
  ].join('\n');
}

async function handleOrderEvent(payload) {
  const statusIdBeforeApi = getNewStatusId(payload);

  if (INTERNAL_STATUS_IDS.has(statusIdBeforeApi)) {
    log('Internal status, skip WhatsApp:', statusIdBeforeApi);
    return;
  }

  const enriched = await enrichRoOrderPayloadWithApi(payload);
  const fullPayload = enriched.payload;

  const clientPhone = enriched.phone || findFirstPhone(fullPayload);

  if (!clientPhone) {
    log('No client phone found, skip WhatsApp template');
    log('RemOnline payload preview:', compactPayloadPreview(fullPayload));
    return;
  }

  const clientName = getClientName(fullPayload);
  const orderNumber = getOrderNumber(fullPayload);
  const status = getOrderStatus(fullPayload);
  const statusId = statusIdBeforeApi || getNewStatusId(fullPayload);
  const amountDebug = getOrderAmountDebug(fullPayload);
  const orderId = getRoOrderId(payload) || getRoOrderId(fullPayload);

  log('Order debug:', {
    eventName: getEventName(payload),
    statusId: statusId || null,
    orderId: orderId || null,
    orderNumber,
    clientName,
    clientPhone,
    phoneSource: enriched.source,
    status,
    amount: amountDebug.formatted,
    amountSource: amountDebug.source
  });

  if (isOrderReady(fullPayload)) {
    await sendTemplate(clientPhone, 'order_ready', [
      clientName,
      orderNumber,
      amountDebug.formatted
    ]);

    return log('order_ready sent', clientPhone, {
      clientName,
      orderNumber,
      statusId,
      amount: amountDebug.formatted,
      amountSource: amountDebug.source
    });
  }

  if (isOrderClosed(fullPayload)) {
    scheduleReceiptSearch({
      orderId,
      clientPhone,
      clientName,
      orderNumber,
      baseOrderData: fullPayload.ro_api_order
    });

    try {
      await sendTemplate(clientPhone, 'order_review_request', [
        clientName
      ]);

      log('order_review_request sent', clientPhone, {
        clientName,
        orderNumber,
        statusId
      });
    } catch (err) {
      console.error('order_review_request send error:', err.data || err.message || err);
    }

    return;
  }

  if (isOrderAccepted(fullPayload)) {
    await sendTemplate(clientPhone, 'order_accepted', [
      clientName,
      orderNumber
    ]);

    return log('order_accepted sent', clientPhone, {
      clientName,
      orderNumber,
      statusId
    });
  }

  log('Unhandled/non-customer status, skip WhatsApp:', {
    statusId: statusId || null,
    orderNumber,
    status
  });
}

function checkRoSecret(req) {
  if (!RO_SECRET) return true;

  const provided =
    req.query.secret ||
    req.headers['x-ro-secret'] ||
    req.headers['x-webhook-secret'] ||
    req.headers['x-remonline-secret'];

  return String(provided || '') === RO_SECRET;
}

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    service: 'wa-masterproservis',
    time: new Date().toISOString(),
    phoneNumberId: WHATSAPP_PHONE_NUMBER_ID,
    wabaId: WABA_ID,
    roApiConfigured: Boolean(REMONLINE_API_KEY),
    templateLang: WHATSAPP_TEMPLATE_LANG,
    readyStatusIds: [...READY_STATUS_IDS],
    closedStatusIds: [...CLOSED_STATUS_IDS],
    acceptedStatusIds: [...ACCEPTED_STATUS_IDS],
    internalStatusIds: [...INTERNAL_STATUS_IDS],
    receiptTemplateName: RECEIPT_TEMPLATE_NAME,
    receiptSearchEnabled: RECEIPT_SEARCH_ENABLED,
    receiptRetryMs: RECEIPT_RETRY_MS,
    siteRepairRequestWebhook: '/site-repair-request'
  });
});

app.get('/wa/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  if (mode === 'subscribe' && token === META_VERIFY_TOKEN) {
    log('Meta webhook verified');
    return res.status(200).send(challenge);
  }

  return res.sendStatus(403);
});

app.post('/wa/webhook', async (req, res) => {
  res.sendStatus(200);

  try {
    const entries = req.body?.entry || [];

    for (const entry of entries) {
      for (const change of entry.changes || []) {
        const value = change.value || {};

        for (const msg of value.messages || []) {
          const from = normalizePhone(msg.from);
          if (!from) continue;

          log('Incoming WhatsApp message from', from, 'type', msg.type);
          await sendWhatsAppText(from, autoReplyText());
          log('Auto-reply sent to', from);
        }
      }
    }
  } catch (err) {
    console.error('WA webhook handling error:', err.data || err.message || err);
  }
});

app.post('/site-repair-request', async (req, res) => {
  try {
    const result = await handleSiteRepairRequest(req.body || {});

    res.json({
      ok: true,
      received: true,
      ...result
    });
  } catch (err) {
    console.error('site-repair-request error:', err.data || err.message || err);

    res.status(500).json({
      ok: false,
      error: err.message,
      meta: err.data
    });
  }
});

app.get('/site-repair-request', (req, res) => {
  res.json({
    ok: true,
    endpoint: '/site-repair-request',
    method: 'POST',
    message: 'Flexbe webhook should send form data here.'
  });
});

app.post('/ro/webhook', async (req, res) => {
  if (!checkRoSecret(req)) {
    return res.status(401).json({
      ok: false,
      error: 'Bad secret'
    });
  }

  res.json({
    ok: true,
    received: true
  });

  const payload = req.body || {};

  try {
    const eventName = getEventName(payload) || 'unknown';
    log('RemOnline webhook event:', eventName);

    if (isRepairRequestCreated(payload)) {
      return await handleRoRepairRequest(payload);
    }

    if (isOrderCreated(payload) || isOrderRelatedPayload(payload) || isOrderAccepted(payload)) {
      return await handleOrderEvent(payload);
    }

    log('No matching RemOnline rule, skipped');
    log('RemOnline payload preview:', compactPayloadPreview(payload));
  } catch (err) {
    console.error('RO webhook handling error:', err.data || err.message || err);
  }
});

app.get('/test-send', async (req, res) => {
  try {
    const to = normalizePhone(req.query.to || MANAGER_WHATSAPP);
    const text = String(req.query.text || 'Тестовый ответ от MasterProServis.kz. API-номер работает.');
    const data = await sendWhatsAppText(to, text);

    res.json({
      ok: true,
      data
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err.message,
      meta: err.data
    });
  }
});

function defaultTemplateParams(template) {
  const params = {
    new_repair_request_alert: [
      'Павел',
      '+77076669955',
      'Тип устройства: Перфоратор\nНеисправность: Не реагирует на кнопку\nКомментарий: тест',
      'https://wa.me/77076669955'
    ],
    order_accepted: [
      'Павел',
      'B4582'
    ],
    order_ready: [
      'Павел',
      'B4582',
      '123 ₸'
    ],
    order_review_request: [
      'Павел'
    ],
    order_closed_receipt: [
      'Павел',
      'B4582',
      'https://c13xs.roapp.page/w/test/'
    ]
  };

  return params[template] || ['Павел'];
}

app.get('/test-template', async (req, res) => {
  try {
    const to = normalizePhone(req.query.to || MANAGER_WHATSAPP);
    const template = String(req.query.template || 'order_ready');

    const manualParams = [];

    for (let i = 1; i <= 10; i += 1) {
      const value = req.query[`p${i}`];
      if (value !== undefined) manualParams.push(String(value));
    }

    const params = manualParams.length ? manualParams : defaultTemplateParams(template);
    const data = await sendTemplate(to, template, params);

    res.json({
      ok: true,
      template,
      params,
      data
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err.message,
      meta: err.data
    });
  }
});

app.get('/test-site-request', async (req, res) => {
  try {
    const payload = {
      name: req.query.name || 'Павел',
      phone: req.query.phone || '77076669955',
      device: req.query.device || 'Перфоратор',
      problem: req.query.problem || 'Не реагирует на кнопку',
      comment: req.query.comment || 'Тестовая заявка'
    };

    const result = await handleSiteRepairRequest(payload);

    res.json({
      ok: true,
      payload,
      result
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err.message,
      meta: err.data
    });
  }
});

app.get('/debug-order-amount', async (req, res) => {
  try {
    const orderId = String(req.query.orderId || '');

    if (!orderId) {
      return res.status(400).json({
        ok: false,
        error: 'orderId is required'
      });
    }

    const order = await fetchRoOrder(orderId);
    const amountDebug = getOrderAmountDebug({ ro_api_order: order });

    res.json({
      ok: true,
      orderId,
      amount: amountDebug.formatted,
      amountSource: amountDebug.source,
      candidates: amountDebug.candidates,
      order
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err.message,
      status: err.status,
      data: err.data
    });
  }
});

app.get('/debug-receipt', async (req, res) => {
  try {
    const orderId = String(req.query.orderId || '');

    if (!orderId) {
      return res.status(400).json({
        ok: false,
        error: 'orderId is required'
      });
    }

    const order = await fetchRoOrder(orderId);
    const result = await findReceiptLink(orderId, order);

    res.json({
      ok: true,
      orderId,
      receiptUrl: result.link,
      candidates: result.candidates,
      sourcesChecked: result.sourcesChecked
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err.message,
      status: err.status,
      data: err.data
    });
  }
});

app.get('/send-receipt', async (req, res) => {
  try {
    const orderId = String(req.query.orderId || '');
    const forcedTo = normalizePhone(req.query.to || '');

    if (!orderId) {
      return res.status(400).json({
        ok: false,
        error: 'orderId is required'
      });
    }

    const order = await fetchRoOrder(orderId);
    const payloadForOrder = { ro_api_order: order };
    const receipt = await findReceiptLink(orderId, order);

    if (!receipt.link) {
      return res.status(404).json({
        ok: false,
        error: 'Receipt link not found',
        orderId,
        candidates: receipt.candidates,
        sourcesChecked: receipt.sourcesChecked
      });
    }

    const clientPhone = forcedTo || findFirstPhone(order);

    if (!clientPhone) {
      return res.status(404).json({
        ok: false,
        error: 'Client phone not found',
        orderId,
        receiptUrl: receipt.link
      });
    }

    const clientName = getClientName(payloadForOrder);
    const orderNumber = getOrderNumber(payloadForOrder);

    const data = await sendTemplate(clientPhone, RECEIPT_TEMPLATE_NAME, [
      clientName,
      orderNumber,
      receipt.link
    ]);

    receiptSent.add(orderId);

    res.json({
      ok: true,
      orderId,
      to: clientPhone,
      clientName,
      orderNumber,
      receiptUrl: receipt.link,
      data
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err.message,
      meta: err.data,
      status: err.status,
      data: err.data
    });
  }
});

app.get('/debug-ro-path', async (req, res) => {
  try {
    const path = String(req.query.path || '').trim();

    if (!path) {
      return res.status(400).json({
        ok: false,
        error: 'path is required'
      });
    }

    const safePath = path.replace(/^\/+/, '');
    const data = await roApiGetRaw(`/${safePath}`);

    res.json({
      ok: true,
      path: `/${safePath}`,
      data
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err.message,
      status: err.status,
      data: err.data
    });
  }
});

app.use((req, res) => {
  res.status(404).json({
    ok: false,
    error: 'Not found'
  });
});

app.listen(PORT, () => {
  log(`wa-masterproservis listening on port ${PORT}`);
});
