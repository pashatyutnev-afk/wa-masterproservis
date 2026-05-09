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

const REMONLINE_API_KEY = process.env.REMONLINE_API_KEY || '';
const REMONLINE_API_BASE_URL = (process.env.REMONLINE_API_BASE_URL || 'https://api.roapp.io/v2').replace(/\/+$/, '');
const RO_SECRET = process.env.RO_SECRET || '';

const READY_STATUS_IDS = parseIdList(process.env.READY_STATUS_IDS || '363629');
const CLOSED_STATUS_IDS = parseIdList(process.env.CLOSED_STATUS_IDS || '363632');
const RECEIVED_STATUS_IDS = parseIdList(process.env.RECEIVED_STATUS_IDS || '');
const INTERNAL_STATUS_IDS = parseIdList(process.env.INTERNAL_STATUS_IDS || '3045928');

const TEMPLATE_SITE_REQUEST = process.env.TEMPLATE_SITE_REQUEST || 'new_repair_request_alert';
const TEMPLATE_ORDER_RECEIVED = process.env.TEMPLATE_ORDER_RECEIVED || 'order_received';
const TEMPLATE_ORDER_READY = process.env.TEMPLATE_ORDER_READY || 'order_ready';
const TEMPLATE_ORDER_CLOSED_REVIEW = process.env.TEMPLATE_ORDER_CLOSED_REVIEW || 'order_review_request';
const TEMPLATE_ORDER_RECEIPT = process.env.TEMPLATE_ORDER_RECEIPT || 'order_closed_receipt';

const RECEIPT_RETRY_MS = parseRetryList(process.env.RECEIPT_RETRY_MS || '0,10000,30000,60000,180000,600000');
const RECEIPT_SEARCH_ENABLED = String(process.env.RECEIPT_SEARCH_ENABLED || 'true').toLowerCase() !== 'false';

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
      .map((item) => item.trim())
      .filter(Boolean)
  );
}

function parseRetryList(value) {
  return String(value || '')
    .split(',')
    .map((item) => Number(item.trim()))
    .filter((item) => Number.isFinite(item) && item >= 0);
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

function cleanName(value) {
  const text = String(value || '').trim();

  if (!text) return '';
  if (looksLikePhone(text)) return '';
  if (looksLikeOrderNumber(text)) return '';
  if (/^(клиент|заявка|форма|обращение|новое обращение)$/i.test(text)) return '';

  return text;
}

function titleCaseName(value) {
  const text = cleanName(value);
  if (!text) return '';

  return text
    .split(/\s+/)
    .map((part) => (part ? part[0].toUpperCase() + part.slice(1) : part))
    .join(' ');
}

function findFirstPhone(obj) {
  const direct = pick(obj, [
    'phone',
    'mobile',
    'telephone',
    'phone_number',
    'phoneNumber',
    'whatsapp',
    'client_phone',
    'customer_phone',

    'client.phone',
    'client.mobile',
    'client.phone_number',
    'client.phoneNumber',
    'client.whatsapp',
    'client.phone.0',
    'client.phones.0.phone',
    'client.phones.0.number',
    'client.phones.0.value',

    'data.phone',
    'data.mobile',
    'data.phone_number',
    'data.phoneNumber',
    'data.client.phone',
    'data.client.mobile',
    'data.client.phone_number',
    'data.client.phoneNumber',
    'data.client.phone.0',
    'data.client.phones.0.phone',
    'data.client.phones.0.number',
    'data.client.phones.0.value',
    'data.form_data.phone.value',
    'form_data.phone.value',

    'metadata.client.phone',
    'metadata.client.mobile',
    'metadata.client.phone_number',
    'metadata.client.phoneNumber',
    'metadata.client.phone.0',
    'metadata.client.phones.0.phone',
    'metadata.client.phones.0.number',
    'metadata.client.phones.0.value',

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
    'ro_api_order.data.client.phones.0.value'
  ]);

  const normalizedDirect = looksLikePhone(direct);
  if (normalizedDirect) return normalizedDirect;

  const foundPhones = [];
  const seen = new Set();

  function walk(node, path = '', depth = 0) {
    if (node == null || depth > 12) return;

    if (typeof node === 'string' || typeof node === 'number') {
      const keyLooksPhone = /phone|mobile|tel|telephone|whatsapp|wa|номер|телефон|домашний/i.test(path);
      const phone = looksLikePhone(node);

      if (keyLooksPhone && phone && !seen.has(phone)) {
        seen.add(phone);
        foundPhones.push(phone);
      }

      return;
    }

    if (Array.isArray(node)) {
      node.forEach((item, index) => walk(item, `${path}.${index}`, depth + 1));
      return;
    }

    if (typeof node === 'object') {
      for (const [key, value] of Object.entries(node)) {
        walk(value, path ? `${path}.${key}` : key, depth + 1);
      }
    }
  }

  walk(obj);

  return foundPhones[0] || '';
}

function findFirstId(obj, paths) {
  const direct = pick(obj, paths, '');
  return direct !== '' ? String(direct) : '';
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

async function fetchRoOrder(orderId) {
  if (!orderId) return null;

  const data = await roApiGetRaw(`/orders/${orderId}`);
  log('RO API order success:', `/orders/${orderId}`);
  return unwrapRoResponse(data);
}

async function fetchRoOrderPublicUrl(orderId) {
  if (!orderId) return null;
  return roApiGetRaw(`/orders/${orderId}/public-url`);
}

async function enrichRoOrderPayload(payload) {
  const orderId = getRoOrderId(payload);

  if (orderId) {
    try {
      const order = await fetchRoOrder(orderId);
      const phone = findFirstPhone(order);

      return {
        payload: { ...payload, ro_api_order: order },
        phone,
        source: phone ? 'ro_api_order' : 'ro_api_order_no_phone'
      };
    } catch (err) {
      log('RO API order lookup failed:', err.status || '', err.message);
    }
  }

  const fallbackPhone = findFirstPhone(payload);

  return {
    payload,
    phone: fallbackPhone,
    source: fallbackPhone ? 'webhook_fallback' : 'not_found'
  };
}

function getClientName(payload) {
  const firstName = pick(payload, [
    'ro_api_order.client.first_name',
    'ro_api_order.data.client.first_name',
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
  if (RECEIVED_STATUS_IDS.has(statusId)) return 'Принят';

  return String(pick(payload, [
    'status.name',
    'status.title',
    'status',
    'new_status.name',
    'order.status.name',
    'order.status.title',
    'data.status.name',
    'data.status.title',
    'data.order.status.name',
    'data.order.status.title',
    'metadata.new.name',
    'metadata.new.title',
    'metadata.new.status.name',
    'metadata.new.status.title',
    'ro_api_order.status.name',
    'ro_api_order.status.title',
    'ro_api_order.data.status.name',
    'ro_api_order.data.status.title'
  ], 'Статус изменён')).trim();
}

function parseMoneyValue(value) {
  if (value === null || value === undefined) return null;

  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }

  const text = String(value).trim();
  if (!text) return null;

  const normalized = text
    .replace(/\s/g, '')
    .replace(/[₸〒тгKZTkzt]/g, '')
    .replace(',', '.');

  if (!/^-?\d+(\.\d+)?$/.test(normalized)) return null;

  const num = Number(normalized);
  return Number.isFinite(num) ? num : null;
}

function formatMoney(num) {
  if (num === null || num === undefined || !Number.isFinite(num)) return '';

  const rounded = Math.round(num * 100) / 100;

  if (Math.abs(rounded - Math.round(rounded)) < 0.001) {
    return String(Math.round(rounded));
  }

  return rounded.toFixed(2).replace('.', ',');
}

function getOrderAmount(payload) {
  const paths = [
    'ro_api_order.total',
    'ro_api_order.data.total',
    'ro_api_order.order.total',
    'ro_api_order.data.order.total',
    'ro_api_order.sum',
    'ro_api_order.data.sum',
    'ro_api_order.amount',
    'ro_api_order.data.amount',
    'ro_api_order.payed',
    'ro_api_order.data.payed',
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
    'data.total',
    'data.amount'
  ];

  const candidates = [];

  for (const path of paths) {
    const raw = pick(payload, [path], '');
    const parsed = parseMoneyValue(raw);

    if (parsed !== null && parsed >= 0 && parsed < 100000000) {
      candidates.push({ path, value: parsed, raw });
    }
  }

  const best = candidates.find((item) => item.value > 0) || candidates[0];

  return {
    formatted: best ? `${formatMoney(best.value)} ₸` : 'уточняется',
    source: best?.path || 'not_found',
    candidates
  };
}

function extractUrlsFromText(value) {
  return String(value || '').match(/https?:\/\/[^\s"'<>]+/g) || [];
}

function receiptUrlLooksGood(url) {
  return /roapp\.page|cabinet\.kofd\.kz|kofd|ofd|webkassa|receipt|check|cheque|fiscal|pdf|consumer|qr/i.test(String(url || ''));
}

function findReceiptCandidates(obj) {
  const candidates = [];
  const seen = new Set();

  function walk(node, path = '', depth = 0) {
    if (node == null || depth > 12) return;

    if (typeof node === 'string' || typeof node === 'number') {
      for (const url of extractUrlsFromText(node)) {
        const cleanUrl = url.replace(/[),.;]+$/g, '');

        if (receiptUrlLooksGood(cleanUrl) && !seen.has(cleanUrl)) {
          seen.add(cleanUrl);
          candidates.push({ url: cleanUrl, path });
        }
      }

      return;
    }

    if (Array.isArray(node)) {
      node.forEach((item, index) => walk(item, `${path}.${index}`, depth + 1));
      return;
    }

    if (typeof node === 'object') {
      for (const [key, value] of Object.entries(node)) {
        walk(value, path ? `${path}.${key}` : key, depth + 1);
      }
    }
  }

  walk(obj);

  candidates.sort((a, b) => {
    const score = (item) => {
      if (/cabinet\.kofd\.kz\/consumer/i.test(item.url)) return 0;
      if (/kofd/i.test(item.url)) return 1;
      if (/webkassa/i.test(item.url)) return 2;
      if (/receipt|check|cheque|fiscal/i.test(item.url)) return 3;
      if (/roapp\.page/i.test(item.url)) return 4;
      return 10;
    };

    return score(a) - score(b);
  });

  return candidates;
}

async function findReceiptLink(orderId, orderData = null) {
  const candidates = [];
  const sourcesChecked = [];

  if (orderData) {
    sourcesChecked.push({ path: 'order', ok: true });
    candidates.push(...findReceiptCandidates(orderData).map((item) => ({ ...item, sourcePath: 'order' })));
  }

  try {
    const publicUrlData = await fetchRoOrderPublicUrl(orderId);
    sourcesChecked.push({ path: `/orders/${orderId}/public-url`, ok: true });
    candidates.push(...findReceiptCandidates(publicUrlData).map((item) => ({ ...item, sourcePath: `/orders/${orderId}/public-url` })));
  } catch (err) {
    sourcesChecked.push({ path: `/orders/${orderId}/public-url`, ok: false, error: `${err.status || ''} ${err.message || ''}` });
  }

  const unique = [];
  const seen = new Set();

  for (const item of candidates) {
    if (seen.has(item.url)) continue;
    seen.add(item.url);
    unique.push(item);
  }

  return {
    link: unique[0]?.url || '',
    candidates: unique,
    sourcesChecked
  };
}

function scheduleReceiptSearch({ orderId, clientPhone, clientName, orderNumber, orderData }) {
  if (!RECEIPT_SEARCH_ENABLED || !orderId) return;

  const key = String(orderId);

  if (receiptSent.has(key) || receiptJobs.has(key)) return;

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
        orderData,
        attempt: index + 1,
        isLast: index === RECEIPT_RETRY_MS.length - 1
      }).catch((err) => {
        console.error('Receipt attempt fatal error:', err.data || err.message || err);
      });
    }, ms);

    if (timer.unref) timer.unref();
  });
}

async function attemptReceiptSend({ orderId, clientPhone, clientName, orderNumber, orderData, attempt, isLast }) {
  if (receiptSent.has(orderId)) return;

  const result = await findReceiptLink(orderId, orderData);

  if (!result.link) {
    log('Receipt link not found yet:', { orderId, orderNumber, attempt });

    if (isLast) {
      receiptJobs.delete(orderId);
      log('Receipt search finished without link:', { orderId, orderNumber });
    }

    return;
  }

  try {
    await sendTemplate(clientPhone, TEMPLATE_ORDER_RECEIPT, [
      clientName,
      orderNumber,
      result.link
    ]);

    receiptSent.add(orderId);
    receiptJobs.delete(orderId);

    log('order_closed_receipt sent:', {
      clientPhone,
      clientName,
      orderNumber,
      orderId,
      receiptUrl: result.link
    });
  } catch (err) {
    console.error('order_closed_receipt send error:', err.data || err.message || err);

    if (isLast) {
      receiptJobs.delete(orderId);
    }
  }
}

function isOrderCreated(payload) {
  const event = getEventName(payload);
  const objectType = String(pick(payload, ['context.object_type'], '')).toLowerCase();

  return event.includes('order.created') || (event.includes('order') && event.includes('created')) || objectType === 'order.created';
}

function isOrderRelatedPayload(payload) {
  const event = getEventName(payload);
  const objectType = String(pick(payload, ['context.object_type'], '')).toLowerCase();

  return event.includes('order') || event.includes('status') || objectType === 'order';
}

function isLeadPayload(payload) {
  const event = getEventName(payload);
  const objectType = String(pick(payload, ['context.object_type'], '')).toLowerCase();

  return event.includes('lead') || event.includes('appeal') || event.includes('request') || objectType === 'lead';
}

function isOrderReady(payload) {
  const statusId = getNewStatusId(payload);
  const status = getOrderStatus(payload).toLowerCase();

  return READY_STATUS_IDS.has(statusId) || status.includes('готов') || status.includes('выдач');
}

function isOrderClosed(payload) {
  const statusId = getNewStatusId(payload);
  const status = getOrderStatus(payload).toLowerCase();

  return CLOSED_STATUS_IDS.has(statusId) || status.includes('закрыт') || status.includes('выдан') || status.includes('closed') || status.includes('completed');
}

function isOrderReceived(payload) {
  const statusId = getNewStatusId(payload);
  const event = getEventName(payload);
  const status = getOrderStatus(payload).toLowerCase();

  return isOrderCreated(payload) || RECEIVED_STATUS_IDS.has(statusId) || status.includes('принят') || status.includes('новый заказ') || (event.includes('order') && event.includes('create'));
}

function collectFormFields(obj) {
  const fields = [];

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

    if (typeof node === 'string' || typeof node === 'number' || typeof node === 'boolean') {
      add(path.split('.').pop() || path, node, path);
      return;
    }

    if (Array.isArray(node)) {
      node.forEach((item, index) => walk(item, `${path}.${index}`, depth + 1));
      return;
    }

    if (typeof node === 'object') {
      const label = pick(node, ['orig_name', 'label', 'name', 'title', 'key', 'field', 'question', 'caption'], '');
      const value = pick(node, ['value', 'text', 'answer', 'content', 'val'], '');

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
    const found = fields.find((field) => regex.test(`${field.label} ${field.path}`));
    if (found?.value) return found.value;
  }

  return '';
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
    'client.first_name',

    'data.client.name',
    'data.client.fullname',
    'data.client.full_name',
    'data.client.first_name',

    'data.name',
    'data.clientName',
    'data.client_name',
    'data.fullname',
    'data.full_name',
    'data.first_name',

    'data.form_data.name.value',
    'form_data.name.value'
  ], '');

  const fromField = findFieldValue(payload, [
    /^имя/i,
    /ваше имя/i,
    /client.*name/i,
    /fullname/i,
    /full_name/i,
    /form_data\.name/i
  ]);

  return titleCaseName(direct) || titleCaseName(fromField) || 'Клиент';
}

function getSiteRepairSubject(payload) {
  const device = findFieldValue(payload, [
    /тип.*устрой/i,
    /устройство/i,
    /инструмент/i,
    /device/i,
    /tool/i,
    /fld_2/i
  ]);

  const problem = findFieldValue(payload, [
    /неисправ/i,
    /проблем/i,
    /problem/i,
    /issue/i,
    /malfunction/i,
    /fld_3/i
  ]);

  const comment = findFieldValue(payload, [
    /коммент/i,
    /сообщ/i,
    /описан/i,
    /comment/i,
    /message/i,
    /description/i,
    /fld_4/i
  ]);

  const lines = [];

  if (device) lines.push(`Тип устройства: ${device}`);
  if (problem) lines.push(`Неисправность: ${problem}`);
  if (comment) lines.push(`Комментарий: ${comment}`);

  if (lines.length) return lines.join('\n');

  return String(pick(payload, [
    'comment',
    'message',
    'description',
    'text',
    'problem',
    'data.comment',
    'data.message',
    'data.description',
    'data.text'
  ], 'Новая заявка с сайта')).trim();
}

function cleanTemplateParam(value) {
  return String(value ?? '')
    .replace(/[\r\n\t]+/g, ' | ')
    .replace(/\s{2,}/g, ' ')
    .replace(/\|\s*\|/g, '|')
    .trim()
    .slice(0, 1000);
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

async function sendTemplate(to, name, parameters = []) {
  const bodyParams = parameters.map((text) => ({
    type: 'text',
    text: cleanTemplateParam(text)
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

async function sendText(to, text) {
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

function autoReplyText() {
  return [
    'Здравствуйте. Это автоматический номер MasterProServis.kz для уведомлений по заказам.',
    '',
    'Пожалуйста, не пишите сюда. Для связи с менеджером напишите в основной WhatsApp:',
    `https://wa.me/${MANAGER_WHATSAPP}`
  ].join('\n');
}

async function handleSiteRepairRequest(payload) {
  const clientName = getSiteClientName(payload);
  const clientPhone = findFirstPhone(payload);
  const subject = getSiteRepairSubject(payload);
  const waLink = clientPhone ? `https://wa.me/${clientPhone}` : 'Телефон не найден';

  log('Site repair request:', {
    clientName,
    clientPhone,
    subject
  });

  await sendTemplate(MANAGER_WHATSAPP, TEMPLATE_SITE_REQUEST, [
    clientName,
    clientPhone ? prettyPhone(clientPhone) : 'не указан',
    subject,
    waLink
  ]);

  log('new_repair_request_alert sent to manager:', {
    clientName,
    clientPhone
  });

  return {
    clientName,
    clientPhone,
    subject
  };
}

async function handleOrderEvent(payload) {
  const statusIdFromWebhook = getNewStatusId(payload);

  if (INTERNAL_STATUS_IDS.has(statusIdFromWebhook)) {
    log('Internal status skipped:', statusIdFromWebhook);
    return;
  }

  const enriched = await enrichRoOrderPayload(payload);
  const fullPayload = enriched.payload;
  const clientPhone = enriched.phone || findFirstPhone(fullPayload);

  if (!clientPhone) {
    log('No client phone found, order skipped:', compactPayloadPreview(fullPayload));
    return;
  }

  const statusId = statusIdFromWebhook || getNewStatusId(fullPayload);

  if (INTERNAL_STATUS_IDS.has(statusId)) {
    log('Internal status skipped after RO API:', statusId);
    return;
  }

  const clientName = getClientName(fullPayload);
  const orderNumber = getOrderNumber(fullPayload);
  const orderId = getRoOrderId(payload) || getRoOrderId(fullPayload);
  const amount = getOrderAmount(fullPayload);
  const status = getOrderStatus(fullPayload);

  log('Order event:', {
    eventName: getEventName(payload),
    orderId,
    orderNumber,
    statusId,
    status,
    clientName,
    clientPhone,
    phoneSource: enriched.source,
    amount: amount.formatted,
    amountSource: amount.source
  });

  if (isOrderReady(fullPayload)) {
    await sendTemplate(clientPhone, TEMPLATE_ORDER_READY, [
      clientName,
      orderNumber,
      amount.formatted
    ]);

    log('order_ready sent:', {
      clientPhone,
      clientName,
      orderNumber,
      amount: amount.formatted
    });

    return;
  }

  if (isOrderClosed(fullPayload)) {
    scheduleReceiptSearch({
      orderId,
      clientPhone,
      clientName,
      orderNumber,
      orderData: fullPayload.ro_api_order
    });

    await sendTemplate(clientPhone, TEMPLATE_ORDER_CLOSED_REVIEW, [clientName]);

    log('order_review_request sent:', {
      clientPhone,
      clientName,
      orderNumber
    });

    return;
  }

  if (isOrderReceived(fullPayload)) {
    await sendTemplate(clientPhone, TEMPLATE_ORDER_RECEIVED, [
      clientName,
      orderNumber
    ]);

    log('order_received sent:', {
      clientPhone,
      clientName,
      orderNumber
    });

    return;
  }

  log('Order status skipped:', {
    orderId,
    orderNumber,
    statusId,
    status
  });
}

function checkRoSecret(req) {
  if (!RO_SECRET) return true;

  const provided = req.query.secret || req.headers['x-ro-secret'] || req.headers['x-webhook-secret'] || req.headers['x-remonline-secret'];
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
    templates: {
      flexbe: TEMPLATE_SITE_REQUEST,
      orderReceived: TEMPLATE_ORDER_RECEIVED,
      orderReady: TEMPLATE_ORDER_READY,
      orderClosedReview: TEMPLATE_ORDER_CLOSED_REVIEW,
      orderReceipt: TEMPLATE_ORDER_RECEIPT
    },
    statuses: {
      ready: [...READY_STATUS_IDS],
      closed: [...CLOSED_STATUS_IDS],
      received: [...RECEIVED_STATUS_IDS],
      internal: [...INTERNAL_STATUS_IDS]
    },
    endpoints: {
      flexbeWebhook: '/site-repair-request',
      remonlineWebhook: '/ro/webhook'
    }
  });
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
    method: 'POST'
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
    const objectType = String(pick(payload, ['context.object_type'], '')).toLowerCase();

    log('RemOnline webhook event:', eventName, { objectType });

    if (isLeadPayload(payload)) {
      log('RemOnline lead/request ignored. Flexbe sends repair requests via /site-repair-request.');
      return;
    }

    if (isOrderCreated(payload) || isOrderRelatedPayload(payload)) {
      await handleOrderEvent(payload);
      return;
    }

    log('RemOnline webhook skipped: not an order event.');
  } catch (err) {
    console.error('RO webhook handling error:', err.data || err.message || err);
  }
});

app.get('/wa/webhook', (req, res) => {
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  if (mode === 'subscribe' && token === META_VERIFY_TOKEN) {
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

        for (const message of value.messages || []) {
          const from = normalizePhone(message.from);
          if (!from) continue;

          await sendText(from, autoReplyText());
          log('Auto reply sent:', from);
        }
      }
    }
  } catch (err) {
    console.error('WA webhook error:', err.data || err.message || err);
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
