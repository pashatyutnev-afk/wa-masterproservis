import express from 'express';

const app = express();

app.use(express.json({ limit: '3mb' }));
app.use(express.urlencoded({ extended: true }));

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
const CLOSED_STATUS_IDS = parseIdList(process.env.CLOSED_STATUS_IDS || '');
const ACCEPTED_STATUS_IDS = parseIdList(process.env.ACCEPTED_STATUS_IDS || '');
const INTERNAL_STATUS_IDS = parseIdList(process.env.INTERNAL_STATUS_IDS || '');

const RECEIPT_TEMPLATE_NAME = process.env.RECEIPT_TEMPLATE_NAME || 'order_closed_receipt';
const RECEIPT_SEARCH_ENABLED = String(process.env.RECEIPT_SEARCH_ENABLED || 'true').toLowerCase() !== 'false';
const RECEIPT_RETRY_MS = parseRetryList(process.env.RECEIPT_RETRY_MS || '0,10000,30000,60000,180000,600000');

const receiptJobs = new Set();
const receiptSent = new Set();

const GRAPH_BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;

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
    'client.telephone',
    'client.phone_number',
    'client.phoneNumber',
    'client.whatsapp',
    'client.phone.0',
    'client.phones.0.phone',
    'client.phones.0.number',
    'client.phones.0.value',
    'client.phones.0.normalized',

    'data.client.phone',
    'data.client.mobile',
    'data.client.telephone',
    'data.client.phone_number',
    'data.client.phoneNumber',
    'data.client.phone.0',
    'data.client.phones.0.phone',
    'data.client.phones.0.number',
    'data.client.phones.0.value',

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
    'order.client.telephone',
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
    'ro_api_order.data.client.phone.0',
    'ro_api_order.data.client.phones.0.phone',
    'ro_api_order.data.client.phones.0.number',
    'ro_api_order.data.client.phones.0.value',

    'ro_api_client.phone.0',
    'ro_api_client.phone',
    'ro_api_client.mobile',
    'ro_api_client.phone_number',
    'ro_api_client.phoneNumber',
    'ro_api_client.phones.0.phone',
    'ro_api_client.phones.0.number',
    'ro_api_client.phones.0.value',

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
      const keyLooksPhone = /phone|mobile|tel|contact|whatsapp|wa|номер|телефон/i.test(path);
      const found = looksLikePhone(node);

      if (found && (keyLooksPhone || found.length === 11)) {
        if (!seen.has(found)) {
          seen.add(found);
          candidates.push(found);
        }
      }

      return;
    }

    if (Array.isArray(node)) {
      node.forEach((v, i) => walk(v, `${path}.${i}`, depth + 1));
      return;
    }

    if (typeof node === 'object') {
      for (const [k, v] of Object.entries(node)) {
        walk(v, path ? `${path}.${k}` : k, depth + 1);
      }
    }
  }

  walk(obj);

  if (candidates.length) return candidates[0];

  const text = JSON.stringify(obj || {});
  const match = text.match(/(?:\+?7|8)[\s\-()]*\d{3}[\s\-()]*\d{3}[\s\-()]*\d{2}[\s\-()]*\d{2}|\b7\d{10}\b|\b8\d{10}\b/);

  return match ? normalizePhone(match[0]) : '';
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
    'lead.id',
    'metadata.lead.id',
    'data.lead.id',
    'context.lead.id',
    'metadata.lead_id',
    'object.id',
    'object_id',
    'context.object_id',
    'data.object_id',
    'ro_api_lead.id'
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

    'lead.client.id',
    'metadata.lead.client.id',
    'ro_api_lead.client.id',
    'ro_api_lead.client_id',

    'ro_api_order.client.id',
    'ro_api_order.client_id',
    'ro_api_order.data.client.id',
    'ro_api_order.data.client_id',
    'ro_api_client.id'
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

function compactPayloadPreview(payload) {
  try {
    return JSON.stringify(payload).slice(0, 16000);
  } catch {
    return '[payload stringify failed]';
  }
}

function unwrapRoResponse(data) {
  if (!data || typeof data !== 'object') return data;

  if (data.data && typeof data.data === 'object') return data.data;
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

async function fetchRoLead(leadId) {
  if (!leadId) return null;

  return tryRoApiPaths([
    `/leads/${leadId}`,
    `/leads/${leadId}?include=client`,
    `/leads/${leadId}?expand=client`,
    `/leads/${leadId}?with=client`,
    `/appeals/${leadId}`,
    `/appeals/${leadId}?include=client`,
    `/requests/${leadId}`,
    `/requests/${leadId}?include=client`
  ], `lead ${leadId}`);
}

async function fetchRoClient(clientId) {
  if (!clientId) return null;

  return tryRoApiPaths([
    `/clients/${clientId}`,
    `/clients/${clientId}?include=phones`,
    `/clients/${clientId}?expand=phones`,
    `/contacts/${clientId}`,
    `/clients?ids[]=${clientId}`
  ], `client ${clientId}`);
}

async function enrichRoOrderPayloadWithApi(payload) {
  const orderId = getRoOrderId(payload);
  const clientIdFromPayload = getRoClientId(payload);

  log('Order webhook: trying RO API first.', {
    orderId: orderId || null,
    clientId: clientIdFromPayload || null
  });

  let orderData = null;
  let clientData = null;

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

  const clientId =
    clientIdFromPayload ||
    getRoClientId(orderData || {}) ||
    findFirstId(orderData || {}, [
      'client.id',
      'customer.id',
      'data.client.id',
      'order.client.id',
      'client_id',
      'customer_id'
    ]);

  if (clientId) {
    try {
      clientData = await fetchRoClient(clientId);
      const phoneFromClient = findFirstPhone(clientData);

      if (phoneFromClient) {
        return {
          payload: { ...payload, ro_api_order: orderData, ro_api_client: clientData },
          phone: phoneFromClient,
          source: 'ro_api_client'
        };
      }
    } catch (err) {
      log('RO API client lookup failed finally:', err.status || '', err.message);
    }
  }

  const fallbackPhone = findFirstPhone(payload);

  if (fallbackPhone) {
    log('WARNING: RO API phone not found, fallback to webhook phone:', fallbackPhone);

    return {
      payload: { ...payload, ro_api_order: orderData, ro_api_client: clientData },
      phone: fallbackPhone,
      source: 'webhook_fallback'
    };
  }

  return {
    payload: { ...payload, ro_api_order: orderData, ro_api_client: clientData },
    phone: '',
    source: 'not_found'
  };
}

async function enrichRoLeadPayloadWithApi(payload) {
  const originalPhone = findFirstPhone(payload);

  if (originalPhone) {
    return {
      payload,
      phone: originalPhone,
      source: 'webhook'
    };
  }

  const leadId = getRoLeadId(payload);
  const clientIdFromPayload = getRoClientId(payload);

  log('No phone in lead webhook payload. Trying RO API.', {
    leadId: leadId || null,
    clientId: clientIdFromPayload || null
  });

  let leadData = null;
  let clientData = null;

  if (leadId) {
    try {
      leadData = await fetchRoLead(leadId);
      const phoneFromLead = findFirstPhone(leadData);

      if (phoneFromLead) {
        return {
          payload: { ...payload, ro_api_lead: leadData },
          phone: phoneFromLead,
          source: 'ro_api_lead'
        };
      }
    } catch (err) {
      log('RO API lead lookup failed finally:', err.status || '', err.message);
    }
  }

  const clientId =
    clientIdFromPayload ||
    getRoClientId(leadData || {}) ||
    findFirstId(leadData || {}, [
      'client.id',
      'customer.id',
      'client_id',
      'customer_id',
      'id'
    ]);

  if (clientId) {
    try {
      clientData = await fetchRoClient(clientId);
      const phoneFromClient = findFirstPhone(clientData);

      if (phoneFromClient) {
        return {
          payload: { ...payload, ro_api_lead: leadData, ro_api_client: clientData },
          phone: phoneFromClient,
          source: 'ro_api_client'
        };
      }
    } catch (err) {
      log('RO API client lookup failed finally:', err.status || '', err.message);
    }
  }

  return {
    payload: { ...payload, ro_api_lead: leadData, ro_api_client: clientData },
    phone: '',
    source: 'not_found'
  };
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

function getClientName(payload) {
  const firstName = pick(payload, [
    'ro_api_client.first_name',
    'ro_api_order.first_name',
    'ro_api_order.data.first_name',
    'ro_api_order.client.first_name',
    'ro_api_order.data.client.first_name',
    'ro_api_lead.client.first_name',

    'metadata.client.first_name',
    'client.first_name',
    'metadata.order.client.first_name',
    'order.client.first_name'
  ], '');

  const fullName = pick(payload, [
    'ro_api_client.name',
    'ro_api_client.fullname',
    'ro_api_client.full_name',

    'ro_api_order.name',
    'ro_api_order.fullname',
    'ro_api_order.full_name',
    'ro_api_order.data.name',
    'ro_api_order.data.fullname',
    'ro_api_order.data.full_name',
    'ro_api_order.client.name',
    'ro_api_order.client.fullname',
    'ro_api_order.client.full_name',
    'ro_api_order.data.client.name',
    'ro_api_order.data.client.fullname',
    'ro_api_order.data.client.full_name',

    'ro_api_lead.client.name',
    'ro_api_lead.client.fullname',
    'ro_api_lead.client.full_name',

    'metadata.client.fullname',
    'metadata.client.full_name',
    'metadata.client.name',
    'client.fullname',
    'client.full_name',
    'client.name',

    'metadata.order.client.fullname',
    'metadata.order.client.full_name',
    'metadata.order.client.name',
    'order.client.fullname',
    'order.client.full_name',
    'order.client.name',

    'customer.fullname',
    'customer.name',
    'lead.name',
    'metadata.lead.client.fullname',
    'metadata.lead.client.name',
    'appeal.name',
    'data.client.fullname',
    'data.client.name',
    'data.customer.name',
    'contact.name',
    'client_name',
    'customer_name',
    'name'
  ], '');

  return titleCaseName(firstName) || titleCaseName(fullName) || 'Клиент';
}

function getRepairSubject(payload) {
  return String(pick(payload, [
    'subject',
    'title',
    'message',
    'comment',
    'description',
    'problem',

    'lead.comment',
    'lead.text',
    'lead.name',
    'metadata.lead.comment',
    'metadata.lead.text',
    'metadata.lead.name',

    'appeal.comment',
    'appeal.text',
    'request.text',

    'data.subject',
    'data.title',
    'data.message',
    'data.comment',
    'data.description',

    'metadata.order.name',
    'order.name',
    'order.type',
    'order.device',
    'data.order.name',

    'device',
    'product.name',
    'item.name',

    'ro_api_lead.comment',
    'ro_api_lead.text',
    'ro_api_lead.description',
    'ro_api_lead.name',
    'ro_api_lead.message',

    'ro_api_order.device',
    'ro_api_order.type',
    'ro_api_order.comment',
    'ro_api_order.description',
    'ro_api_order.data.device',
    'ro_api_order.data.type',
    'ro_api_order.data.comment',
    'ro_api_order.data.description'
  ], 'Новое обращение')).trim();
}

function getOrderNumber(payload) {
  return String(pick(payload, [
    'metadata.order.name',
    'order.name',
    'data.order.name',
    'ro_api_order.order.name',
    'ro_api_order.data.order.name',
    'ro_api_order.number',
    'ro_api_order.data.number',
    'ro_api_order.name',
    'ro_api_order.data.name',

    'metadata.order.number',
    'order.number',
    'data.order.number',
    'ro_api_order.order.number',
    'ro_api_order.data.order.number',

    'metadata.order.id',
    'order.id',
    'order_id',
    'number',
    'id',
    'data.order.id',
    'data.id',
    'ro_api_order.order.id',
    'ro_api_order.data.order.id',
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

function moneyLooksBadPath(path) {
  return /id|phone|телефон|status|статус|number|номер|barcode|code|код|date|time|timestamp|created|updated|client|customer|employee|user|manager/i.test(path);
}

function moneyLooksGoodPath(path) {
  return /total|sum|amount|price|cost|paid|payed|payment|balance|debt|due|to_pay|payable|grand|final|subtotal|work|service|goods|item|product|parts|materials|profit|estimated/i.test(path);
}

function formatMoney(num) {
  if (num === null || num === undefined || !Number.isFinite(num)) return '';
  const rounded = Math.round(num * 100) / 100;

  if (Math.abs(rounded - Math.round(rounded)) < 0.001) {
    return String(Math.round(rounded));
  }

  return rounded.toFixed(2).replace('.', ',');
}

function getDirectAmountCandidate(payload) {
  const paths = [
    'ro_api_order.total',
    'ro_api_order.sum',
    'ro_api_order.amount',
    'ro_api_order.price',
    'ro_api_order.cost',
    'ro_api_order.final_sum',
    'ro_api_order.total_sum',
    'ro_api_order.grand_total',
    'ro_api_order.to_pay',
    'ro_api_order.payable',
    'ro_api_order.balance',
    'ro_api_order.debt',
    'ro_api_order.estimated_cost',

    'ro_api_order.data.total',
    'ro_api_order.data.sum',
    'ro_api_order.data.amount',
    'ro_api_order.data.price',
    'ro_api_order.data.cost',
    'ro_api_order.data.final_sum',
    'ro_api_order.data.total_sum',
    'ro_api_order.data.grand_total',
    'ro_api_order.data.to_pay',
    'ro_api_order.data.payable',
    'ro_api_order.data.balance',
    'ro_api_order.data.debt',
    'ro_api_order.data.estimated_cost',

    'ro_api_order.order.total',
    'ro_api_order.order.sum',
    'ro_api_order.order.amount',
    'ro_api_order.order.price',
    'ro_api_order.order.cost',
    'ro_api_order.order.final_sum',
    'ro_api_order.order.total_sum',
    'ro_api_order.order.grand_total',
    'ro_api_order.order.to_pay',
    'ro_api_order.order.payable',
    'ro_api_order.order.balance',
    'ro_api_order.order.debt',
    'ro_api_order.order.estimated_cost',

    'ro_api_order.data.order.total',
    'ro_api_order.data.order.sum',
    'ro_api_order.data.order.amount',
    'ro_api_order.data.order.price',
    'ro_api_order.data.order.cost',
    'ro_api_order.data.order.final_sum',
    'ro_api_order.data.order.total_sum',
    'ro_api_order.data.order.grand_total',
    'ro_api_order.data.order.to_pay',
    'ro_api_order.data.order.payable',
    'ro_api_order.data.order.balance',
    'ro_api_order.data.order.debt',
    'ro_api_order.data.order.estimated_cost',

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

  for (const path of paths) {
    const raw = pick(payload, [path], '');
    const parsed = parseMoneyValue(raw);

    if (parsed !== null && parsed >= 0 && parsed < 100000000) {
      return {
        value: parsed,
        source: path,
        raw
      };
    }
  }

  return null;
}

function findMoneyCandidates(obj) {
  const candidates = [];

  function walk(node, path = '', depth = 0) {
    if (node == null || depth > 12) return;

    if (typeof node === 'number' || typeof node === 'string') {
      const value = parseMoneyValue(node);

      if (
        value !== null &&
        value >= 0 &&
        value < 100000000 &&
        !moneyLooksBadPath(path) &&
        moneyLooksGoodPath(path)
      ) {
        candidates.push({
          path,
          value,
          raw: node
        });
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

  return candidates
    .sort((a, b) => {
      const aPriority = /to_pay|payable|balance|debt|grand_total|final|total_sum|total|sum|amount|price/i.test(a.path) ? 0 : 1;
      const bPriority = /to_pay|payable|balance|debt|grand_total|final|total_sum|total|sum|amount|price/i.test(b.path) ? 0 : 1;

      if (aPriority !== bPriority) return aPriority - bPriority;
      return String(a.path).localeCompare(String(b.path));
    })
    .slice(0, 50);
}

function computeAmountFromLineItems(payload) {
  const arrays = [];

  function walk(node, path = '', depth = 0) {
    if (node == null || depth > 10) return;

    if (Array.isArray(node)) {
      if (/items|goods|services|works|parts|materials|products|positions|rows|lines|jobs|work/i.test(path)) {
        arrays.push({
          path,
          items: node
        });
      }

      node.forEach((item, i) => walk(item, `${path}.${i}`, depth + 1));
      return;
    }

    if (typeof node === 'object') {
      for (const [key, value] of Object.entries(node)) {
        walk(value, path ? `${path}.${key}` : key, depth + 1);
      }
    }
  }

  walk(payload);

  for (const arr of arrays) {
    let total = 0;
    let used = 0;

    for (const item of arr.items) {
      if (!item || typeof item !== 'object') continue;

      const direct = getDirectAmountCandidate({ ro_api_order: item }) || getDirectAmountCandidate(item);

      if (direct && direct.value > 0) {
        total += direct.value;
        used += 1;
        continue;
      }

      const price = parseMoneyValue(pick(item, [
        'price',
        'cost',
        'amount',
        'unit_price',
        'sale_price',
        'data.price',
        'data.cost',
        'data.amount',
        'data.unit_price'
      ], ''));

      const qty = parseMoneyValue(pick(item, [
        'quantity',
        'qty',
        'count',
        'amount_count',
        'data.quantity',
        'data.qty',
        'data.count'
      ], 1));

      if (price !== null && price > 0) {
        total += price * (qty && qty > 0 ? qty : 1);
        used += 1;
      }
    }

    if (used > 0 && total > 0) {
      return {
        value: total,
        source: `computed:${arr.path}`,
        used
      };
    }
  }

  return null;
}

function getOrderAmountDebug(payload) {
  const direct = getDirectAmountCandidate(payload);

  if (direct) {
    return {
      value: direct.value,
      formatted: `${formatMoney(direct.value)} ₸`,
      source: direct.source,
      raw: direct.raw,
      candidates: findMoneyCandidates(payload)
    };
  }

  const computed = computeAmountFromLineItems(payload);

  if (computed) {
    return {
      value: computed.value,
      formatted: `${formatMoney(computed.value)} ₸`,
      source: computed.source,
      raw: null,
      candidates: findMoneyCandidates(payload)
    };
  }

  return {
    value: null,
    formatted: 'уточняется',
    source: 'not_found',
    raw: null,
    candidates: findMoneyCandidates(payload)
  };
}

function getOrderAmount(payload) {
  return getOrderAmountDebug(payload).formatted;
}

function extractUrlsFromText(value) {
  const text = String(value || '');
  const matches = text.match(/https?:\/\/[^\s"'<>]+/g) || [];

  return matches.map((url) => url.replace(/[),.;]+$/g, ''));
}

function receiptPathLooksGood(path) {
  return /receipt|check|cheque|fiscal|fiskal|kofd|ofd|webkassa|web-kassa|cashbox|payment|pay|invoice|bill|document|pdf|qr|чек|касс|фискал|оплат/i.test(path);
}

function receiptUrlLooksGood(url) {
  return /cabinet\.kofd\.kz|kofd|ofd|webkassa|receipt|check|cheque|fiscal|pdf|consumer|qr/i.test(String(url || ''));
}

function scoreReceiptCandidate(candidate) {
  const url = String(candidate.url || '');
  const path = String(candidate.path || '');

  if (/cabinet\.kofd\.kz\/consumer/i.test(url)) return 0;
  if (/kofd/i.test(url)) return 1;
  if (/webkassa/i.test(url)) return 2;
  if (/fiscal|receipt|check|cheque/i.test(url)) return 3;
  if (receiptPathLooksGood(path)) return 4;
  return 10;
}

function findReceiptCandidates(obj) {
  const candidates = [];
  const seen = new Set();

  function addCandidate(url, path, raw) {
    if (!url) return;

    const cleanUrl = String(url).replace(/[),.;]+$/g, '');

    if (!/^https?:\/\//i.test(cleanUrl)) return;

    const goodByUrl = receiptUrlLooksGood(cleanUrl);
    const goodByPath = receiptPathLooksGood(path);

    if (!goodByUrl && !goodByPath) return;
    if (seen.has(cleanUrl)) return;

    seen.add(cleanUrl);

    candidates.push({
      url: cleanUrl,
      path,
      raw: String(raw || '').slice(0, 500)
    });
  }

  function walk(node, path = '', depth = 0) {
    if (node == null || depth > 14) return;

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
    const scoreA = scoreReceiptCandidate(a);
    const scoreB = scoreReceiptCandidate(b);

    if (scoreA !== scoreB) return scoreA - scoreB;
    return String(a.path).localeCompare(String(b.path));
  });
}

async function tryReceiptPath(path, label) {
  try {
    const data = await roApiGetRaw(path);
    log(`RO API receipt ${label} success:`, path);
    return {
      path,
      data
    };
  } catch (err) {
    log(`RO API receipt ${label} failed:`, path, err.status || '', err.message);
    return null;
  }
}

async function findReceiptLink(orderId, baseOrderData = null) {
  const sources = [];
  const checked = [];

  function remember(path, ok, error = '') {
    checked.push({
      path,
      ok,
      error: String(error || '').slice(0, 300)
    });
  }

  if (baseOrderData) {
    sources.push({
      path: 'base_ro_api_order',
      data: baseOrderData
    });

    remember('base_ro_api_order', true);
  }

  const paths = [
    // Сам заказ разными способами
    `/orders/${orderId}`,
    `/orders/${orderId}?include=payments`,
    `/orders/${orderId}?include=receipts`,
    `/orders/${orderId}?include=payments,receipts`,
    `/orders/${orderId}?include=documents`,
    `/orders/${orderId}?include=invoices`,
    `/orders/${orderId}?include=checks`,
    `/orders/${orderId}?include=fiscal`,
    `/orders/${orderId}?include=cashbox`,
    `/orders/${orderId}?include=webkassa`,
    `/orders/${orderId}?expand=payments`,
    `/orders/${orderId}?expand=receipts`,
    `/orders/${orderId}?expand=documents`,
    `/orders/${orderId}?expand=invoices`,
    `/orders/${orderId}?expand=checks`,
    `/orders/${orderId}?expand=fiscal`,
    `/orders/${orderId}?expand=cashbox`,
    `/orders/${orderId}?expand=webkassa`,

    // Вложенные сущности заказа
    `/orders/${orderId}/payments`,
    `/orders/${orderId}/payments?include=receipt`,
    `/orders/${orderId}/payments?include=receipts`,
    `/orders/${orderId}/payments?include=check`,
    `/orders/${orderId}/payments?include=checks`,
    `/orders/${orderId}/payments?include=fiscal`,
    `/orders/${orderId}/payments?include=webkassa`,
    `/orders/${orderId}/payments?expand=receipt`,
    `/orders/${orderId}/payments?expand=receipts`,
    `/orders/${orderId}/payments?expand=check`,
    `/orders/${orderId}/payments?expand=checks`,
    `/orders/${orderId}/payments?expand=fiscal`,
    `/orders/${orderId}/payments?expand=webkassa`,
    `/orders/${orderId}/transactions`,
    `/orders/${orderId}/cashbox-transactions`,
    `/orders/${orderId}/cashbox_transactions`,
    `/orders/${orderId}/receipts`,
    `/orders/${orderId}/receipt`,
    `/orders/${orderId}/checks`,
    `/orders/${orderId}/check`,
    `/orders/${orderId}/fiscal`,
    `/orders/${orderId}/fiscal-receipts`,
    `/orders/${orderId}/fiscal_receipts`,
    `/orders/${orderId}/cashbox`,
    `/orders/${orderId}/documents`,
    `/orders/${orderId}/invoices`,
    `/orders/${orderId}/invoice`,
    `/orders/${orderId}/public-url`,
    `/orders/${orderId}/public_url`,
    `/orders/${orderId}/public`,

    // Глобальные платежи / касса по order_id
    `/payments?order_id=${orderId}`,
    `/payments?orderId=${orderId}`,
    `/payments?order=${orderId}`,
    `/payments?filter[order_id]=${orderId}`,
    `/payments?filter[orderId]=${orderId}`,
    `/payments?include=receipt&order_id=${orderId}`,
    `/payments?include=receipts&order_id=${orderId}`,
    `/payments?include=check&order_id=${orderId}`,
    `/payments?include=checks&order_id=${orderId}`,
    `/payments?include=fiscal&order_id=${orderId}`,
    `/payments?include=webkassa&order_id=${orderId}`,
    `/payments?expand=receipt&order_id=${orderId}`,
    `/payments?expand=receipts&order_id=${orderId}`,
    `/payments?expand=check&order_id=${orderId}`,
    `/payments?expand=checks&order_id=${orderId}`,
    `/payments?expand=fiscal&order_id=${orderId}`,
    `/payments?expand=webkassa&order_id=${orderId}`,

    // Cashbox варианты
    `/cashbox?order_id=${orderId}`,
    `/cashbox?orderId=${orderId}`,
    `/cashboxes?order_id=${orderId}`,
    `/cashboxes?orderId=${orderId}`,
    `/cashbox/transactions?order_id=${orderId}`,
    `/cashbox/transactions?orderId=${orderId}`,
    `/cashbox-transactions?order_id=${orderId}`,
    `/cashbox-transactions?orderId=${orderId}`,
    `/cashbox_transactions?order_id=${orderId}`,
    `/cashbox_transactions?orderId=${orderId}`,
    `/cashbox_transactions?filter[order_id]=${orderId}`,
    `/cashbox-transactions?filter[order_id]=${orderId}`,
    `/transactions?order_id=${orderId}`,
    `/transactions?orderId=${orderId}`,
    `/transactions?filter[order_id]=${orderId}`,

    // Документы / инвойсы / чеки глобально
    `/documents?order_id=${orderId}`,
    `/documents?orderId=${orderId}`,
    `/documents?filter[order_id]=${orderId}`,
    `/invoices?order_id=${orderId}`,
    `/invoices?orderId=${orderId}`,
    `/invoices?filter[order_id]=${orderId}`,
    `/receipts?order_id=${orderId}`,
    `/receipts?orderId=${orderId}`,
    `/receipts?filter[order_id]=${orderId}`,
    `/checks?order_id=${orderId}`,
    `/checks?orderId=${orderId}`,
    `/checks?filter[order_id]=${orderId}`,
    `/fiscal?order_id=${orderId}`,
    `/fiscal?orderId=${orderId}`,
    `/fiscal-receipts?order_id=${orderId}`,
    `/fiscal-receipts?orderId=${orderId}`,
    `/fiscal_receipts?order_id=${orderId}`,
    `/fiscal_receipts?orderId=${orderId}`,

    // Webkassa / OFD варианты
    `/webkassa?order_id=${orderId}`,
    `/webkassa?orderId=${orderId}`,
    `/webkassa/receipts?order_id=${orderId}`,
    `/webkassa/receipts?orderId=${orderId}`,
    `/webkassa/checks?order_id=${orderId}`,
    `/webkassa/checks?orderId=${orderId}`,
    `/ofd?order_id=${orderId}`,
    `/ofd?orderId=${orderId}`,
    `/kofd?order_id=${orderId}`,
    `/kofd?orderId=${orderId}`
  ];

  for (const path of paths) {
    try {
      const data = await roApiGetRaw(path);

      remember(path, true);

      sources.push({
        path,
        data
      });

      log('RO API receipt source success:', path);
    } catch (err) {
      remember(path, false, `${err.status || ''} ${err.message || ''}`);
      log('RO API receipt source failed:', path, err.status || '', err.message);
    }
  }

  const candidates = [];

  for (const source of sources) {
    const found = findReceiptCandidates(source.data);

    for (const candidate of found) {
      candidates.push({
        ...candidate,
        sourcePath: source.path
      });
    }
  }

  const unique = [];
  const seen = new Set();

  for (const candidate of candidates) {
    if (seen.has(candidate.url)) continue;

    seen.add(candidate.url);
    unique.push(candidate);
  }

  unique.sort((a, b) => {
    const scoreA = scoreReceiptCandidate(a);
    const scoreB = scoreReceiptCandidate(b);

    if (scoreA !== scoreB) return scoreA - scoreB;
    return String(a.sourcePath).localeCompare(String(b.sourcePath));
  });

  return {
    link: unique[0]?.url || '',
    candidates: unique,
    sourcesChecked: checked
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

function isOrderStatusChanged(payload) {
  const event = getEventName(payload);

  return (
    event.includes('status') ||
    event.includes('order.status') ||
    event.includes('order') ||
    JSON.stringify(payload || {}).toLowerCase().includes('status')
  );
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
    receiptRetryMs: RECEIPT_RETRY_MS
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

function checkRoSecret(req) {
  if (!RO_SECRET) return true;

  const provided =
    req.query.secret ||
    req.headers['x-ro-secret'] ||
    req.headers['x-webhook-secret'] ||
    req.headers['x-remonline-secret'];

  return String(provided || '') === RO_SECRET;
}

async function handleRepairRequest(payload) {
  const enriched = await enrichRoLeadPayloadWithApi(payload);
  const fullPayload = enriched.payload;

  const clientName = getClientName(fullPayload);
  const clientPhone = enriched.phone || findFirstPhone(fullPayload);
  const subject = getRepairSubject(fullPayload);
  const clientWaLink = clientPhone ? `https://wa.me/${clientPhone}` : 'Телефон не найден';

  if (!clientPhone) {
    log('No client phone found for repair request, sending manager alert without phone');
    log('RemOnline payload preview:', compactPayloadPreview(fullPayload));
  } else {
    log('Client phone found for repair request:', clientPhone, 'source:', enriched.source);
  }

  await sendTemplate(MANAGER_WHATSAPP, 'new_repair_request_alert', [
    clientName || 'Клиент',
    clientPhone ? `+${clientPhone}` : 'не указан',
    subject || 'Новое обращение',
    clientWaLink
  ]);

  log('Repair request alert sent to manager');
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

  log('Client phone found:', clientPhone, 'source:', enriched.source);

  const clientName = getClientName(fullPayload);
  const orderNumber = getOrderNumber(fullPayload);
  const status = getOrderStatus(fullPayload);
  const statusId = statusIdBeforeApi || getNewStatusId(fullPayload);
  const amountDebug = getOrderAmountDebug(fullPayload);
  const orderId = getRoOrderId(payload) || getRoOrderId(fullPayload);

  log('Order status debug:', {
    eventName: getEventName(payload),
    statusId: statusId || null,
    orderId: orderId || null,
    orderNumber,
    clientName,
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

      log('order_review_request sent', clientPhone, { clientName, orderNumber, statusId });
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

    return log('order_accepted sent', clientPhone, { clientName, orderNumber, statusId });
  }

  log('Unhandled/non-customer status, skip WhatsApp:', {
    statusId: statusId || null,
    orderNumber,
    status
  });
}

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
      return await handleRepairRequest(payload);
    }

    if (isOrderCreated(payload) || isOrderStatusChanged(payload) || isOrderAccepted(payload)) {
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
      'Новое обращение',
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
      'https://cabinet.kofd.kz/consumer?test=1'
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

app.get('/test-reply', async (req, res) => {
  try {
    const to = normalizePhone(req.query.to || MANAGER_WHATSAPP);
    const data = await sendWhatsAppText(to, autoReplyText());

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

app.get('/test-ro', async (req, res) => {
  try {
    const orderId = String(req.query.orderId || '');
    const leadId = String(req.query.leadId || '');
    const clientId = String(req.query.clientId || '');

    const result = {
      ok: true,
      orderId,
      leadId,
      clientId,
      order: null,
      lead: null,
      client: null,
      phoneFromOrder: '',
      phoneFromLead: '',
      phoneFromClient: '',
      guessedClientNameFromOrder: '',
      orderStatusId: '',
      orderStatus: '',
      orderAmount: 'уточняется',
      orderAmountSource: '',
      orderAmountCandidates: []
    };

    if (orderId) {
      result.order = await fetchRoOrder(orderId);
      const payloadForOrder = { ro_api_order: result.order };

      result.phoneFromOrder = findFirstPhone(result.order);
      result.guessedClientNameFromOrder = getClientName(payloadForOrder);
      result.orderStatusId = getNewStatusId(payloadForOrder);
      result.orderStatus = getOrderStatus(payloadForOrder);

      const amountDebug = getOrderAmountDebug(payloadForOrder);
      result.orderAmount = amountDebug.formatted;
      result.orderAmountSource = amountDebug.source;
      result.orderAmountCandidates = amountDebug.candidates;
    }

    if (leadId) {
      result.lead = await fetchRoLead(leadId);
      result.phoneFromLead = findFirstPhone(result.lead);
    }

    if (clientId) {
      result.client = await fetchRoClient(clientId);
      result.phoneFromClient = findFirstPhone(result.client);
    }

    res.json(result);
  } catch (err) {
    res.status(500).json({
      ok: false,
      error: err.message,
      status: err.status,
      data: err.data
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

app.use((req, res) => {
  res.status(404).json({
    ok: false,
    error: 'Not found'
  });
});

app.listen(PORT, () => {
  log(`wa-masterproservis listening on port ${PORT}`);
});
