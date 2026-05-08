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
const CLOSED_STATUS_IDS = parseIdList(process.env.CLOSED_STATUS_IDS || '363632');
const ACCEPTED_STATUS_IDS = parseIdList(process.env.ACCEPTED_STATUS_IDS || '');
const INTERNAL_STATUS_IDS = parseIdList(process.env.INTERNAL_STATUS_IDS || '3045928');

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

    'data.client.phone',
    'data.client.mobile',
    'data.client.phone_number',
    'data.client.phone.0',
    'data.client.phones.0.phone',
    'data.client.phones.0.number',
    'data.client.phones.0.value',

    'metadata.client.phone',
    'metadata.client.mobile',
    'metadata.client.phone_number',
    'metadata.client.phone.0',
    'metadata.client.phones.0.phone',
    'metadata.client.phones.0.number',
    'metadata.client.phones.0.value',

    'order.client.phone',
    'order.client.mobile',
    'order.client.phone_number',
    'order.client.phone.0',
    'order.client.phones.0.phone',
    'order.client.phones.0.number',
    'order.client.phones.0.value',

    'ro_api_order.client.phone',
    'ro_api_order.client.mobile',
    'ro_api_order.client.phone_number',
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
function asRoArray(data) {
  const buckets = [
    data,
    data?.data,
    data?.result,
    data?.items,
    data?.rows,
    data?.leads,
    data?.clients,
    data?.data?.data,
    data?.data?.items,
    data?.data?.rows,
    data?.result?.data,
    data?.result?.items,
    data?.result?.rows
  ];

  for (const bucket of buckets) {
    if (Array.isArray(bucket)) return bucket;
  }

  if (data && typeof data === 'object') return [data];

  return [];
}

function normalizeCompareText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/\s+/g, ' ')
    .trim();
}

function getObjectName(obj) {
  return String(pick(obj, [
    'fullname',
    'full_name',
    'name',
    'first_name',
    'client.fullname',
    'client.full_name',
    'client.name',
    'client.first_name',
    'data.fullname',
    'data.full_name',
    'data.name',
    'data.first_name',
    'data.client.fullname',
    'data.client.full_name',
    'data.client.name',
    'data.client.first_name'
  ], '')).trim();
}

function getLeadClientName(payload) {
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
    'data.client.first_name',

    'ro_api_lead.client.fullname',
    'ro_api_lead.client.full_name',
    'ro_api_lead.client.name',
    'ro_api_lead.client.first_name',
    'ro_api_lead.data.client.fullname',
    'ro_api_lead.data.client.full_name',
    'ro_api_lead.data.client.name',
    'ro_api_lead.data.client.first_name'
  ], ''));
}

function pickBestClientByName(data, wantedName) {
  const wanted = normalizeCompareText(wantedName);
  const list = asRoArray(data).filter((item) => item && typeof item === 'object');

  if (!list.length) return null;

  const withPhones = list.filter((item) => findFirstPhone(item));

  if (!wanted) {
    return withPhones[0] || list[0] || null;
  }

  const exact = withPhones.find((item) => normalizeCompareText(getObjectName(item)) === wanted);
  if (exact) return exact;

  const contains = withPhones.find((item) => {
    const name = normalizeCompareText(getObjectName(item));
    return name && (name.includes(wanted) || wanted.includes(name));
  });

  if (contains) return contains;

  return withPhones[0] || list[0] || null;
}

async function searchRoClientByName(clientName) {
  const name = String(clientName || '').trim();
  if (!name) return null;

  const q = encodeURIComponent(name);

  const paths = [
    `/clients?query=${q}`,
    `/clients?search=${q}`,
    `/clients?name=${q}`,
    `/clients?fullname=${q}`,
    `/clients?full_name=${q}`,
    `/clients?filter[name]=${q}`,
    `/clients?filter[fullname]=${q}`,
    `/clients?filter[full_name]=${q}`,
    `/contacts?query=${q}`,
    `/contacts?search=${q}`,
    `/contacts?name=${q}`,
    `/contacts?fullname=${q}`
  ];

  let lastErr = null;

  for (const path of paths) {
    try {
      const data = await roApiGetRaw(path);
      const client = pickBestClientByName(data, name);

      if (client && findFirstPhone(client)) {
        log('RO API client search success:', path);
        return {
          client,
          path
        };
      }

      log('RO API client search empty/no phone:', path);
    } catch (err) {
      lastErr = err;
      log('RO API client search failed:', path, err.status || '', err.message);
    }
  }

  if (lastErr) {
    log('RO API client search failed finally:', lastErr.status || '', lastErr.message);
  }

  return null;
}

function pickLeadFromListResponse(data, leadId) {
  const id = String(leadId || '');
  const list = asRoArray(data).filter((item) => item && typeof item === 'object');

  if (!list.length) return null;

  const exact = list.find((item) => {
    const itemId = String(pick(item, [
      'id',
      'lead_id',
      'object_id',
      'data.id',
      'data.lead_id',
      'data.object_id'
    ], ''));

    return itemId === id;
  });

  if (exact) return exact;

  if (list.length === 1) return list[0];

  return null;
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

function findFormTextInPayload(obj) {
  const candidates = [];

  function walk(node, path = '', depth = 0) {
    if (node == null || depth > 12) return;

    if (typeof node === 'string' || typeof node === 'number') {
      const text = String(node || '').trim();

      if (
        text &&
        (
          /Тип устройства\s*:/i.test(text) ||
          /Неисправность\s*:/i.test(text) ||
          /Комментарий\s*:/i.test(text) ||
          /FormID\s*:/i.test(text)
        )
      ) {
        candidates.push({
          path,
          text
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

  candidates.sort((a, b) => {
    const score = (item) => {
      let s = 0;
      if (/comment|description|message|text|notes|коммент/i.test(item.path)) s -= 10;
      if (/Тип устройства\s*:/i.test(item.text)) s -= 5;
      if (/Неисправность\s*:/i.test(item.text)) s -= 5;
      if (/Комментарий\s*:/i.test(item.text)) s -= 5;
      return s;
    };

    return score(a) - score(b);
  });

  return candidates[0]?.text || '';
}
async function fetchRoOrderPublicUrl(orderId) {
  if (!orderId) return null;

  return roApiGetRaw(`/orders/${orderId}/public-url`);
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
  const rawSubject = String(pick(payload, [
    'comment',
    'description',
    'message',
    'problem',
    'text',

    'lead.comment',
    'lead.description',
    'lead.text',
    'lead.message',

    'metadata.lead.comment',
    'metadata.lead.description',
    'metadata.lead.text',
    'metadata.lead.message',

    'appeal.comment',
    'appeal.description',
    'appeal.text',
    'appeal.message',

    'request.comment',
    'request.description',
    'request.text',
    'request.message',

    'data.comment',
    'data.description',
    'data.message',
    'data.text',

    'ro_api_lead.comment',
    'ro_api_lead.description',
    'ro_api_lead.text',
    'ro_api_lead.message',

    'ro_api_order.comment',
    'ro_api_order.description',
    'ro_api_order.manager_notes',
    'ro_api_order.engineer_notes'
  ], '')).trim();

  if (rawSubject) return rawSubject;

  const leadName = String(pick(payload, [
    'lead.name',
    'metadata.lead.name',
    'data.lead.name',
    'ro_api_lead.name'
  ], '')).trim();

  // РемОнлайн иногда присылает сюда просто номер обращения: "21".
  // Это не описание заявки, поэтому клиенту/менеджеру его не показываем как "Что нужно".
  if (leadName && !/^\d+$/.test(leadName)) {
    return leadName;
  }

  return 'Комментарий';
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

  const best = candidates.find((c) => c.value > 0) || candidates[0];

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

function getOrderAmount(payload) {
  return getOrderAmountDebug(payload).formatted;
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

  log('Order debug:', {
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
