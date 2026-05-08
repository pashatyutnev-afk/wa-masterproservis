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

function findFirstPhone(obj) {
  const direct = pick(obj, [
    'client.phone',
    'client.mobile',
    'client.telephone',
    'client.phone_number',
    'client.phoneNumber',
    'client.whatsapp',
    'client.phones.0.phone',
    'client.phones.0.number',
    'client.phones.0.value',
    'client.phones.0.normalized',
    'client.phone_numbers.0.phone',
    'client.phone_numbers.0.number',
    'client.phone_numbers.0.value',
    'client.contacts.0.phone',
    'client.contacts.0.value',
    'client.contacts.0.number',

    'metadata.client.phone',
    'metadata.client.mobile',
    'metadata.client.phone_number',
    'metadata.client.phoneNumber',
    'metadata.client.phones.0.phone',
    'metadata.client.phones.0.number',
    'metadata.client.phones.0.value',

    'client_phone',
    'phone',
    'mobile',
    'telephone',
    'phone_number',
    'phoneNumber',
    'whatsapp',

    'customer.phone',
    'customer.mobile',
    'customer.telephone',
    'customer.phone_number',
    'customer.phoneNumber',
    'customer.phones.0.phone',
    'customer.phones.0.number',
    'customer.phones.0.value',
    'customer_phone',

    'lead.phone',
    'lead.mobile',
    'lead.phone_number',
    'lead.client.phone',
    'lead.client.mobile',
    'lead.client.phone_number',

    'metadata.lead.phone',
    'metadata.lead.mobile',
    'metadata.lead.phone_number',
    'metadata.lead.client.phone',
    'metadata.lead.client.mobile',
    'metadata.lead.client.phone_number',

    'appeal.phone',
    'appeal.mobile',
    'appeal.phone_number',

    'order.client.phone',
    'order.client.mobile',
    'order.client.telephone',
    'order.client.phone_number',
    'order.client.phoneNumber',
    'order.client.phones.0.phone',
    'order.client.phones.0.number',
    'order.client.phones.0.value',

    'data.client.phone',
    'data.client.mobile',
    'data.client.telephone',
    'data.client.phone_number',
    'data.client.phoneNumber',
    'data.client.phones.0.phone',
    'data.client.phones.0.number',
    'data.client.phones.0.value',
    'data.phone',
    'data.mobile',
    'data.phone_number',

    'data.customer.phone',
    'data.customer.mobile',
    'data.customer.phone_number',

    'contact.phone',
    'contact.mobile',
    'contact.phone_number',
    'contact.phoneNumber',

    'ro_api_order.client.phone',
    'ro_api_order.client.mobile',
    'ro_api_order.client.phone_number',
    'ro_api_order.client.phoneNumber',
    'ro_api_order.client.phones.0.phone',
    'ro_api_order.client.phones.0.number',
    'ro_api_order.client.phones.0.value',

    'ro_api_client.phone',
    'ro_api_client.mobile',
    'ro_api_client.phone_number',
    'ro_api_client.phoneNumber',
    'ro_api_client.phones.0.phone',
    'ro_api_client.phones.0.number',
    'ro_api_client.phones.0.value',

    'ro_api_lead.client.phone',
    'ro_api_lead.client.mobile',
    'ro_api_lead.client.phone_number',
    'ro_api_lead.client.phoneNumber',
    'ro_api_lead.client.phones.0.phone',
    'ro_api_lead.client.phones.0.number',
    'ro_api_lead.client.phones.0.value',
    'ro_api_lead.phone',
    'ro_api_lead.mobile',
    'ro_api_lead.phone_number'
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
    'order.id',
    'metadata.order.id',
    'data.order.id',
    'context.order.id',
    'metadata.order_id',
    'object.id',
    'object_id',
    'context.object_id',
    'data.object_id',
    'rel_obj.id',
    'ro_api_order.id'
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
    'client.id',
    'metadata.client.id',
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
    'ro_api_order.status.id'
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
  if (data.order && typeof data.order === 'object') return data.order;
  if (data.client && typeof data.client === 'object') return data.client;
  if (data.lead && typeof data.lead === 'object') return data.lead;
  if (data.item && typeof data.item === 'object') return data.item;

  return data;
}

async function roApiGet(path) {
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

  return unwrapRoResponse(data);
}

async function tryRoApiPaths(paths, label) {
  let lastErr = null;

  for (const path of paths) {
    try {
      const data = await roApiGet(path);
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
  ], `order ${orderId}`);
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
  const originalPhone = findFirstPhone(payload);

  if (originalPhone) {
    return {
      payload,
      phone: originalPhone,
      source: 'webhook'
    };
  }

  const orderId = getRoOrderId(payload);
  const clientIdFromPayload = getRoClientId(payload);

  log('No phone in webhook payload. Trying RO API.', {
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
      'order.client.id'
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
      'customer_id'
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
  return String(pick(payload, [
    'client.name',
    'client.fullname',
    'client.full_name',
    'metadata.client.name',
    'metadata.client.fullname',
    'metadata.client.full_name',
    'client_name',

    'customer.name',
    'customer.fullname',
    'customer_name',

    'lead.name',
    'metadata.lead.client.name',
    'metadata.lead.client.fullname',

    'appeal.name',
    'order.client.name',
    'order.client.fullname',
    'metadata.order.client.name',
    'metadata.order.client.fullname',

    'data.client.name',
    'data.client.fullname',
    'data.customer.name',

    'contact.name',

    'ro_api_client.name',
    'ro_api_client.fullname',
    'ro_api_client.full_name',

    'ro_api_order.client.name',
    'ro_api_order.client.fullname',
    'ro_api_order.client.full_name',

    'ro_api_lead.client.name',
    'ro_api_lead.client.fullname',
    'ro_api_lead.client.full_name',

    'name'
  ], 'Клиент')).trim();
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

    'order.type',
    'order.device',
    'order.name',
    'metadata.order.name',
    'data.order.name',

    'device',
    'product.name',
    'item.name',

    'ro_api_lead.comment',
    'ro_api_lead.text',
    'ro_api_lead.description',
    'ro_api_lead.name',
    'ro_api_lead.message',

    'ro_api_order.name',
    'ro_api_order.device',
    'ro_api_order.type',
    'ro_api_order.comment',
    'ro_api_order.description'
  ], 'Новое обращение')).trim();
}

function getOrderNumber(payload) {
  return String(pick(payload, [
    'order.number',
    'order.id',
    'order.name',
    'metadata.order.number',
    'metadata.order.id',
    'metadata.order.name',
    'order_id',
    'number',
    'name',
    'id',
    'data.order.number',
    'data.order.id',
    'data.order.name',
    'data.id',
    'ro_api_order.number',
    'ro_api_order.id',
    'ro_api_order.name'
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
    'ro_api_order.status'
  ], 'Статус изменён')).trim();
}

function getOrderAmount(payload) {
  const amount = pick(payload, [
    'amount',
    'total',
    'price',
    'order.total',
    'order.amount',
    'metadata.order.total',
    'metadata.order.amount',
    'data.total',
    'data.amount',
    'ro_api_order.total',
    'ro_api_order.amount',
    'ro_api_order.price',
    'ro_api_order.payed',
    'ro_api_order.estimated_cost'
  ], '');

  const currency = pick(payload, [
    'currency',
    'order.currency',
    'data.currency',
    'ro_api_order.currency'
  ], 'KZT');

  return amount ? `${amount} ${currency}` : 'уточняется';
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
    objectType === 'order' && event.includes('created')
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
    closedStatusIds: [...CLOSED_STATUS_IDS]
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

  if (isOrderReady(fullPayload)) {
    await sendTemplate(clientPhone, 'order_ready', [
      clientName,
      orderNumber,
      getOrderAmount(fullPayload)
    ]);

    return log('order_ready sent', clientPhone);
  }

  if (isOrderClosed(fullPayload)) {
    await sendTemplate(clientPhone, 'order_review_request', [
      clientName
    ]);

    return log('order_review_request sent', clientPhone);
  }

  if (isOrderAccepted(fullPayload)) {
    await sendTemplate(clientPhone, 'order_accepted', [
      clientName,
      orderNumber
    ]);

    return log('order_accepted sent', clientPhone);
  }

  if (isOrderStatusChanged(fullPayload)) {
    await sendTemplate(clientPhone, 'order_status_changed', [
      clientName,
      orderNumber,
      status
    ]);

    return log('order_status_changed sent', clientPhone);
  }

  log('No matching order rule, skipped');
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
      '12345'
    ],
    order_status_changed: [
      'Павел',
      '12345',
      'Готов к выдаче'
    ],
    order_ready: [
      'Павел',
      '12345',
      'уточняется'
    ],
    order_review_request: [
      'Павел'
    ]
  };

  return params[template] || ['Павел'];
}

app.get('/test-template', async (req, res) => {
  try {
    const to = normalizePhone(req.query.to || MANAGER_WHATSAPP);
    const template = String(req.query.template || 'order_status_changed');

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
      orderStatusId: '',
      orderStatus: ''
    };

    if (orderId) {
      result.order = await fetchRoOrder(orderId);
      result.phoneFromOrder = findFirstPhone(result.order);
      result.orderStatusId = getNewStatusId({ ro_api_order: result.order });
      result.orderStatus = getOrderStatus({ ro_api_order: result.order });
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

app.use((req, res) => {
  res.status(404).json({
    ok: false,
    error: 'Not found'
  });
});

app.listen(PORT, () => {
  log(`wa-masterproservis listening on port ${PORT}`);
});
