import express from 'express';

const app = express();
app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true }));

const PORT = Number(process.env.PORT || 3000);
const GRAPH_API_VERSION = process.env.GRAPH_API_VERSION || 'v25.0';
const META_ACCESS_TOKEN = process.env.META_ACCESS_TOKEN || '';
const WHATSAPP_PHONE_NUMBER_ID = process.env.WHATSAPP_PHONE_NUMBER_ID || '1118555908004589';
const WABA_ID = process.env.WABA_ID || '1251319566775221';
const META_VERIFY_TOKEN = process.env.META_VERIFY_TOKEN || 'change_this_verify_token';
const MANAGER_WHATSAPP = normalizePhone(process.env.MANAGER_WHATSAPP || '77076669955');
const RO_SECRET = process.env.RO_SECRET || '';

const GRAPH_BASE = `https://graph.facebook.com/${GRAPH_API_VERSION}`;

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function normalizePhone(value) {
  if (!value) return '';
  let digits = String(value).replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('8')) digits = `7${digits.slice(1)}`;
  if (digits.length === 10 && digits.startsWith('7')) digits = `7${digits}`;
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
    if (cur !== undefined && cur !== null && String(cur).trim() !== '') return cur;
  }
  return fallback;
}

function findFirstPhone(obj) {
  const direct = pick(obj, [
    'client.phone', 'client_phone', 'phone', 'customer.phone', 'customer_phone',
    'lead.phone', 'appeal.phone', 'order.client.phone', 'data.client.phone',
    'data.phone', 'data.customer.phone', 'contact.phone'
  ]);
  const normalizedDirect = normalizePhone(direct);
  if (normalizedDirect) return normalizedDirect;

  const text = JSON.stringify(obj || {});
  const match = text.match(/(?:\+?7|8)[\s\-()]*\d{3}[\s\-()]*\d{3}[\s\-()]*\d{2}[\s\-()]*\d{2}/);
  return match ? normalizePhone(match[0]) : '';
}

function getEventName(payload) {
  return String(pick(payload, [
    'event', 'event_name', 'type', 'action', 'topic', 'hook', 'webhook_event', 'data.event', 'data.type', 'object.event'
  ], '')).toLowerCase();
}

function getClientName(payload) {
  return String(pick(payload, [
    'client.name', 'client_name', 'customer.name', 'customer_name',
    'lead.name', 'appeal.name', 'order.client.name', 'data.client.name',
    'data.customer.name', 'contact.name', 'name'
  ], 'Клиент')).trim();
}

function getRepairSubject(payload) {
  return String(pick(payload, [
    'subject', 'title', 'message', 'comment', 'description', 'problem',
    'lead.comment', 'appeal.comment', 'appeal.text', 'request.text',
    'data.subject', 'data.title', 'data.message', 'data.comment', 'data.description',
    'order.type', 'order.device', 'device', 'product.name', 'item.name'
  ], 'Новое обращение')).trim();
}

function getOrderNumber(payload) {
  return String(pick(payload, [
    'order.number', 'order.id', 'order_id', 'number', 'id', 'data.order.number', 'data.order.id', 'data.id'
  ], 'Без номера')).trim();
}

function getOrderStatus(payload) {
  return String(pick(payload, [
    'status.name', 'status', 'new_status', 'order.status', 'data.status', 'data.order.status'
  ], 'Статус изменён')).trim();
}

function getOrderAmount(payload) {
  const amount = pick(payload, ['amount', 'total', 'price', 'order.total', 'data.total', 'data.amount'], '');
  const currency = pick(payload, ['currency', 'order.currency', 'data.currency'], 'KZT');
  return amount ? `${amount} ${currency}` : 'уточняется';
}

function isRepairRequestCreated(payload) {
  const event = getEventName(payload);
  const text = JSON.stringify(payload || {}).toLowerCase();
  return (
    event.includes('appeal') || event.includes('lead') || event.includes('request') ||
    text.includes('обращен') || text.includes('заявк') || text.includes('lead') || text.includes('appeal')
  ) && (
    event.includes('create') || event.includes('new') || event.includes('add') || text.includes('created') || text.includes('создан')
  );
}

function isOrderAccepted(payload) {
  const event = getEventName(payload);
  const status = getOrderStatus(payload).toLowerCase();
  return event.includes('order') && (event.includes('create') || status.includes('принят') || status.includes('нов'));
}

function isOrderReady(payload) {
  const status = getOrderStatus(payload).toLowerCase();
  return status.includes('готов') || status.includes('выдач');
}

function isOrderClosed(payload) {
  const status = getOrderStatus(payload).toLowerCase();
  return status.includes('закрыт') || status.includes('выдан') || status.includes('completed') || status.includes('closed');
}

function isOrderStatusChanged(payload) {
  const event = getEventName(payload);
  return event.includes('status') || event.includes('order') || JSON.stringify(payload || {}).toLowerCase().includes('status');
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
  try { data = text ? JSON.parse(text) : {}; } catch { data = { raw: text }; }
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
    text: { preview_url: true, body: text }
  };
  return metaRequest(`${WHATSAPP_PHONE_NUMBER_ID}/messages`, body);
}

async function sendTemplate(to, name, languageCode, parameters = []) {
  const bodyParams = parameters.map((text) => ({ type: 'text', text: String(text ?? '') }));
  const body = {
    messaging_product: 'whatsapp',
    to: normalizePhone(to),
    type: 'template',
    template: {
      name,
      language: { code: languageCode },
      components: bodyParams.length ? [{ type: 'body', parameters: bodyParams }] : []
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
    wabaId: WABA_ID
  });
});

// Meta webhook verification
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

// Meta incoming messages
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
  const provided = req.query.secret || req.headers['x-ro-secret'] || req.headers['x-webhook-secret'] || req.headers['x-remonline-secret'];
  return String(provided || '') === RO_SECRET;
}

app.post('/ro/webhook', async (req, res) => {
  if (!checkRoSecret(req)) return res.status(401).json({ ok: false, error: 'Bad secret' });
  res.json({ ok: true, received: true });

  const payload = req.body || {};
  try {
    log('RemOnline webhook event:', getEventName(payload) || 'unknown');

    if (isRepairRequestCreated(payload)) {
      const clientName = getClientName(payload);
      const clientPhone = findFirstPhone(payload);
      const subject = getRepairSubject(payload);
      const clientWaLink = clientPhone ? `https://wa.me/${clientPhone}` : 'Телефон не найден';

      await sendTemplate(MANAGER_WHATSAPP, 'new_repair_request_alert', 'ru', [
        clientName,
        clientPhone ? `+${clientPhone}` : 'не указан',
        subject,
        clientWaLink
      ]);
      return log('Repair request alert sent to manager');
    }

    const clientPhone = findFirstPhone(payload);
    if (!clientPhone) return log('No client phone found, skip WhatsApp template');

    const clientName = getClientName(payload);
    const orderNumber = getOrderNumber(payload);
    const status = getOrderStatus(payload);

    if (isOrderReady(payload)) {
      await sendTemplate(clientPhone, 'order_ready', 'ru', [clientName, orderNumber, getOrderAmount(payload)]);
      return log('order_ready sent', clientPhone);
    }

    if (isOrderClosed(payload)) {
      await sendTemplate(clientPhone, 'order_review_request', 'ru', [clientName]);
      return log('order_review_request sent', clientPhone);
    }

    if (isOrderAccepted(payload)) {
      await sendTemplate(clientPhone, 'order_accepted', 'ru', [clientName, orderNumber]);
      return log('order_accepted sent', clientPhone);
    }

    if (isOrderStatusChanged(payload)) {
      await sendTemplate(clientPhone, 'order_status_changed', 'ru', [clientName, orderNumber, status]);
      return log('order_status_changed sent', clientPhone);
    }

    log('No matching RemOnline rule, skipped');
  } catch (err) {
    console.error('RO webhook handling error:', err.data || err.message || err);
  }
});

// Manual tests
app.get('/test-send', async (req, res) => {
  try {
    const to = normalizePhone(req.query.to || MANAGER_WHATSAPP);
    const text = String(req.query.text || 'Тестовый ответ от MasterProServis.kz. API-номер работает.');
    const data = await sendWhatsAppText(to, text);
    res.json({ ok: true, data });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message, meta: err.data });
  }
});

app.get('/test-template', async (req, res) => {
  try {
    const to = normalizePhone(req.query.to || MANAGER_WHATSAPP);
    const template = String(req.query.template || 'order_status_changed');
    const data = await sendTemplate(to, template, 'ru', ['Павел', '12345', 'Готов к выдаче']);
    res.json({ ok: true, data });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message, meta: err.data });
  }
});

app.get('/test-reply', async (req, res) => {
  try {
    const to = normalizePhone(req.query.to || MANAGER_WHATSAPP);
    const data = await sendWhatsAppText(to, autoReplyText());
    res.json({ ok: true, data });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message, meta: err.data });
  }
});

app.use((req, res) => {
  res.status(404).json({ ok: false, error: 'Not found' });
});

app.listen(PORT, () => {
  log(`wa-masterproservis listening on port ${PORT}`);
});
