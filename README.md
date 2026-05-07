# wa-masterproservis

Backend bridge for MasterProServis.kz:

- `/health` — health check
- `/wa/webhook` — Meta WhatsApp webhook
- `/ro/webhook` — RemOnline webhook
- `/test-send` — manual text send test
- `/test-reply` — manual auto-reply test
- `/test-template` — manual template test

## Timeweb settings

Use Backend → Node.js 24 → Express.

Suggested fields:

- Build command: empty
- Dependencies command: `npm install`
- Start command: `npm start`
- Health check path: `/health`
- App name: `wa-masterproservis`

## Required env variables

Copy `.env.example` values into Timeweb variables, but use a clean real token.

Important IDs:

- `WHATSAPP_PHONE_NUMBER_ID=1118555908004589`
- `WABA_ID=1251319566775221`
- `MANAGER_WHATSAPP=77076669955`

Never commit the real token into Git.
