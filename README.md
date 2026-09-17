# Mapa de Pontos de Rede — Cloudflare

Versão 100% Cloudflare Workers + Durable Objects.

## O que mudou

- Persistência em **Durable Object** (consistência forte, sem race conditions)
- SSE em tempo real mantido
- Operações de criar / editar / excluir atômicas
- Seed inicial com os 143 pontos originais
- Tracing habilitado (Metadata only + 100% sampling)
- UX mobile melhorada (botões maiores, modal adaptado, safe-area)

## Como rodar localmente

```bash
npm install
npx wrangler login   # só na primeira vez
npm run dev
```

Abra o endereço que o Wrangler mostrar (geralmente http://localhost:8787).

## Deploy

```bash
npm run deploy
```

Depois do deploy o site fica em:
`https://mapa-pontos-rede.<seu-subdominio>.workers.dev`

## Estrutura

- `src/index.js` — Worker + Durable Object (fonte única da verdade)
- `src/seed.js` — dados iniciais
- `public/index.html` — frontend
- `wrangler.toml` — configuração Cloudflare + tracing

## API (igual à original)

- `GET  /api/health`
- `GET  /api/pontos`
- `GET  /api/events`   (SSE)
- `POST /api/pontos`
- `PUT  /api/pontos/:id`
- `DELETE /api/pontos/:id`
