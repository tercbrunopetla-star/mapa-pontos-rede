import { DurableObject } from "cloudflare:workers";
import { HTML } from "./html.js";

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
      ...CORS,
    },
  });
}

function text(v) {
  return String(v ?? "").trim();
}

function validId(v) {
  return /^[A-Za-z0-9_-]{1,100}$/.test(String(v || ""));
}

export class PontosStore extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.subscribers = new Set();
  }

  async ensureData() {
    const stored = await this.ctx.storage.get("pontos");
    if (Array.isArray(stored)) return stored;
    return [];
  }

  async getData() {
    return this.ensureData();
  }

  async setData(next, reason) {
    await this.ctx.storage.put("pontos", next);
    this.broadcast(next);
    return next;
  }

  broadcast(data) {
    const encoder = new TextEncoder();
    const payload = encoder.encode(`data: ${JSON.stringify(data)}\n\n`);
    const dead = [];
    for (const writer of this.subscribers) {
      try { writer.write(payload); } catch { dead.push(writer); }
    }
    for (const w of dead) {
      this.subscribers.delete(w);
      try { w.close(); } catch (_) {}
    }
  }

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS });
    }

    if (path === "/api/health" && request.method === "GET") {
      const data = await this.getData();
      return json({ ok: true, clients: this.subscribers.size, points: data.length, time: Date.now() });
    }

    if (path === "/api/pontos" && request.method === "GET") {
      return json(await this.getData());
    }

    if (path === "/api/events" && request.method === "GET") {
      const { readable, writable } = new TransformStream();
      const writer = writable.getWriter();
      const encoder = new TextEncoder();
      const write = async (str) => { await writer.write(encoder.encode(str)); };

      const data = await this.getData();
      await write("retry: 1500\n");
      await write(`data: ${JSON.stringify(data)}\n\n`);
      this.subscribers.add(writer);

      const heartbeat = setInterval(async () => {
        try { await write(`: hb ${Date.now()}\n\n`); }
        catch { clearInterval(heartbeat); this.subscribers.delete(writer); }
      }, 12000);

      request.signal.addEventListener("abort", () => {
        clearInterval(heartbeat);
        this.subscribers.delete(writer);
        try { writer.close(); } catch (_) {}
      });

      return new Response(readable, {
        headers: {
          "Content-Type": "text/event-stream; charset=utf-8",
          "Cache-Control": "no-cache, no-store",
          Connection: "keep-alive",
          ...CORS,
        },
      });
    }

    if (path === "/api/pontos" && request.method === "POST") {
      let body;
      try { body = await request.json(); } catch { return json({ error: "JSON inválido" }, 400); }
      const setor = text(body.setor), lado = text(body.lado), sala = text(body.sala).toUpperCase();
      if (!setor || !lado || !sala) return json({ error: "setor, lado e sala são obrigatórios" }, 400);
      const current = await this.getData();
      const id = validId(body.id) ? String(body.id) : "p_" + Date.now() + "_" + Math.random().toString(36).slice(2, 8);
      if (current.some((p) => p.id === id)) return json({ error: "id já existe" }, 409);
      const item = { id, setor, lado, sala, porta: text(body.porta), switch: text(body.switch), obs: text(body.obs), createdAt: Date.now() };
      await this.setData(current.concat(item), "POST " + id);
      return json(item, 201);
    }

    const match = path.match(/^\/api\/pontos\/([^/]+)$/);
    if (match) {
      const id = decodeURIComponent(match[1]);
      if (request.method === "DELETE") {
        const current = await this.getData();
        const idx = current.findIndex((p) => p.id === id);
        if (idx < 0) return json({ error: "ponto não encontrado" }, 404);
        const next = current.slice(); next.splice(idx, 1);
        await this.setData(next, "DELETE " + id);
        return json({ ok: true });
      }
      if (request.method === "PUT") {
        let body;
        try { body = await request.json(); } catch { return json({ error: "JSON inválido" }, 400); }
        const current = await this.getData();
        const idx = current.findIndex((p) => p.id === id);
        if (idx < 0) return json({ error: "ponto não encontrado" }, 404);
        const old = current[idx];
        const updated = {
          ...old,
          setor: text(body.setor) || old.setor,
          lado: text(body.lado) || old.lado,
          sala: text(body.sala || old.sala).toUpperCase(),
          porta: text(body.porta),
          switch: text(body.switch),
          obs: text(body.obs),
          updatedAt: Date.now(),
        };
        const next = current.slice(); next[idx] = updated;
        await this.setData(next, "PUT " + id);
        return json(updated);
      }
    }

    return json({ error: "not found" }, 404);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname.startsWith("/api/")) {
      const id = env.PONTOS.idFromName("shared");
      return env.PONTOS.get(id).fetch(request);
    }

    // Serve the SPA for everything else
    return new Response(HTML, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-cache",
      },
    });
  },
};
