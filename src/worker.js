import { DurableObject } from "cloudflare:workers";

export class Counter extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.items = null;
  }

  async loadItems() {
    if (this.items === null) {
      this.items = (await this.ctx.storage.get("items")) ?? [];
    }
    return this.items;
  }

  async saveItems() {
    await this.ctx.storage.put("items", this.items);
  }

  broadcast(message) {
    const text = JSON.stringify(message);
    for (const ws of this.ctx.getWebSockets()) {
      try {
        ws.send(text);
      } catch {
        // ignore — closing sockets will be cleaned up
      }
    }
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.ctx.acceptWebSocket(server);

    const items = await this.loadItems();
    server.send(JSON.stringify({ type: "state", items }));

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    const items = await this.loadItems();

    switch (msg.type) {
      case "add": {
        const name = (msg.name ?? "").toString().trim();
        if (!name) return;
        items.push({
          id: crypto.randomUUID(),
          name,
          count: 0,
          createdAt: Date.now(),
        });
        break;
      }
      case "tap": {
        const item = items.find((i) => i.id === msg.id);
        if (!item) return;
        item.count += 1;
        break;
      }
      case "untap": {
        const item = items.find((i) => i.id === msg.id);
        if (!item) return;
        item.count = Math.max(0, item.count - 1);
        break;
      }
      case "set": {
        const item = items.find((i) => i.id === msg.id);
        if (!item) return;
        const n = Number(msg.count);
        if (!Number.isFinite(n) || n < 0) return;
        item.count = Math.floor(n);
        break;
      }
      case "rename": {
        const item = items.find((i) => i.id === msg.id);
        if (!item) return;
        const name = (msg.name ?? "").toString().trim();
        if (!name) return;
        item.name = name;
        break;
      }
      case "delete": {
        const idx = items.findIndex((i) => i.id === msg.id);
        if (idx === -1) return;
        items.splice(idx, 1);
        break;
      }
      case "resetAll": {
        for (const item of items) item.count = 0;
        break;
      }
      case "clearAll": {
        items.length = 0;
        break;
      }
      default:
        return;
    }

    await this.saveItems();
    this.broadcast({ type: "state", items });
  }

  async webSocketClose(ws) {
    try {
      ws.close();
    } catch {}
  }

  async webSocketError(ws) {
    try {
      ws.close();
    } catch {}
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/ws") {
      if (request.headers.get("Upgrade") !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }
      // Single shared room — everyone counts in the same place.
      const id = env.COUNTER.idFromName("warehouse");
      const stub = env.COUNTER.get(id);
      return stub.fetch(request);
    }

    // Everything else is served from the ./public static assets.
    return env.ASSETS.fetch(request);
  },
};
