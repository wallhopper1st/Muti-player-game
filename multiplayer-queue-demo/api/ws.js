const http = require("http");
const { WebSocketServer } = require("ws");

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("WebSocket endpoint ready");
});

const wss = new WebSocketServer({ server });

// Note: On Vercel, each function instance has its own memory.
// For multi-instance room/queue state across all users, pair with Redis.
const waitingQueue = new Set();

function broadcastQueueState() {
  const count = waitingQueue.size;
  for (const client of waitingQueue) {
    if (client.readyState === client.OPEN) {
      client.send(JSON.stringify({ type: "queue_update", playersInQueue: count }));
    }
  }
}

function tryMatchPlayers() {
  if (waitingQueue.size < 2) return;

  const iterator = waitingQueue.values();
  const p1 = iterator.next().value;
  const p2 = iterator.next().value;

  waitingQueue.delete(p1);
  waitingQueue.delete(p2);

  const matchId = "match_" + Math.random().toString(36).slice(2, 8);

  if (p1.readyState === p1.OPEN) {
    p1.send(JSON.stringify({ type: "matched", matchId, opponent: "Human Player 2", role: "p1" }));
  }
  if (p2.readyState === p2.OPEN) {
    p2.send(JSON.stringify({ type: "matched", matchId, opponent: "Human Player 1", role: "p2" }));
  }

  broadcastQueueState();
}

wss.on("connection", (ws) => {
  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.type === "join_queue") {
      waitingQueue.add(ws);
      broadcastQueueState();
      tryMatchPlayers();
    } else if (msg.type === "leave_queue") {
      waitingQueue.delete(ws);
      broadcastQueueState();
    } else if (msg.type === "match_event") {
      // Relay gameplay/state updates to other connected clients on this instance
      for (const client of wss.clients) {
        if (client !== ws && client.readyState === client.OPEN) {
          client.send(JSON.stringify({ type: "peer_event", payload: msg.payload }));
        }
      }
    }
  });

  ws.on("close", () => {
    waitingQueue.delete(ws);
    broadcastQueueState();
  });
});

module.exports = server;
