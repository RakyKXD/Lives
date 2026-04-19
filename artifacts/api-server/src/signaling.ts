import { WebSocketServer, WebSocket } from "ws";
import type { Server } from "http";
import { logger } from "./lib/logger";

interface Peer {
  ws: WebSocket;
  roomId: string;
  peerId: string;
  role: "broadcaster" | "viewer";
}

// roomId -> { broadcasterId, viewerIds }
const rooms = new Map<string, { broadcasterId: string; viewerIds: Set<string> }>();
const peers = new Map<string, Peer>();

export function setupSignaling(server: Server) {
  const wss = new WebSocketServer({ server, path: "/api/ws" });

  wss.on("connection", (ws: WebSocket) => {
    let myPeerId: string | null = null;
    let myRoomId: string | null = null;
    let myRole: "broadcaster" | "viewer" | null = null;

    ws.on("message", (data: Buffer) => {
      try {
        const msg = JSON.parse(data.toString()) as Record<string, unknown>;

        // ── JOIN ──────────────────────────────────────────────────────────────
        if (msg["type"] === "join") {
          myPeerId = msg["peerId"] as string;
          myRoomId = msg["roomId"] as string;
          myRole = msg["role"] as "broadcaster" | "viewer";

          peers.set(myPeerId, { ws, roomId: myRoomId, peerId: myPeerId, role: myRole });

          if (myRole === "broadcaster") {
            if (rooms.has(myRoomId)) {
              // Room already has a broadcaster
              ws.send(JSON.stringify({ type: "error", message: "Room already has a broadcaster" }));
              return;
            }
            rooms.set(myRoomId, { broadcasterId: myPeerId, viewerIds: new Set() });
            ws.send(JSON.stringify({ type: "joined", role: "broadcaster", viewers: 0 }));
            logger.info({ peerId: myPeerId, roomId: myRoomId }, "Broadcaster joined");

          } else {
            // Viewer joining
            const room = rooms.get(myRoomId);
            if (!room) {
              ws.send(JSON.stringify({ type: "error", message: "Room not found or no broadcaster yet" }));
              return;
            }

            room.viewerIds.add(myPeerId);
            ws.send(JSON.stringify({ type: "joined", role: "viewer", broadcasterId: room.broadcasterId }));

            // Notify broadcaster about new viewer
            const broadcaster = peers.get(room.broadcasterId);
            if (broadcaster && broadcaster.ws.readyState === WebSocket.OPEN) {
              broadcaster.ws.send(JSON.stringify({ type: "viewer-joined", viewerId: myPeerId }));
            }

            // Broadcast updated viewer count to all
            broadcastViewerCount(myRoomId);
            logger.info({ peerId: myPeerId, roomId: myRoomId, viewers: room.viewerIds.size }, "Viewer joined");
          }

        // ── SIGNALING (offer / answer / ice-candidate) ────────────────────────
        } else if (msg["type"] === "offer" || msg["type"] === "answer" || msg["type"] === "ice-candidate") {
          const target = peers.get(msg["to"] as string);
          if (target && target.ws.readyState === WebSocket.OPEN) {
            target.ws.send(JSON.stringify({ ...msg, from: myPeerId }));
          }

        // ── LIST ROOMS ────────────────────────────────────────────────────────
        } else if (msg["type"] === "list-rooms") {
          const list = [...rooms.entries()].map(([roomId, room]) => ({
            roomId,
            viewers: room.viewerIds.size,
          }));
          ws.send(JSON.stringify({ type: "rooms", rooms: list }));

        // ── LEAVE ─────────────────────────────────────────────────────────────
        } else if (msg["type"] === "leave") {
          cleanup();
        }
      } catch (err) {
        logger.error({ err }, "Signaling message error");
      }
    });

    ws.on("close", cleanup);
    ws.on("error", (err) => {
      logger.error({ err }, "WebSocket error");
      cleanup();
    });

    function broadcastViewerCount(roomId: string) {
      const room = rooms.get(roomId);
      if (!room) return;
      const count = room.viewerIds.size;
      const countMsg = JSON.stringify({ type: "viewer-count", count });

      const broadcaster = peers.get(room.broadcasterId);
      if (broadcaster && broadcaster.ws.readyState === WebSocket.OPEN) {
        broadcaster.ws.send(countMsg);
      }
      room.viewerIds.forEach((vid) => {
        const v = peers.get(vid);
        if (v && v.ws.readyState === WebSocket.OPEN) v.ws.send(countMsg);
      });
    }

    function cleanup() {
      if (!myPeerId || !myRoomId || !myRole) return;

      const room = rooms.get(myRoomId);
      if (!room) {
        peers.delete(myPeerId);
        return;
      }

      if (myRole === "broadcaster") {
        // Notify all viewers the stream ended
        room.viewerIds.forEach((vid) => {
          const v = peers.get(vid);
          if (v && v.ws.readyState === WebSocket.OPEN) {
            v.ws.send(JSON.stringify({ type: "stream-ended" }));
          }
        });
        rooms.delete(myRoomId);
        logger.info({ peerId: myPeerId, roomId: myRoomId }, "Broadcaster left — room closed");
      } else {
        room.viewerIds.delete(myPeerId);
        // Notify broadcaster a viewer left
        const broadcaster = peers.get(room.broadcasterId);
        if (broadcaster && broadcaster.ws.readyState === WebSocket.OPEN) {
          broadcaster.ws.send(JSON.stringify({ type: "viewer-left", viewerId: myPeerId }));
        }
        broadcastViewerCount(myRoomId);
        logger.info({ peerId: myPeerId, roomId: myRoomId, viewers: room.viewerIds.size }, "Viewer left");
      }

      peers.delete(myPeerId);
      myPeerId = null;
      myRoomId = null;
      myRole = null;
    }
  });

  logger.info("WebRTC signaling server ready at /api/ws");
  return wss;
}
