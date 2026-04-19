import { WebSocketServer, WebSocket } from "ws";
import type { Server } from "http";
import { logger } from "./lib/logger";

interface Peer {
  ws: WebSocket;
  roomId: string;
  peerId: string;
}

const rooms = new Map<string, Set<string>>();
const peers = new Map<string, Peer>();

export function setupSignaling(server: Server) {
  const wss = new WebSocketServer({ server, path: "/api/ws" });

  wss.on("connection", (ws: WebSocket) => {
    let myPeerId: string | null = null;
    let myRoomId: string | null = null;

    ws.on("message", (data: Buffer) => {
      try {
        const msg = JSON.parse(data.toString()) as Record<string, unknown>;

        if (msg["type"] === "join") {
          myPeerId = msg["peerId"] as string;
          myRoomId = msg["roomId"] as string;

          peers.set(myPeerId, { ws, roomId: myRoomId, peerId: myPeerId });

          if (!rooms.has(myRoomId)) {
            rooms.set(myRoomId, new Set());
          }

          const room = rooms.get(myRoomId)!;
          const existingPeers = [...room];

          existingPeers.forEach((otherId) => {
            const other = peers.get(otherId);
            if (other && other.ws.readyState === WebSocket.OPEN) {
              other.ws.send(
                JSON.stringify({ type: "peer-joined", peerId: myPeerId })
              );
            }
          });

          room.add(myPeerId);

          ws.send(
            JSON.stringify({ type: "joined", peers: existingPeers })
          );

          logger.info({ peerId: myPeerId, roomId: myRoomId, size: room.size }, "Peer joined");
        } else if (
          msg["type"] === "offer" ||
          msg["type"] === "answer" ||
          msg["type"] === "ice-candidate"
        ) {
          const target = peers.get(msg["to"] as string);
          if (target && target.ws.readyState === WebSocket.OPEN) {
            target.ws.send(JSON.stringify({ ...msg, from: myPeerId }));
          }
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

    function cleanup() {
      if (!myPeerId || !myRoomId) return;

      const room = rooms.get(myRoomId);
      if (room) {
        room.delete(myPeerId);
        room.forEach((otherId) => {
          const other = peers.get(otherId);
          if (other && other.ws.readyState === WebSocket.OPEN) {
            other.ws.send(JSON.stringify({ type: "peer-left", peerId: myPeerId }));
          }
        });
        if (room.size === 0) rooms.delete(myRoomId);
      }

      peers.delete(myPeerId);
      logger.info({ peerId: myPeerId, roomId: myRoomId }, "Peer left");
      myPeerId = null;
      myRoomId = null;
    }
  });

  logger.info("WebRTC signaling server ready at /api/ws");
  return wss;
}
