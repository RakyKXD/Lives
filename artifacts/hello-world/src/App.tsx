import { useState, useEffect, useRef, useCallback } from "react";

const ICE_SERVERS = [
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:stun1.l.google.com:19302" },
];

function generateId() {
  return Math.random().toString(36).slice(2, 8).toUpperCase();
}

function getWsUrl() {
  const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${window.location.host}/api/ws`;
}

type Screen = "home" | "broadcaster" | "viewer" | "watch";

export default function App() {
  const [screen, setScreen] = useState<Screen>("home");
  const [roomId, setRoomId] = useState("");
  const [viewerCount, setViewerCount] = useState(0);
  const [viewers, setViewers] = useState<Set<string>>(new Set());
  const [statusMsg, setStatusMsg] = useState("");
  const [streamEnded, setStreamEnded] = useState(false);
  const [liveRooms, setLiveRooms] = useState<{ roomId: string; viewers: number }[]>([]);

  const localVideoRef = useRef<HTMLVideoElement>(null);
  const remoteVideoRef = useRef<HTMLVideoElement>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const streamRef = useRef<MediaStream | null>(null);
  // broadcaster: Map of viewerId -> RTCPeerConnection
  const pcsRef = useRef<Map<string, RTCPeerConnection>>(new Map());
  // viewer: single RTCPeerConnection
  const pcRef = useRef<RTCPeerConnection | null>(null);
  const myId = useRef(generateId());

  // ── WebSocket connect ───────────────────────────────────────────────────────
  const connectWs = useCallback(
    (onOpen: (ws: WebSocket) => void): Promise<WebSocket> => {
      return new Promise((resolve, reject) => {
        const ws = new WebSocket(getWsUrl());
        wsRef.current = ws;
        ws.onopen = () => {
          onOpen(ws);
          resolve(ws);
        };
        ws.onerror = () => reject(new Error("WebSocket error"));
      });
    },
    []
  );

  // ── Create RTCPeerConnection for broadcaster→viewer ─────────────────────────
  const createBroadcasterPc = useCallback(
    (viewerId: string, stream: MediaStream): RTCPeerConnection => {
      const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
      pcsRef.current.set(viewerId, pc);

      stream.getTracks().forEach((t) => pc.addTrack(t, stream));

      pc.onicecandidate = (e) => {
        if (e.candidate) {
          wsRef.current?.send(
            JSON.stringify({ type: "ice-candidate", to: viewerId, candidate: e.candidate })
          );
        }
      };

      pc.onconnectionstatechange = () => {
        if (pc.connectionState === "disconnected" || pc.connectionState === "failed") {
          pc.close();
          pcsRef.current.delete(viewerId);
        }
      };

      return pc;
    },
    []
  );

  // ── Broadcaster: handle incoming viewer ─────────────────────────────────────
  const handleViewerJoined = useCallback(
    async (viewerId: string) => {
      if (!streamRef.current) return;
      const pc = createBroadcasterPc(viewerId, streamRef.current);
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      wsRef.current?.send(JSON.stringify({ type: "offer", to: viewerId, sdp: offer }));
      setViewers((prev) => new Set([...prev, viewerId]));
    },
    [createBroadcasterPc]
  );

  // ── Broadcaster message handler ─────────────────────────────────────────────
  const handleBroadcasterMsg = useCallback(
    async (msg: Record<string, unknown>) => {
      if (msg["type"] === "viewer-joined") {
        await handleViewerJoined(msg["viewerId"] as string);
      } else if (msg["type"] === "answer") {
        const pc = pcsRef.current.get(msg["from"] as string);
        if (pc) await pc.setRemoteDescription(new RTCSessionDescription(msg["sdp"] as RTCSessionDescriptionInit));
      } else if (msg["type"] === "ice-candidate") {
        const pc = pcsRef.current.get(msg["from"] as string);
        if (pc && msg["candidate"])
          await pc.addIceCandidate(new RTCIceCandidate(msg["candidate"] as RTCIceCandidateInit));
      } else if (msg["type"] === "viewer-left") {
        const vid = msg["viewerId"] as string;
        pcsRef.current.get(vid)?.close();
        pcsRef.current.delete(vid);
        setViewers((prev) => {
          const next = new Set(prev);
          next.delete(vid);
          return next;
        });
      } else if (msg["type"] === "viewer-count") {
        setViewerCount(msg["count"] as number);
      }
    },
    [handleViewerJoined]
  );

  // ── Viewer message handler ──────────────────────────────────────────────────
  const handleViewerMsg = useCallback(async (msg: Record<string, unknown>) => {
    const pc = pcRef.current;
    if (!pc) return;

    if (msg["type"] === "offer") {
      await pc.setRemoteDescription(new RTCSessionDescription(msg["sdp"] as RTCSessionDescriptionInit));
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      wsRef.current?.send(JSON.stringify({ type: "answer", to: msg["from"], sdp: answer }));
    } else if (msg["type"] === "ice-candidate" && msg["candidate"]) {
      await pc.addIceCandidate(new RTCIceCandidate(msg["candidate"] as RTCIceCandidateInit));
    } else if (msg["type"] === "stream-ended") {
      setStreamEnded(true);
      setStatusMsg("El directo ha terminado");
    } else if (msg["type"] === "viewer-count") {
      setViewerCount(msg["count"] as number);
    }
  }, []);

  // ── Start broadcasting ──────────────────────────────────────────────────────
  const startBroadcast = async (mode: "camera" | "screen") => {
    const id = generateId();
    setRoomId(id);

    let stream: MediaStream;
    try {
      stream =
        mode === "camera"
          ? await navigator.mediaDevices.getUserMedia({ video: true, audio: true })
          : await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true });
    } catch {
      setStatusMsg("Permiso denegado al dispositivo");
      return;
    }

    streamRef.current = stream;
    if (localVideoRef.current) localVideoRef.current.srcObject = stream;

    try {
      const ws = await connectWs((ws) => {
        ws.send(JSON.stringify({ type: "join", roomId: id, peerId: myId.current, role: "broadcaster" }));
      });

      ws.onmessage = (e) => handleBroadcasterMsg(JSON.parse(e.data) as Record<string, unknown>);
      ws.onclose = () => setStatusMsg("Desconectado del servidor");

      setScreen("broadcaster");
      setStatusMsg("Directo activo");
    } catch {
      setStatusMsg("No se pudo conectar al servidor");
      stream.getTracks().forEach((t) => t.stop());
    }
  };

  // ── Watch a stream ──────────────────────────────────────────────────────────
  const watchStream = async (targetRoomId: string) => {
    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    pcRef.current = pc;

    pc.ontrack = (e) => {
      if (remoteVideoRef.current) remoteVideoRef.current.srcObject = e.streams[0];
    };

    pc.onicecandidate = (e) => {
      if (e.candidate) {
        wsRef.current?.send(JSON.stringify({ type: "ice-candidate", to: broadcasterId.current, candidate: e.candidate }));
      }
    };

    try {
      const ws = await connectWs((ws) => {
        ws.send(JSON.stringify({ type: "join", roomId: targetRoomId, peerId: myId.current, role: "viewer" }));
      });

      ws.onmessage = (e) => handleViewerMsg(JSON.parse(e.data) as Record<string, unknown>);
      ws.onclose = () => setStatusMsg("Desconectado");

      setRoomId(targetRoomId);
      setScreen("watch");
      setStatusMsg("Conectando...");
    } catch {
      setStatusMsg("No se pudo conectar al directo");
    }
  };

  const broadcasterId = useRef<string>("");

  // Set broadcasterId when viewer joins
  useEffect(() => {
    if (screen !== "watch") return;
    const ws = wsRef.current;
    if (!ws) return;
    const orig = ws.onmessage;
    ws.onmessage = async (e) => {
      const msg = JSON.parse(e.data) as Record<string, unknown>;
      if (msg["type"] === "joined" && msg["role"] === "viewer") {
        broadcasterId.current = msg["broadcasterId"] as string;
      }
      await handleViewerMsg(msg);
    };
    return () => { if (ws) ws.onmessage = orig; };
  }, [screen, handleViewerMsg]);

  // ── List live rooms ─────────────────────────────────────────────────────────
  const fetchRooms = useCallback(() => {
    const ws = new WebSocket(getWsUrl());
    ws.onopen = () => ws.send(JSON.stringify({ type: "list-rooms" }));
    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data) as Record<string, unknown>;
      if (msg["type"] === "rooms") {
        setLiveRooms(msg["rooms"] as { roomId: string; viewers: number }[]);
      }
      ws.close();
    };
  }, []);

  // ── Stop ────────────────────────────────────────────────────────────────────
  const stop = useCallback(() => {
    wsRef.current?.send(JSON.stringify({ type: "leave" }));
    wsRef.current?.close();
    pcsRef.current.forEach((pc) => pc.close());
    pcsRef.current.clear();
    pcRef.current?.close();
    pcRef.current = null;
    streamRef.current?.getTracks().forEach((t) => t.stop());
    streamRef.current = null;
    if (localVideoRef.current) localVideoRef.current.srcObject = null;
    if (remoteVideoRef.current) remoteVideoRef.current.srcObject = null;
    setScreen("home");
    setViewerCount(0);
    setViewers(new Set());
    setStreamEnded(false);
    setStatusMsg("");
    myId.current = generateId();
  }, []);

  useEffect(() => () => { stop(); }, [stop]);

  // ── UI ──────────────────────────────────────────────────────────────────────
  return (
    <div style={{ minHeight: "100vh", background: "#09090b", color: "#fafafa", fontFamily: "system-ui, sans-serif", display: "flex", flexDirection: "column", alignItems: "center", padding: "32px 16px", gap: 24 }}>
      <h1 style={{ margin: 0, fontSize: "1.75rem", fontWeight: 700 }}>Directos WebRTC</h1>

      {/* HOME */}
      {screen === "home" && (
        <div style={{ width: "100%", maxWidth: 480, display: "flex", flexDirection: "column", gap: 16 }}>
          <div style={{ background: "#18181b", borderRadius: 12, padding: 20, display: "flex", flexDirection: "column", gap: 12 }}>
            <p style={{ margin: 0, fontWeight: 600, color: "#a1a1aa" }}>Iniciar un directo</p>
            <div style={{ display: "flex", gap: 10 }}>
              <Btn color="#dc2626" onClick={() => startBroadcast("camera")}><CamIcon /> Camara</Btn>
              <Btn color="#7c3aed" onClick={() => startBroadcast("screen")}><ScreenIcon /> Pantalla</Btn>
            </div>
          </div>

          <div style={{ background: "#18181b", borderRadius: 12, padding: 20, display: "flex", flexDirection: "column", gap: 12 }}>
            <p style={{ margin: 0, fontWeight: 600, color: "#a1a1aa" }}>Ver un directo</p>
            <div style={{ display: "flex", gap: 8 }}>
              <input
                value={roomId}
                onChange={(e) => setRoomId(e.target.value.toUpperCase())}
                placeholder="ID del directo"
                style={{ flex: 1, background: "#09090b", border: "1px solid #27272a", borderRadius: 8, padding: "10px 14px", color: "#fafafa", fontSize: "0.95rem", outline: "none", letterSpacing: 2, textTransform: "uppercase" }}
              />
              <Btn color="#2563eb" onClick={() => watchStream(roomId)}>Ver</Btn>
            </div>
            <button onClick={fetchRooms} style={{ background: "none", border: "1px solid #27272a", borderRadius: 8, padding: "8px", color: "#71717a", cursor: "pointer", fontSize: "0.8rem" }}>
              Buscar directos activos
            </button>
            {liveRooms.length > 0 && (
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {liveRooms.map((r) => (
                  <div key={r.roomId} onClick={() => watchStream(r.roomId)} style={{ background: "#09090b", borderRadius: 8, padding: "10px 14px", cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center", border: "1px solid #27272a" }}>
                    <span style={{ fontWeight: 600, letterSpacing: 2 }}>{r.roomId}</span>
                    <span style={{ color: "#dc2626", fontSize: "0.8rem" }}>🔴 {r.viewers} espectadores</span>
                  </div>
                ))}
              </div>
            )}
            {liveRooms.length === 0 && statusMsg && <p style={{ margin: 0, color: "#71717a", fontSize: "0.85rem" }}>{statusMsg}</p>}
          </div>
        </div>
      )}

      {/* BROADCASTER */}
      {screen === "broadcaster" && (
        <div style={{ width: "100%", maxWidth: 720, display: "flex", flexDirection: "column", gap: 16 }}>
          <div style={{ background: "#18181b", borderRadius: 12, padding: "14px 20px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <div>
              <span style={{ background: "#dc2626", borderRadius: 6, padding: "3px 10px", fontSize: "0.78rem", fontWeight: 700, marginRight: 12 }}>EN DIRECTO</span>
              <span style={{ color: "#a1a1aa", fontSize: "0.9rem" }}>
                ID: <strong style={{ color: "#fafafa", letterSpacing: 2 }}>{roomId}</strong>
              </span>
            </div>
            <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
              <span style={{ color: "#dc2626", fontSize: "0.9rem" }}>👁 {viewerCount} espectadores</span>
              <Btn color="#dc2626" onClick={stop}>Terminar</Btn>
            </div>
          </div>
          <video ref={localVideoRef} autoPlay playsInline muted style={{ width: "100%", borderRadius: 12, background: "#000", aspectRatio: "16/9", objectFit: "cover" }} />
          <p style={{ margin: 0, color: "#71717a", fontSize: "0.85rem", textAlign: "center" }}>
            Comparte este ID con tus espectadores: <strong style={{ color: "#fafafa", letterSpacing: 2 }}>{roomId}</strong>
          </p>
        </div>
      )}

      {/* WATCH */}
      {screen === "watch" && (
        <div style={{ width: "100%", maxWidth: 720, display: "flex", flexDirection: "column", gap: 16 }}>
          <div style={{ background: "#18181b", borderRadius: 12, padding: "14px 20px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <div>
              {!streamEnded && <span style={{ background: "#dc2626", borderRadius: 6, padding: "3px 10px", fontSize: "0.78rem", fontWeight: 700, marginRight: 12 }}>EN DIRECTO</span>}
              <span style={{ color: "#a1a1aa", fontSize: "0.9rem" }}>
                Sala: <strong style={{ color: "#fafafa", letterSpacing: 2 }}>{roomId}</strong>
              </span>
            </div>
            <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
              <span style={{ color: "#71717a", fontSize: "0.9rem" }}>👁 {viewerCount}</span>
              <Btn color="#27272a" onClick={stop}>Salir</Btn>
            </div>
          </div>
          {streamEnded ? (
            <div style={{ aspectRatio: "16/9", background: "#18181b", borderRadius: 12, display: "flex", alignItems: "center", justifyContent: "center", flexDirection: "column", gap: 8 }}>
              <p style={{ margin: 0, color: "#71717a" }}>El directo ha terminado</p>
              <Btn color="#27272a" onClick={stop}>Volver</Btn>
            </div>
          ) : (
            <video ref={remoteVideoRef} autoPlay playsInline style={{ width: "100%", borderRadius: 12, background: "#000", aspectRatio: "16/9", objectFit: "cover" }} />
          )}
          {statusMsg && <p style={{ margin: 0, color: "#71717a", fontSize: "0.85rem", textAlign: "center" }}>{statusMsg}</p>}
        </div>
      )}
    </div>
  );
}

function Btn({ children, onClick, color }: { children: React.ReactNode; onClick: () => void; color: string }) {
  return (
    <button onClick={onClick} style={{ background: color, border: "none", borderRadius: 8, padding: "10px 18px", color: "#fff", cursor: "pointer", fontWeight: 600, fontSize: "0.9rem", display: "flex", alignItems: "center", gap: 8, whiteSpace: "nowrap" }}>
      {children}
    </button>
  );
}

function CamIcon() {
  return <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M23 7l-7 5 7 5V7z" /><rect x="1" y="5" width="15" height="14" rx="2" /></svg>;
}

function ScreenIcon() {
  return <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><rect x="2" y="3" width="20" height="14" rx="2" /><line x1="8" y1="21" x2="16" y2="21" /><line x1="12" y1="17" x2="12" y2="21" /></svg>;
}
