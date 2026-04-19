import { useState, useEffect, useRef, useCallback } from "react";

const ICE_SERVERS = [
  { urls: "stun:stun.l.google.com:19302" },
  { urls: "stun:stun1.l.google.com:19302" },
];

function generateId() {
  return Math.random().toString(36).slice(2, 9);
}

function getWsUrl() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${window.location.host}/api/ws`;
}

type Status = "idle" | "joining" | "waiting" | "negotiating" | "connected" | "error";

export default function App() {
  const [roomId, setRoomId] = useState("");
  const [joined, setJoined] = useState(false);
  const [status, setStatus] = useState<Status>("idle");
  const [statusMsg, setStatusMsg] = useState("Introduce una sala y selecciona el modo");
  const [hasRemote, setHasRemote] = useState(false);

  const localVideoRef = useRef<HTMLVideoElement>(null);
  const remoteVideoRef = useRef<HTMLVideoElement>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const pcRef = useRef<RTCPeerConnection | null>(null);
  const streamRef = useRef<MediaStream | null>(null);
  const myId = useRef(generateId());
  const remoteId = useRef<string | null>(null);

  const sendSignal = (msg: Record<string, unknown>) => {
    wsRef.current?.send(JSON.stringify(msg));
  };

  const createPc = useCallback((stream: MediaStream) => {
    const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
    pcRef.current = pc;

    stream.getTracks().forEach((t) => pc.addTrack(t, stream));

    pc.onicecandidate = (e) => {
      if (e.candidate && remoteId.current) {
        sendSignal({ type: "ice-candidate", to: remoteId.current, candidate: e.candidate });
      }
    };

    pc.ontrack = (e) => {
      if (remoteVideoRef.current) {
        remoteVideoRef.current.srcObject = e.streams[0];
      }
      setHasRemote(true);
      setStatus("connected");
      setStatusMsg("Conectado");
    };

    pc.onconnectionstatechange = () => {
      if (pc.connectionState === "disconnected" || pc.connectionState === "failed") {
        setHasRemote(false);
        setStatus("waiting");
        setStatusMsg("Par desconectado. Esperando nuevo participante...");
        if (remoteVideoRef.current) remoteVideoRef.current.srcObject = null;
      }
    };

    return pc;
  }, []);

  const handleMsg = useCallback(
    async (msg: Record<string, unknown>) => {
      const pc = pcRef.current;

      if (msg["type"] === "joined") {
        const existingPeers = msg["peers"] as string[];
        if (existingPeers.length > 0) {
          remoteId.current = existingPeers[0];
          setStatus("negotiating");
          setStatusMsg("Par encontrado, esperando oferta...");
        } else {
          setStatus("waiting");
          setStatusMsg("Sala creada. Comparte el ID con otro participante.");
        }
      } else if (msg["type"] === "peer-joined" && pc) {
        remoteId.current = msg["peerId"] as string;
        setStatus("negotiating");
        setStatusMsg("Par conectado, iniciando negociación...");
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        sendSignal({ type: "offer", to: remoteId.current, sdp: offer });
      } else if (msg["type"] === "offer" && pc) {
        remoteId.current = msg["from"] as string;
        await pc.setRemoteDescription(
          new RTCSessionDescription(msg["sdp"] as RTCSessionDescriptionInit)
        );
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        sendSignal({ type: "answer", to: remoteId.current, sdp: answer });
        setStatus("negotiating");
        setStatusMsg("Respuesta enviada...");
      } else if (msg["type"] === "answer" && pc) {
        await pc.setRemoteDescription(
          new RTCSessionDescription(msg["sdp"] as RTCSessionDescriptionInit)
        );
      } else if (msg["type"] === "ice-candidate" && pc && msg["candidate"]) {
        await pc.addIceCandidate(
          new RTCIceCandidate(msg["candidate"] as RTCIceCandidateInit)
        );
      } else if (msg["type"] === "peer-left") {
        setHasRemote(false);
        setStatus("waiting");
        setStatusMsg("El par se desconectó. Esperando nuevo participante...");
        if (remoteVideoRef.current) remoteVideoRef.current.srcObject = null;
        remoteId.current = null;
      }
    },
    [createPc]
  );

  const start = async (mode: "camera" | "screen") => {
    if (!roomId.trim()) {
      setStatusMsg("Introduce un ID de sala primero");
      return;
    }

    try {
      setStatus("joining");
      setStatusMsg("Accediendo al dispositivo...");

      let stream: MediaStream;
      if (mode === "camera") {
        stream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
      } else {
        stream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true });
      }

      streamRef.current = stream;
      if (localVideoRef.current) localVideoRef.current.srcObject = stream;

      const ws = new WebSocket(getWsUrl());
      wsRef.current = ws;

      ws.onopen = () => {
        createPc(stream);
        ws.send(
          JSON.stringify({
            type: "join",
            roomId: roomId.trim(),
            peerId: myId.current,
          })
        );
        setJoined(true);
        setStatus("joining");
        setStatusMsg("Conectando a la sala...");
      };

      ws.onmessage = (e) => handleMsg(JSON.parse(e.data) as Record<string, unknown>);

      ws.onerror = () => {
        setStatus("error");
        setStatusMsg("Error de conexión. ¿Está el servidor corriendo?");
      };

      ws.onclose = () => {
        if (joined) {
          setJoined(false);
          setStatus("idle");
          setStatusMsg("Desconectado del servidor");
        }
      };
    } catch (err) {
      const e = err as Error;
      setStatus("error");
      setStatusMsg(
        e.name === "NotAllowedError"
          ? "Permiso denegado al dispositivo"
          : `Error: ${e.message}`
      );
    }
  };

  const leave = useCallback(() => {
    wsRef.current?.send(JSON.stringify({ type: "leave" }));
    wsRef.current?.close();
    pcRef.current?.close();
    streamRef.current?.getTracks().forEach((t) => t.stop());
    if (localVideoRef.current) localVideoRef.current.srcObject = null;
    if (remoteVideoRef.current) remoteVideoRef.current.srcObject = null;
    setJoined(false);
    setHasRemote(false);
    setStatus("idle");
    setStatusMsg("Introduce una sala y selecciona el modo");
    remoteId.current = null;
    pcRef.current = null;
    wsRef.current = null;
    streamRef.current = null;
  }, []);

  useEffect(() => () => { leave(); }, [leave]);

  const statusColor: Record<Status, string> = {
    idle: "#6b7280",
    joining: "#d97706",
    waiting: "#2563eb",
    negotiating: "#7c3aed",
    connected: "#16a34a",
    error: "#dc2626",
  };

  const dot = (
    <span
      style={{
        display: "inline-block",
        width: 10,
        height: 10,
        borderRadius: "50%",
        background: statusColor[status],
        marginRight: 8,
        verticalAlign: "middle",
        boxShadow: status === "connected" ? "0 0 6px #16a34a" : "none",
      }}
    />
  );

  return (
    <div
      style={{
        minHeight: "100vh",
        background: "#0f172a",
        color: "#e2e8f0",
        fontFamily: "'Inter', system-ui, sans-serif",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        padding: "32px 16px",
        gap: 24,
      }}
    >
      <div style={{ textAlign: "center" }}>
        <h1 style={{ fontSize: "2rem", fontWeight: 700, color: "#f1f5f9", margin: 0 }}>
          WebRTC P2P
        </h1>
        <p style={{ color: "#94a3b8", marginTop: 6, fontSize: "0.95rem" }}>
          Videollamada y pantalla compartida entre navegadores
        </p>
      </div>

      <div
        style={{
          background: "#1e293b",
          borderRadius: 12,
          padding: "20px 24px",
          width: "100%",
          maxWidth: 500,
          display: "flex",
          flexDirection: "column",
          gap: 14,
          boxShadow: "0 4px 24px rgba(0,0,0,0.3)",
        }}
      >
        <div style={{ fontSize: "0.85rem", color: "#94a3b8" }}>
          {dot}
          {statusMsg}
        </div>

        {!joined ? (
          <>
            <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
              <input
                value={roomId}
                onChange={(e) => setRoomId(e.target.value)}
                placeholder="ID de sala (ej: abc123)"
                style={{
                  flex: 1,
                  background: "#0f172a",
                  border: "1px solid #334155",
                  borderRadius: 8,
                  padding: "10px 14px",
                  color: "#f1f5f9",
                  fontSize: "0.95rem",
                  outline: "none",
                }}
                onKeyDown={(e) => e.key === "Enter" && start("camera")}
              />
              <button
                onClick={() => setRoomId(generateId())}
                style={{
                  background: "#334155",
                  border: "none",
                  borderRadius: 8,
                  padding: "10px 12px",
                  color: "#94a3b8",
                  cursor: "pointer",
                  fontSize: "0.8rem",
                  whiteSpace: "nowrap",
                }}
              >
                Generar
              </button>
            </div>

            <div style={{ display: "flex", gap: 10 }}>
              <button
                onClick={() => start("camera")}
                style={{
                  flex: 1,
                  background: "#2563eb",
                  border: "none",
                  borderRadius: 8,
                  padding: "12px",
                  color: "#fff",
                  cursor: "pointer",
                  fontWeight: 600,
                  fontSize: "0.9rem",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  gap: 8,
                }}
              >
                <CameraIcon /> Camara
              </button>
              <button
                onClick={() => start("screen")}
                style={{
                  flex: 1,
                  background: "#7c3aed",
                  border: "none",
                  borderRadius: 8,
                  padding: "12px",
                  color: "#fff",
                  cursor: "pointer",
                  fontWeight: 600,
                  fontSize: "0.9rem",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  gap: 8,
                }}
              >
                <ScreenIcon /> Pantalla
              </button>
            </div>
          </>
        ) : (
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <div style={{ flex: 1, background: "#0f172a", borderRadius: 8, padding: "10px 14px", color: "#64748b", fontSize: "0.85rem" }}>
              Sala: <span style={{ color: "#f1f5f9", fontWeight: 600 }}>{roomId}</span>
            </div>
            <button
              onClick={leave}
              style={{
                background: "#dc2626",
                border: "none",
                borderRadius: 8,
                padding: "10px 18px",
                color: "#fff",
                cursor: "pointer",
                fontWeight: 600,
                fontSize: "0.9rem",
              }}
            >
              Salir
            </button>
          </div>
        )}
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 16,
          width: "100%",
          maxWidth: 900,
        }}
      >
        <VideoBox label="Tu video" videoRef={localVideoRef} muted />
        <VideoBox label={hasRemote ? "Participante remoto" : "Esperando participante..."} videoRef={remoteVideoRef} dim={!hasRemote} />
      </div>

      <div
        style={{
          background: "#1e293b",
          borderRadius: 10,
          padding: "14px 20px",
          maxWidth: 500,
          width: "100%",
          fontSize: "0.8rem",
          color: "#64748b",
          lineHeight: 1.6,
        }}
      >
        <strong style={{ color: "#94a3b8" }}>Como conectar dos dispositivos:</strong>
        <ol style={{ paddingLeft: 18, marginTop: 6 }}>
          <li>El primer dispositivo genera o escribe un ID de sala y pulsa Camara o Pantalla</li>
          <li>El segundo dispositivo escribe el mismo ID y pulsa cualquier boton</li>
          <li>La conexion P2P se establece automaticamente</li>
        </ol>
      </div>
    </div>
  );
}

function VideoBox({
  label,
  videoRef,
  muted,
  dim,
}: {
  label: string;
  videoRef: React.RefObject<HTMLVideoElement | null>;
  muted?: boolean;
  dim?: boolean;
}) {
  return (
    <div
      style={{
        background: "#1e293b",
        borderRadius: 12,
        overflow: "hidden",
        position: "relative",
        aspectRatio: "16/9",
        boxShadow: "0 4px 24px rgba(0,0,0,0.3)",
      }}
    >
      <video
        ref={videoRef}
        autoPlay
        playsInline
        muted={muted}
        style={{
          width: "100%",
          height: "100%",
          objectFit: "cover",
          opacity: dim ? 0.2 : 1,
          transition: "opacity 0.3s",
        }}
      />
      <div
        style={{
          position: "absolute",
          bottom: 10,
          left: 12,
          background: "rgba(0,0,0,0.6)",
          borderRadius: 6,
          padding: "4px 10px",
          fontSize: "0.78rem",
          color: "#cbd5e1",
          backdropFilter: "blur(4px)",
        }}
      >
        {label}
      </div>
    </div>
  );
}

function CameraIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
      <path d="M23 7l-7 5 7 5V7z" />
      <rect x="1" y="5" width="15" height="14" rx="2" ry="2" />
    </svg>
  );
}

function ScreenIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
      <rect x="2" y="3" width="20" height="14" rx="2" ry="2" />
      <line x1="8" y1="21" x2="16" y2="21" />
      <line x1="12" y1="17" x2="12" y2="21" />
    </svg>
  );
}
