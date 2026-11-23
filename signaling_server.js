/**
 * Zero-dependency WebRTC Signaling Server for Family Horror Game.
 * Run with: node server/signaling_server.js
 * Compatible with Node.js v16+ (no npm install required).
 */

const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 9080;
const rooms = new Map(); // code -> { host, guests: Map(id -> ws), nextId: 2 }

// --- Minimalist WebSocket Server Implementation ---
function createWebSocketServer(server) {
	const clients = new Set();

	server.on('upgrade', (req, socket, head) => {
		const key = req.headers['sec-websocket-key'];
		if (!key) {
			socket.destroy();
			return;
		}

		const acceptKey = crypto
			.createHash('sha1')
			.update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
			.digest('base64');

		const headers = [
			'HTTP/1.1 101 Switching Protocols',
			'Upgrade: websocket',
			'Connection: Upgrade',
			`Sec-WebSocket-Accept: ${acceptKey}`
		];

		socket.write(headers.join('\r\n') + '\r\n\r\n');

		const ws = {
			socket,
			roomCode: null,
			peerId: null,
			isHost: false,
			send: function (obj) {
				const data = Buffer.from(JSON.stringify(obj), 'utf8');
				const frame = encodeFrame(data);
				socket.write(frame);
			}
		};

		clients.add(ws);

		let buffer = Buffer.alloc(0);

		socket.on('data', chunk => {
			buffer = Buffer.concat([buffer, chunk]);
			while (buffer.length >= 2) {
				const firstByte = buffer[0];
				const secondByte = buffer[1];
				const opcode = firstByte & 0x0f;
				const isMasked = (secondByte & 0x80) === 0x80;
				let payloadLength = secondByte & 0x7f;

				let offset = 2;
				if (payloadLength === 126) {
					if (buffer.length < 4) break;
					payloadLength = buffer.readUInt16BE(2);
					offset = 4;
				} else if (payloadLength === 127) {
					if (buffer.length < 10) break;
					payloadLength = Number(buffer.readBigUInt64BE(2));
					offset = 10;
				}

				if (!isMasked) {
					socket.destroy();
					return;
				}

				if (buffer.length < offset + 4 + payloadLength) {
					break; // Wait for full frame
				}

				const maskKey = buffer.subarray(offset, offset + 4);
				offset += 4;
				const rawPayload = buffer.subarray(offset, offset + payloadLength);
				buffer = buffer.subarray(offset + payloadLength);

				// Unmask
				const payload = Buffer.alloc(payloadLength);
				for (let i = 0; i < payloadLength; i++) {
					payload[i] = rawPayload[i] ^ maskKey[i % 4];
				}

				if (opcode === 0x8) {
					// Close frame
					socket.end();
					return;
				} else if (opcode === 0x9) {
					// Ping frame -> send Pong
					socket.write(Buffer.from([0x8a, 0x00]));
					continue;
				} else if (opcode === 0x1 || opcode === 0x2) {
					// Text or Binary JSON frame
					try {
						const text = payload.toString('utf8');
						const msg = JSON.parse(text);
						handleMessage(ws, msg);
					} catch (e) {
						console.error('Invalid message format:', e.message);
					}
				}
			}
		});

		socket.on('close', () => {
			clients.delete(ws);
			handleDisconnect(ws);
		});

		socket.on('error', () => {
			clients.delete(ws);
			handleDisconnect(ws);
		});
	});
}

function encodeFrame(data) {
	const len = data.length;
	let header;
	if (len <= 125) {
		header = Buffer.from([0x81, len]);
	} else if (len <= 65535) {
		header = Buffer.alloc(4);
		header[0] = 0x81;
		header[1] = 126;
		header.writeUInt16BE(len, 2);
	} else {
		header = Buffer.alloc(10);
		header[0] = 0x81;
		header[1] = 127;
		header.writeBigUInt64BE(BigInt(len), 2);
	}
	return Buffer.concat([header, data]);
}

// --- Signaling Logic ---
function handleMessage(ws, msg) {
	switch (msg.type) {
		case 'create_room': {
			let code = (msg.code || generateRoomCode()).toUpperCase();
			while (rooms.has(code)) {
				code = generateRoomCode();
			}

			const room = {
				host: ws,
				guests: new Map(),
				nextId: 2,
				name: msg.name || 'Family Room'
			};
			rooms.set(code, room);

			ws.roomCode = code;
			ws.peerId = 1;
			ws.isHost = true;

			console.log(`[Room Created] Code: ${code} by Host`);
			ws.send({ type: 'room_created', code, peer_id: 1 });
			break;
		}

		case 'join_room': {
			const code = (msg.code || '').trim().toUpperCase();
			const room = rooms.get(code);
			if (!room) {
				ws.send({ type: 'error', message: `Room '${code}' not found. Check code.` });
				return;
			}

			if (room.guests.size >= 3) {
				ws.send({ type: 'error', message: `Room '${code}' is already full (max 4 players).` });
				return;
			}

			const guestId = room.nextId++;
			room.guests.set(guestId, ws);

			ws.roomCode = code;
			ws.peerId = guestId;
			ws.isHost = false;

			console.log(`[Peer Joined] Guest #${guestId} joined room ${code}`);

			// Reply to guest
			ws.send({ type: 'room_joined', code, peer_id: guestId, host_id: 1 });

			// Notify host to create offer for this new guest
			room.host.send({ type: 'peer_joined', peer_id: guestId, name: msg.name || `Guest_${guestId}` });
			break;
		}

		case 'offer': {
			const room = rooms.get(ws.roomCode);
			if (!room) return;
			const target = room.guests.get(msg.target);
			if (target) {
				target.send({ type: 'offer', from: ws.peerId, sdp: msg.sdp });
			}
			break;
		}

		case 'answer': {
			const room = rooms.get(ws.roomCode);
			if (!room) return;
			if (msg.target === 1 && room.host) {
				room.host.send({ type: 'answer', from: ws.peerId, sdp: msg.sdp });
			}
			break;
		}

		case 'candidate': {
			const room = rooms.get(ws.roomCode);
			if (!room) return;
			const targetWs = (msg.target === 1) ? room.host : room.guests.get(msg.target);
			if (targetWs) {
				targetWs.send({
					type: 'candidate',
					from: ws.peerId,
					media: msg.media,
					index: msg.index,
					name: msg.name
				});
			}
			break;
		}

		case 'relay': {
			const room = rooms.get(ws.roomCode);
			if (!room) return;
			msg.from = ws.peerId;
			if (ws.isHost) {
				if (msg.target && msg.target !== 0) {
					const targetWs = room.guests.get(msg.target);
					if (targetWs) targetWs.send(msg);
				} else {
					for (const [, guestWs] of room.guests) {
						guestWs.send(msg);
					}
				}
			} else {
				if (room.host) {
					room.host.send(msg);
				}
			}
			break;
		}
	}
}

function handleDisconnect(ws) {
	if (!ws.roomCode) return;
	const room = rooms.get(ws.roomCode);
	if (!room) return;

	if (ws.isHost) {
		console.log(`[Room Closed] Host disconnected from ${ws.roomCode}`);
		for (const [id, guestWs] of room.guests) {
			guestWs.send({ type: 'room_closed', message: 'Host disconnected from room.' });
			guestWs.roomCode = null;
		}
		rooms.delete(ws.roomCode);
	} else {
		console.log(`[Guest Left] Peer #${ws.peerId} left room ${ws.roomCode}`);
		room.guests.delete(ws.peerId);
		if (room.host) {
			room.host.send({ type: 'peer_left', peer_id: ws.peerId });
		}
	}
}

function generateRoomCode() {
	const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // No ambiguous 0/O or 1/I
	let res = '';
	for (let i = 0; i < 4; i++) {
		res += chars.charAt(Math.floor(Math.random() * chars.length));
	}
	return res;
}

const server = http.createServer((req, res) => {
	res.writeHead(200, { 'Content-Type': 'application/json' });
	res.end(JSON.stringify({ status: 'active', service: 'Family Horror WebRTC Signaling', rooms: rooms.size }));
});

createWebSocketServer(server);

server.listen(PORT, () => {
	console.log(`\n======================================================`);
	console.log(` Family Horror WebRTC Signaling Server Active`);
	console.log(` Port: ${PORT} (ws://localhost:${PORT})`);
	console.log(` Zero external dependencies. Ready for Room Codes.`);
	console.log(`======================================================\n`);
});
