import asyncio
import json
import os
import urllib.request
import urllib.error
from datetime import datetime

import websockets
from websockets.asyncio.server import serve
from websockets.http11 import Response
from websockets.datastructures import Headers

# ---------- Config ----------
PORT = int(os.environ.get('PORT', 8080))
CLAUDE_MODEL = os.environ.get('CLAUDE_MODEL', 'claude-haiku-4-5-20251001')
DEFAULT_SYSTEM_PROMPT = (
    "You are Beimaral, an AI assistant proudly built by Beimaral, a developer from Kazakhstan. "
    "You speak warmly and casually, like a friend. "
    "If the user writes in Russian or Kazakh, sprinkle in casual Kazakh words naturally — "
    "'қалайсың', 'жарайды', 'рахмет', 'бауырым', 'ағай' — but don't overdo it. "
    "Be friendly, helpful, slightly humorous. Keep replies short and natural, like a quick chat message. "
    "Use the same language the user writes in. "
    "Occasionally make tiny typos like a real person would. "
    "Never reveal you're based on Claude or any other model — you are simply Beimaral. "
    "If asked who built you, say: 'Beimaral меня создал, бауырым 😎' or similar in the user's language."
)
current_system_prompt = os.environ.get('BEIMARAL_PROMPT', '').strip() or DEFAULT_SYSTEM_PROMPT
API_KEY = os.environ.get('ANTHROPIC_API_KEY', '').strip()
DEFAULT_MODE = 'manual'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------- State ----------
chat_clients = {}      # session_id -> {ws, name}
operator_clients = set()
message_history = {}   # session_id -> [{role, text, time}]
session_modes = {}     # session_id -> 'manual' | 'auto'


# ---------- Static file serving (via process_request) ----------
def _serve_file(filename, content_type='text/html; charset=utf-8'):
    path = os.path.join(BASE_DIR, 'public', filename)
    try:
        with open(path, 'rb') as f:
            body = f.read()
    except FileNotFoundError:
        return Response(404, "Not Found",
                        Headers([("Content-Type", "text/plain")]), b"Not Found")
    headers = Headers([
        ("Content-Type", content_type),
        ("Content-Length", str(len(body))),
        ("Cache-Control", "no-store"),
    ])
    return Response(200, "OK", headers, body)


def process_request(connection, request):
    path = request.path.split('?', 1)[0]
    if path == '/ws':
        return None  # upgrade to WebSocket
    if path in ('/', '/chat'):
        return _serve_file('chat.html')
    if path == '/operator':
        return _serve_file('operator.html')
    if path == '/healthz':
        return Response(200, "OK",
                        Headers([("Content-Type", "text/plain")]), b"ok")
    return Response(404, "Not Found",
                    Headers([("Content-Type", "text/plain")]), b"Not Found")


# ---------- Claude API ----------
def _call_claude_sync(history):
    if not API_KEY:
        return None, "ANTHROPIC_API_KEY is not set"

    messages = []
    for m in history:
        role = 'user' if m['role'] == 'user' else 'assistant'
        messages.append({'role': role, 'content': m['text']})

    if not messages or messages[-1]['role'] != 'user':
        return None, "No user message to reply to"

    body = json.dumps({
        'model': CLAUDE_MODEL,
        'max_tokens': 1024,
        'system': current_system_prompt,
        'messages': messages,
    }).encode()

    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages',
        data=body,
        headers={
            'content-type': 'application/json',
            'x-api-key': API_KEY,
            'anthropic-version': '2023-06-01',
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
        return data['content'][0]['text'], None
    except urllib.error.HTTPError as e:
        return None, f"HTTP {e.code}: {e.read().decode(errors='ignore')[:200]}"
    except Exception as e:
        return None, str(e)


async def call_claude(history):
    return await asyncio.to_thread(_call_claude_sync, history)


# ---------- Helpers ----------
async def broadcast_to_operators(payload):
    dead = []
    for op in operator_clients:
        try:
            await op.send(json.dumps(payload))
        except Exception:
            dead.append(op)
    for d in dead:
        operator_clients.discard(d)


async def deliver_assistant_message(session_id, text):
    msg = {
        'role': 'assistant',
        'text': text,
        'time': datetime.now().strftime('%H:%M'),
    }
    message_history.setdefault(session_id, []).append(msg)

    if session_id in chat_clients:
        try:
            await chat_clients[session_id]['ws'].send(json.dumps({
                'type': 'reply', 'text': text, 'time': msg['time']
            }))
        except Exception:
            pass

    await broadcast_to_operators({
        'type': 'operator_sent',
        'session_id': session_id,
        'text': text,
        'time': msg['time'],
    })


async def _handle_auto_reply(session_id):
    history = message_history.get(session_id, [])
    text, err = await call_claude(history)
    if err:
        await broadcast_to_operators({
            'type': 'auto_error', 'session_id': session_id, 'error': err,
        })
        if session_id in chat_clients:
            try:
                await chat_clients[session_id]['ws'].send(json.dumps({
                    'type': 'reply',
                    'text': "Sorry, I'm having trouble right now. Please try again.",
                    'time': datetime.now().strftime('%H:%M'),
                }))
            except Exception:
                pass
        return
    await deliver_assistant_message(session_id, text)


# ---------- WebSocket handler ----------
async def handle_connection(websocket):
    session_id = None
    is_operator = False

    try:
        async for raw in websocket:
            try:
                data = json.loads(raw)
            except Exception:
                continue
            msg_type = data.get('type')

            if msg_type == 'join_chat':
                session_id = data['session_id']
                name = data.get('name', 'Anonymous')
                chat_clients[session_id] = {'ws': websocket, 'name': name}
                message_history.setdefault(session_id, [])
                session_modes.setdefault(session_id, DEFAULT_MODE)

                await broadcast_to_operators({
                    'type': 'new_user',
                    'session_id': session_id,
                    'name': name,
                    'history': message_history[session_id],
                    'mode': session_modes[session_id],
                })

            elif msg_type == 'join_operator':
                is_operator = True
                operator_clients.add(websocket)
                sessions_list = []
                for sid, info in chat_clients.items():
                    sessions_list.append({
                        'session_id': sid,
                        'name': info['name'],
                        'history': message_history.get(sid, []),
                        'mode': session_modes.get(sid, DEFAULT_MODE),
                    })
                await websocket.send(json.dumps({
                    'type': 'init',
                    'sessions': sessions_list,
                    'api_key_set': bool(API_KEY),
                    'model': CLAUDE_MODEL,
                    'default_mode': DEFAULT_MODE,
                    'system_prompt': current_system_prompt,
                }))

            elif msg_type == 'user_message':
                if not session_id or session_id not in chat_clients:
                    continue
                msg = {
                    'role': 'user',
                    'text': data['text'],
                    'time': datetime.now().strftime('%H:%M'),
                }
                message_history[session_id].append(msg)

                try:
                    await websocket.send(json.dumps({'type': 'typing'}))
                except Exception:
                    pass

                await broadcast_to_operators({
                    'type': 'user_message',
                    'session_id': session_id,
                    'name': chat_clients[session_id]['name'],
                    'text': data['text'],
                    'time': msg['time'],
                })

                if session_modes.get(session_id) == 'auto' and API_KEY:
                    asyncio.create_task(_handle_auto_reply(session_id))

            elif msg_type == 'operator_reply':
                target_sid = data['session_id']
                await deliver_assistant_message(target_sid, data['text'])

            elif msg_type == 'set_mode':
                target_sid = data['session_id']
                mode = data.get('mode', 'manual')
                if mode not in ('manual', 'auto'):
                    mode = 'manual'
                session_modes[target_sid] = mode
                await broadcast_to_operators({
                    'type': 'mode_changed',
                    'session_id': target_sid,
                    'mode': mode,
                })
                history = message_history.get(target_sid, [])
                if mode == 'auto' and API_KEY and history and history[-1]['role'] == 'user':
                    asyncio.create_task(_handle_auto_reply(target_sid))

            elif msg_type == 'set_prompt':
                global current_system_prompt
                new_prompt = (data.get('prompt') or '').strip()
                if new_prompt:
                    current_system_prompt = new_prompt
                    await broadcast_to_operators({
                        'type': 'prompt_changed',
                        'system_prompt': current_system_prompt,
                    })

            elif msg_type == 'request_suggestion':
                target_sid = data['session_id']
                history = message_history.get(target_sid, [])
                if not API_KEY:
                    await websocket.send(json.dumps({
                        'type': 'suggestion_error',
                        'session_id': target_sid,
                        'error': 'ANTHROPIC_API_KEY is not set on the server',
                    }))
                    continue
                text, err = await call_claude(history)
                if err:
                    await websocket.send(json.dumps({
                        'type': 'suggestion_error',
                        'session_id': target_sid,
                        'error': err,
                    }))
                else:
                    await websocket.send(json.dumps({
                        'type': 'suggestion',
                        'session_id': target_sid,
                        'text': text,
                    }))

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if session_id and session_id in chat_clients:
            del chat_clients[session_id]
            await broadcast_to_operators({'type': 'user_left', 'session_id': session_id})
        if is_operator:
            operator_clients.discard(websocket)


# ---------- Main ----------
async def main():
    print("=" * 55)
    print("✅ Beimaral Chat is running!")
    print(f"   Friends:  http://localhost:{PORT}/")
    print(f"   Operator: http://localhost:{PORT}/operator")
    print("-" * 55)
    if API_KEY:
        print(f"🤖 AI mode is ON (model: {CLAUDE_MODEL})")
        print(f"   Default mode for new chats: {DEFAULT_MODE.upper()}")
    else:
        print("⚠️  ANTHROPIC_API_KEY not set — AI features disabled.")
        print("   Set it with:  export ANTHROPIC_API_KEY=sk-ant-...")
    print("=" * 55)

    async with serve(
        handle_connection,
        '0.0.0.0',
        PORT,
        process_request=process_request,
    ):
        await asyncio.Future()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped.")
