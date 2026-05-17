
# server.py
import asyncio
import websockets
import socket
import json
import pygame
import struct

# ===== 設定 =====
UDP_IP = "192.168.11.20"   # 上位マイコンIP
UDP_PORT = 5005            # MLRCSのreceivePort

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# ===== PS5初期化 =====
pygame.init()
pygame.joystick.init()

if pygame.joystick.get_count() == 0:
    print("PS5コントローラーが接続されていません")
    exit()

js = pygame.joystick.Joystick(0)
js.init()

print("PS5コントローラー接続OK")

# ===== スマホ用（6ボタン）=====
smartphone_buttons = {
    "custom1": 0,
    "custom2": 0,
    "custom3": 0,
    "custom4": 0,
    "custom5": 0,
    "custom6": 0
}

# ===== WebSocket受信 =====
async def handler(websocket):
    global smartphone_buttons
    print("スマホ接続OK")

    async for message in websocket:
        try:
            data = json.loads(message)

            btns = data.get("buttons", {})
            for k in smartphone_buttons.keys():
                smartphone_buttons[k] = btns.get(k, 0)

            await websocket.send(json.dumps({"status": "ok"}))

        except Exception as e:
            print("WebSocketエラー:", e)

# ===== UDP送信ループ =====
async def send_loop():
    while True:
        pygame.event.pump()

        button_bits = 0

        # ===== フェイスボタン =====
        if js.get_button(0): button_bits |= (1 << 0)   # CROSS
        if js.get_button(1): button_bits |= (1 << 1)   # CIRCLE
        if js.get_button(2): button_bits |= (1 << 2)   # SQUARE
        if js.get_button(3): button_bits |= (1 << 3)   # TRIANGLE

        # ===== 方向キー（D-pad）=====
        hat = js.get_hat(0)
        if hat[1] == 1:   button_bits |= (1 << 4)   # UP
        if hat[1] == -1:  button_bits |= (1 << 5)   # DOWN
        if hat[0] == -1:  button_bits |= (1 << 6)   # LEFT
        if hat[0] == 1:   button_bits |= (1 << 7)   # RIGHT

        # ===== ショルダー =====
        if js.get_button(4): button_bits |= (1 << 8)   # L1
        if js.get_button(5): button_bits |= (1 << 9)   # R1
        if js.get_button(6): button_bits |= (1 << 10)  # L2
        if js.get_button(7): button_bits |= (1 << 11)  # R2

        # ===== スティック押し込み =====
        if js.get_button(10): button_bits |= (1 << 12) # L3
        if js.get_button(11): button_bits |= (1 << 13) # R3

        # ===== システム =====
        if js.get_button(8):  button_bits |= (1 << 14) # SHARE
        if js.get_button(9):  button_bits |= (1 << 15) # OPTIONS
        if js.get_button(12): button_bits |= (1 << 16) # PS
        if js.get_button(13): button_bits |= (1 << 17) # タッチパッド

        # ===== スマホ（後ろ）=====
        if smartphone_buttons["custom1"]: button_bits |= (1 << 18)
        if smartphone_buttons["custom2"]: button_bits |= (1 << 19)
        if smartphone_buttons["custom3"]: button_bits |= (1 << 20)
        if smartphone_buttons["custom4"]: button_bits |= (1 << 21)
        if smartphone_buttons["custom5"]: button_bits |= (1 << 22)
        if smartphone_buttons["custom6"]: button_bits |= (1 << 23)

        # ===== スティック =====
        axes = [
            int(js.get_axis(1) * 10000),  # 左スティック上下
            int(js.get_axis(0) * 10000),  # 左スティック左右
            int(js.get_axis(2) * 10000),  # 右スティック左右
            int(js.get_axis(3) * 10000),  # 右スティック上下
        ]

        # ===== パケット生成 =====
        packet = struct.pack(
            "<B I hhhh I",
            1,              # GAMEPAD_DATA
            0,              # timestamp
            axes[0],
            axes[1],
            axes[2],
            axes[3],
            button_bits
        )

        # ===== UDP送信 =====
        sock.sendto(packet, (UDP_IP, UDP_PORT))

        await asyncio.sleep(0.02)  # 50Hz

# ===== メイン =====
async def main():
    print("サーバー起動中...")
    print("WebSocket: ws://0.0.0.0:8765")

    ws_server = await websockets.serve(handler, "0.0.0.0", 8765)

    await asyncio.gather(
        send_loop(),
        ws_server.wait_closed()
    )

# ===== 実行 =====
asyncio.run(main())

