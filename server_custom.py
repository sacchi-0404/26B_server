#!/usr/bin/env python3
"""
シンプルゲームパッドサーバー
- iPhoneのUIからWebSocketでゲームパッドデータとPingを受信
- ゲームパッドデータとカスタムボタンデータをmbedマイクロコントローラーへUDPで転送
- UIからのPing要求にはPongを応答
- マイコンとの間でUDP Pingを周期的に計測し、その往復時間(RTT)をUIに通知
"""

# --- ライブラリのインポート ---
import asyncio      # 非同期I/O（ネットワーク通信など）を扱うためのライブラリ
import websockets   # WebSocketサーバーを簡単に構築するためのライブラリ
import json         # JSON形式のデータを扱うためのライブラリ
import socket       # IPアドレス取得など低レベルなネットワーク操作のためのライブラリ
import struct       # Pythonのデータ型をC言語の構造体（バイナリデータ）に変換するためのライブラリ
import logging      # ログ出力を行うためのライブラリ
import time         # タイムスタンプ取得など時間関連の操作のためのライブラリ
from functools import partial # 関数の一部引数を固定した新しい関数を作成するためのユーティリティ

# --- 全体設定 ---
SERVER_HOST = "0.0.0.0"       # サーバーが待ち受けるIPアドレス。0.0.0.0は全てのネットワークインターフェースを意味する
WEBSOCKET_PORT = 9001         # WebSocketサーバーが待ち受けるポート番号
NUCLEO_IP = "192.168.11.20"   # データ送信先であるマイコンのIPアドレス
NUCLEO_PORT_TX = 8080         # マイコンへのデータ送信ポート番号
NUCLEO_PORT_RX = 4000         # マイコンからのデータ受信ポート番号

# マイコンと通信する際のパケット種別を定義
PACKET_TYPE = {
    "GAMEPAD_DATA": 1,
    "PING": 2,
    "PONG": 3,
    "ODOMETRY_DATA": 4
}

# --- ロガーの設定 ---
# ログの出力レベルやフォーマットを設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- グローバル変数 ---
# マイコンとのPing往復時間(RTT)を格納するための辞書
ping_stats = {"last_ping_time": 0, "rtt_ms": None}
# 接続中の全WebSocketクライアント（UI）を管理するためのセット
connected_clients = set()


def get_local_ip():
    """
    このサーバーが動作しているマシンのローカルIPアドレスを取得する関数。
    UI側でどのIPに接続すればよいかを表示するために使用する。
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # 外部のIPアドレスに接続を試みることで、実際使用されるネットワークインターフェースのIPを取得
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1' # 取得に失敗した場合はローカルホストを返す
    finally:
        s.close()
    return IP

async def broadcast_ping(rtt):
    """
    マイコンとのPing往復時間(RTT)を、接続されている全てのUIクライアントに送信する非同期関数。
    Args:
        rtt (float or None): 計測した往復時間(ミリ秒)。タイムアウトした場合はNone。
    """
    if connected_clients:
        message = json.dumps({'type': 'mcu_ping', 'rtt': rtt})
        # asyncio.gatherを使い、全てのクライアントへの送信処理を並行して効率的に行う
        await asyncio.gather(*(client.send(message) for client in connected_clients))

async def broadcast_odom(message):
    """
    オドメトリデータをUIへ送信
    """
    if connected_clients:
        await asyncio.gather(
            *(client.send(message) for client in connected_clients)
        )

def send_to_nucleo(gamepad_data, transport):
    """
    UIから受信したゲームパッドデータを解析し、マイコン向けのUDPパケットを送信する関数。
    Args:
        gamepad_data (dict): UIから送られてきたJSONデータ。
        transport (asyncio.DatagramTransport): UDP送信用トランスポートオブジェクト。
    """
    try:
        # JSONデータから各値を取得。存在しない場合のデフォルト値も設定。
        axes = gamepad_data.get('axes', [0.0] * 4)
        buttons = gamepad_data.get('buttons', [])
        timestamp = int(time.time() * 1000)
        axis_data = [int(axis * 1000) for axis in axes[:4]]
        while len(axis_data) < 4: axis_data.append(0) # 軸データが4つ未満の場合に0で埋める
        
        # 通常のコントローラーボタンの状態をビットマスクに変換
        button_mask = sum(1 << i for i, btn in enumerate(buttons[:17]) if btn.get('pressed'))
        # UIのカスタムボタンの状態をビットマスクに変換（17ビット目から割り当て）
        custom_buttons = gamepad_data.get('customButtons', [False] * 6)
        custom_button_mask = sum(1 << (17 + i) for i, pressed in enumerate(custom_buttons) if pressed)
        
        # 2つのビットマスクを合成
        final_button_mask = button_mask | custom_button_mask

        # struct.packを使い、データをリトルエンディアンのバイナリ形式に変換
        # '<' : リトルエンディアン
        # 'B' : 符号なしchar (1バイト) - パケット種別
        # 'I' : 符号なしint (4バイト) - タイムスタンプ
        # '4h': 符号ありshort (2バイト)が4つ - 軸データ
        # 'I' : 符号なしint (4バイト) - ボタンマスク
        packet_data = struct.pack(
            '<BI4hI',
            PACKET_TYPE["GAMEPAD_DATA"],
            timestamp & 0xFFFFFFFF,
            *axis_data,
            final_button_mask
        )
        # UDPパケットをマイコンに送信
        transport.sendto(packet_data, (NUCLEO_IP, NUCLEO_PORT_TX))
    except Exception as e:
        logger.error(f"マイコンへのデータ送信中にエラーが発生しました: {e}")

async def websocket_handler(websocket, transport):
    """
    WebSocketクライアントからの接続を処理するメインの非同期ハンドラ。
    クライアントが接続するたびに、この関数が実行される。
    Args:
        websocket: 接続されたクライアントとの通信用オブジェクト。
        transport (asyncio.DatagramTransport): UDP送信用トランスポートオブジェクト。
    """
    client_ip = websocket.remote_address[0]
    logger.info(f"クライアントが接続しました: {client_ip}")
    connected_clients.add(websocket) # 新しいクライアントを管理セットに追加
    try:
        # クライアントからメッセージを非同期で待ち受けるループ
        async for message in websocket:
            try:
                data = json.loads(message)
                # 受信メッセージがPing要求か、それ以外（ゲームパッドデータ）かを判定
                if data.get('type') == 'ping' and 'timestamp' in data:
                    # Ping要求なら、タイムスタンプを含んだPongメッセージを返信
                    pong_message = json.dumps({'type': 'pong', 'timestamp': data['timestamp']})
                    await websocket.send(pong_message)
                else:
                    # ゲームパッドデータなら、マイコンに転送
                    send_to_nucleo(data, transport)
            except json.JSONDecodeError:
                logger.warning(f"JSONではない不正なメッセージを受信しました: {client_ip}")
            except Exception as e:
                logger.error(f"メッセージ処理中にエラーが発生しました ({client_ip}): {e}")
    except websockets.exceptions.ConnectionClosed as e:
        logger.info(f"クライアントが切断しました: {client_ip} (理由: {e.reason}, コード: {e.code})")
    finally:
        logger.info(f"クライアント接続が終了しました: {client_ip}")
        connected_clients.remove(websocket) # 切断されたクライアントを管理セットから削除

async def run_ping_cycle(transport):
    """
    マイコンに対して定期的にUDP Pingを送信し、RTTを計測するバックグラウンドタスク。
    Args:
        transport (asyncio.DatagramTransport): UDP送信用トランスポートオブジェクト。
    """
    while True:
        try:
            current_time = time.time()
            ping_stats["last_ping_time"] = current_time
            # パケット種別と送信時刻をバイナリデータとしてパック
            packet = struct.pack('<Bd', PACKET_TYPE["PING"], current_time)
            transport.sendto(packet, (NUCLEO_IP, NUCLEO_PORT_TX))
            
            await asyncio.sleep(0.5) # 0.5秒待機
            
            rtt = ping_stats["rtt_ms"]
            if rtt is not None:
                # Pong応答があればRTTをログに出力し、UIにブロードキャスト
                logger.info(f"{NUCLEO_IP} へのPing RTT: {rtt:.2f} ms")
                await broadcast_ping(rtt)
                ping_stats["rtt_ms"] = None # RTTをリセット
            else:
                # Pong応答がなければタイムアウトとして処理
                logger.warning(f"{NUCLEO_IP} へのPing: 応答なし (タイムアウト)")
                await broadcast_ping(None) # UIにもタイムアウトを通知

        except Exception as e:
            logger.error(f"Pingサイクルでエラーが発生しました: {e}")
            await asyncio.sleep(1) # エラー発生時は少し長く待機

class UdpProtocol(asyncio.DatagramProtocol):
    """
    asyncioのためのUDPプロトコル実装クラス。
    UDPソケットでのイベント（データ受信など）を処理する。
    """
    def connection_made(self, transport):
        """UDPソケットの準備が完了したときに呼ばれる。"""
        self.transport = transport

    def datagram_received(self, data, addr):
        """UDPデータグラムを受信したときに呼ばれる。"""

    # ---------- PONG ----------
        if addr[0] == NUCLEO_IP and data and data[0] == PACKET_TYPE["PONG"]:
            if len(data) >= 9:
                try:
                    _, sent_time = struct.unpack("<Bd", data[:9])
                    ping_stats["rtt_ms"] = (time.time() - sent_time) * 1000
                except struct.error:
                    logger.warning("不正な形式のPONGパケットを受信しました。")

        # ---------- オドメトリ ----------
        elif addr[0] == NUCLEO_IP and data and data[0] == PACKET_TYPE["ODOMETRY_DATA"]:

            if len(data) >= 13:
                try:
                    _, x, y, theta = struct.unpack("<Bfff", data[:13])

                    message = json.dumps({
                        "type": "odom",
                        "x": x,
                        "y": y,
                        "theta": theta
                    })

                    asyncio.create_task(
                        broadcast_odom(message)
                    )

                except struct.error:
                    logger.warning("不正なオドメトリパケットを受信しました。")
    def error_received(self, exc):
        """データ受信中にエラーが発生したときに呼ばれる。"""
        logger.error(f"UDP受信エラー: {exc}")

    def connection_lost(self, exc):
        """UDP接続が（何らかの理由で）失われたときに呼ばれる。"""
        logger.warning("UDP接続が失われました。")

async def main():
    """
    サーバーを起動するためのメインの非同期関数。
    """
    local_ip = get_local_ip()
    loop = asyncio.get_running_loop()
    
    # UDPの送受信を行うためのエンドポイントを作成
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    sock.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    sock.bind(("", NUCLEO_PORT_RX))


    mreq = struct.pack(
        "4s4s",
        socket.inet_aton("239.0.0.1"),
        socket.inet_aton(local_ip)
    )

    sock.setsockopt(
        socket.IPPROTO_IP,
        socket.IP_ADD_MEMBERSHIP,
        mreq
    )


    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UdpProtocol(),
        sock=sock
    )

    logger.info("--- サーバーが起動しました ---")
    logger.info(f"UIからの接続先: ws://{local_ip}:{WEBSOCKET_PORT}")
    logger.info(f"UDP送信先: {NUCLEO_IP}:{NUCLEO_PORT_TX}, UDP受信ポート: {NUCLEO_PORT_RX}")

    # マイコンへのPing計測タスクをバックグラウンドで開始
    ping_task = asyncio.create_task(run_ping_cycle(transport))
    
    # WebSocketハンドラにUDPトランスポートを渡すため、partialで新しい関数を作成
    handler_with_transport = partial(websocket_handler, transport=transport)
    
    # WebSocketサーバーを起動し、接続を待ち受ける
    async with websockets.serve(handler_with_transport, SERVER_HOST, WEBSOCKET_PORT, ping_interval=20, ping_timeout=20):
        # サーバーとPingタスクが終了するまで待機
        await asyncio.gather(ping_task, asyncio.Future())

if __name__ == "__main__":
    """
    このスクリプトが直接実行されたときのエントリーポイント。
    """
    try:
        # 非同期のmain関数を実行
        asyncio.run(main())
    except KeyboardInterrupt:
        # Ctrl+C が押されたらサーバーを正常にシャットダウン
        logger.info("サーバーをシャットダウンします。")

