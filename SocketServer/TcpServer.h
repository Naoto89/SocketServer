#pragma once

#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
#include <algorithm>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

class TcpServer
{
public:
    using MessageHandler = std::function<void(
        uint32_t,
        uint32_t,
        uint32_t,
        std::vector<uint8_t>)>;

private:
    /// <summary>
    /// 受信バッファに対する処理関数オブジェクト
    /// </summary>
    MessageHandler m_handler;

    /// <summary>
    /// 停止指示
    /// </summary>
    std::atomic<bool> m_stop{ false };

    /// <summary>
    /// 実行中かどうか
    /// </summary>
    std::atomic<bool> m_running{ false };

    /// <summary>
    /// 接続中かどうか
    /// </summary>
    std::atomic<bool> m_connected{ false };

    /// <summary>
    /// サーバー側としてのポート番号
    /// </summary>
    uint16_t m_port = 0;

    /// <summary>
    /// ワーカースレッドの関数オブジェクト
    /// </summary>
    std::thread m_thread;

    /// <summary>
    /// 送信キューの処理のためのミューテックス
    /// </summary>
    std::mutex m_sendMutex;

    /// <summary>
    /// 送信キュー
    /// </summary>
    std::deque<std::vector<uint8_t>> m_sendQueue;

    /// <summary>
    /// だ送信し終わっていないデータの総バイト数
    /// </summary>
    size_t m_queuedBytes = 0;

    /// <summary>
    /// サーバー側の接続待機処理のミューテックス
    /// </summary>
    std::mutex m_retryMutex;

    /// <summary>
    /// サーバーの接続待機処理のリトライまでの待機処理用
    /// </summary>
    std::condition_variable m_retryCv;

    /// <summary>
    /// ソケット初期化実行済みかどうか
    /// </summary>
    bool m_wsaStarted = false;


    static constexpr uint32_t HeaderSize = 16;
    static constexpr uint32_t MaxFrameSize = 256 * 1024 * 1024;

public:
    /// <summary>
    /// コンストラクタ
    /// </summary>
    /// <param name="handler"></param>
    explicit TcpServer(MessageHandler handler)
        : m_handler(std::move(handler))
    {}

    /// <summary>
    /// デストラクタ
    /// </summary>
    ~TcpServer()
    {
        Stop();
    }

    /// <summary>
    /// コピーコンストラクタを禁止
    /// </summary>
    /// <param name=""></param>
    TcpServer(const TcpServer&) = delete;

    /// <summary>
    /// コピー代入演算子を禁止
    /// </summary>
    /// <param name=""></param>
    /// <returns></returns>
    TcpServer& operator=(const TcpServer&) = delete;

    /// <summary>
    /// サーバーの開始
    /// </summary>
    /// <param name="port"></param>
    /// <returns></returns>
    bool Start(uint16_t port)
    {
        if (m_running.exchange(true))
        {
            return false;
        }

        WSADATA wsaData{};

        if (::WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
        {
            m_running = false;
            return false;
        }

        m_wsaStarted = true;
        m_stop = false;
        m_port = port;

        try
        {
            m_thread = std::thread(&TcpServer::Worker, this);
        }
        catch (...)
        {
            ::WSACleanup();
            m_wsaStarted = false;
            m_running = false;
            return false;
        }

        return true;
    }

    /// <summary>
    /// サーバーの停止
    /// </summary>
    void Stop()
    {
        m_stop = true;
        m_retryCv.notify_all();

        if (m_thread.joinable())
        {
            m_thread.join();
        }

        m_connected = false;
        m_running = false;

        if (m_wsaStarted)
        {
            ::WSACleanup();
            m_wsaStarted = false;
        }
    }

    /// <summary>
    /// サーバー開始中か
    /// </summary>
    /// <returns></returns>
    bool IsRunning() const
    {
        return m_running;
    }

    /// <summary>
    /// クライアントが接続しているか
    /// </summary>
    /// <returns></returns>
    bool IsConnected() const
    {
        return m_connected;
    }

    /// <summary>
    /// メッセージパケット送信
    /// </summary>
    /// <param name="commandId"></param>
    /// <param name="requestId"></param>
    /// <param name="messageType"></param>
    /// <param name="payload"></param>
    /// <returns></returns>
    bool SendMessage(
        uint32_t commandId,
        uint32_t requestId,
        uint32_t messageType,
        const std::vector<uint8_t>& payload)
    {
        if (m_stop || !m_connected)
        {
            return false;
        }

		const uint64_t totalSize = static_cast<uint64_t>(HeaderSize) + payload.size();

        if (totalSize > MaxFrameSize)
        {
            return false;
        }

        std::vector<uint8_t> frame;
        frame.reserve(static_cast<size_t>(totalSize));

        WriteU32(frame, static_cast<uint32_t>(totalSize));
        WriteU32(frame, commandId);
        WriteU32(frame, requestId);
        WriteU32(frame, messageType);

        frame.insert(frame.end(), payload.begin(), payload.end());

        {
            std::lock_guard<std::mutex> lock(m_sendMutex);

            if (m_stop || !m_connected)
            {
                return false;
            }

            constexpr size_t MaxQueuedBytes = 64 * 1024 * 1024;

            if (frame.size() > MaxQueuedBytes - m_queuedBytes)
            {
                return false;
            }

            m_queuedBytes += frame.size();
            m_sendQueue.push_back(std::move(frame));
        }

        return true;
    }

private:

    /// <summary>
    /// 指定されたベクタに32bit変数をリトルエンディアンで書き込む
    /// </summary>
    /// <param name="out"></param>
    /// <param name="value"></param>
    static void WriteU32(
        std::vector<uint8_t>& out,
        uint32_t value)
    {
        out.push_back(static_cast<uint8_t>(value));
        out.push_back(static_cast<uint8_t>(value >> 8));
        out.push_back(static_cast<uint8_t>(value >> 16));
        out.push_back(static_cast<uint8_t>(value >> 24));
    }

    /// <summary>
    /// 指定されたポインタから32bit変数をリトルエンディアンで読み込む
    /// </summary>
    /// <param name="p"></param>
    /// <returns></returns>
    static uint32_t ReadU32(const uint8_t* p)
    {
        return
            static_cast<uint32_t>(p[0]) |
            (static_cast<uint32_t>(p[1]) << 8) |
            (static_cast<uint32_t>(p[2]) << 16) |
            (static_cast<uint32_t>(p[3]) << 24);
    }

    /// <summary>
    /// ノンブロッキングモードの設定：
    /// sendやrecvで、送受信すべきデータがない場合に、通常であればブロッキング＝待機するが、
    /// ブロッキングしないようにする。
    /// </summary>
    /// <param name="socket"></param>
    /// <returns></returns>
    static bool SetNonBlocking(SOCKET socket)
    {
        u_long mode = 1;

        return ::ioctlsocket(socket, FIONBIO, &mode) == 0;
    }

    /// <summary>
    /// ソケットのクローズ
    /// </summary>
    /// <param name="socket"></param>
    static void CloseSocket(SOCKET& socket)
    {
        if (socket != INVALID_SOCKET)
        {
            ::closesocket(socket);
            socket = INVALID_SOCKET;
        }
    }

    /// <summary>
    /// サーバーソケットインスタンスの生成
    /// </summary>
    /// <returns></returns>
    SOCKET CreateListener()
    {
		SOCKET socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

        if (socket == INVALID_SOCKET)
        {
            return INVALID_SOCKET;
        }

        BOOL reuse = TRUE;

        ::setsockopt(
            socket,
            SOL_SOCKET,
            SO_REUSEADDR,
            reinterpret_cast<const char*>(&reuse),
            sizeof(reuse));

        if (!SetNonBlocking(socket))
        {
            CloseSocket(socket);
            return INVALID_SOCKET;
        }

        /////////////////////////////////////////////////////////////////
        // address.sin_addr.s_addr = htonl(INADDR_ANY);
        // これは任意のIPアドレスで接続待機する意味。
        /////////////////////////////////////////////////////////////////
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(m_port);
        address.sin_addr.s_addr = htonl(INADDR_ANY);

        if (::bind(
            socket,
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)) == SOCKET_ERROR)
        {
            CloseSocket(socket);
            return INVALID_SOCKET;
        }

        if (::listen(socket, SOMAXCONN) == SOCKET_ERROR)
        {
            CloseSocket(socket);
            return INVALID_SOCKET;
        }

        return socket;
    }

    /// <summary>
    /// 送信キューをクリア
    /// </summary>
    void ClearSendQueue()
    {
        std::lock_guard<std::mutex> lock(m_sendMutex);

        m_sendQueue.clear();
        m_queuedBytes = 0;
    }

    /// <summary>
    /// クライアントとの接続状態を切断する
    /// </summary>
    /// <param name="client"></param>
    void DisconnectClient(SOCKET& client)
    {
        CloseSocket(client);
        m_connected = false;
        ClearSendQueue();
    }

    /// <summary>
    /// リトライ前に待機する
    /// </summary>
    /// <returns></returns>
    bool WaitBeforeRetry(int durationMs = 1000)
    {
        std::unique_lock<std::mutex> lock(m_retryMutex);

        m_retryCv.wait_for(
            lock,
            std::chrono::milliseconds(durationMs),
            [this]()
            {
                return m_stop.load();
            });

        return !m_stop;
    }

    /// <summary>
    /// ループ処理1回の実行で、
    /// 受信すべきパケットがあれば受信処理を、
    /// 送信すべきパケットがあれば送信処理を行う。
    /// </summary>
    void Worker()
    {
        SOCKET listener = INVALID_SOCKET;
        SOCKET client = INVALID_SOCKET;

        std::deque<std::vector<uint8_t>> pending;
        size_t sendOffset = 0;
        std::vector<uint8_t> receiveBuffer;

        while (!m_stop)
        {
            if (!EnsureListener(listener))
            {
                if (!WaitBeforeRetry())
                {
                    break;
                }

                continue;
            }

            MoveQueuedMessages(client, pending);

            fd_set readSet;
            fd_set writeSet;

            /////////////////////////////////////////////////////////////////
            // select関数の仕様に従い、毎回FdSetを設定する
            /////////////////////////////////////////////////////////////////
			BuildFdSets(listener, client, pending, readSet, writeSet);

            const bool hadClient = client != INVALID_SOCKET;

            timeval timeout{};
            timeout.tv_sec = 0;
            timeout.tv_usec = 100000;

            const int selected = ::select(
                0,
                &readSet,
                &writeSet,
                nullptr,
                &timeout);

            if (m_stop)
            {
                break;
            }

            if (selected == SOCKET_ERROR)
            {
				ResetClient(client, pending, sendOffset, receiveBuffer);
                CloseSocket(listener);
                continue;
            }

            if (selected == 0)
            {
                continue;
            }

            if (FD_ISSET(listener, &readSet))
            {
                /////////////////////////////////////////////////////////////////
                // クライアント接続受付処理：
                // client変数に有効な値が格納される
                /////////////////////////////////////////////////////////////////
				AcceptClient(listener, client, pending, sendOffset, receiveBuffer);
            }

            if (listener == INVALID_SOCKET)
            {
                continue;
            }

            // 今回のselect時点ですでに接続されていたクライアントだけを処理する。
            // 新規接続したソケットは、次のselectから監視する。
            if (!hadClient || client == INVALID_SOCKET)
            {
                continue;
            }

            /////////////////////////////////////////////////////////////////
            // クライアントソケットが読み取り可能となったら受信処理：
            // データが来ていたら処理するが、来ていなかったらすぐ戻る。
            /////////////////////////////////////////////////////////////////
            if (FD_ISSET(client, &readSet))
            {
				if (!ProcessReceivePackets(client, pending, sendOffset, receiveBuffer))
                {
                    continue;
                }
            }

            /////////////////////////////////////////////////////////////////
            // クライアントソケットが書き込み可能となったら送信処理
            /////////////////////////////////////////////////////////////////
			if (client != INVALID_SOCKET && FD_ISSET(client, &writeSet))
            {
				ProcessSend(client, pending, sendOffset, receiveBuffer);
            }
        }

        /////////////////////////////////////////////////////////////////
        // 停止要求でループを抜けたので、切断処理
        /////////////////////////////////////////////////////////////////
        DisconnectClient(client);
        CloseSocket(listener);

        m_running = false;
    }

    /// <summary>
    /// ソケットインスタンスの存在を確認し、
    /// 存在していなければ新たに生成する
    /// </summary>
    /// <param name="listener"></param>
    /// <returns></returns>
    bool EnsureListener(SOCKET& listener)
    {
        if (listener != INVALID_SOCKET)
        {
            return true;
        }

        listener = CreateListener();

        return listener != INVALID_SOCKET;
    }

	/// <summary>
	/// 送信キューから、送信用バッファにデータを移動させる
	/// </summary>
	/// <param name="client"></param>
	/// <param name="pending"></param>
	void MoveQueuedMessages(SOCKET client, std::deque<std::vector<uint8_t>>& pending)
    {
        if (client == INVALID_SOCKET)
        {
            return;
        }

        std::lock_guard<std::mutex> lock(m_sendMutex);

        while (!m_sendQueue.empty())
        {
            pending.push_back(std::move(m_sendQueue.front()));
            m_sendQueue.pop_front();
        }

        //m_queuedBytes = 0;
    }

    /// <summary>
    /// select() 用の監視設定
    /// </summary>
    /// <param name="listener"></param>
    /// <param name="client"></param>
    /// <param name="pending"></param>
    /// <param name="readSet"></param>
    /// <param name="writeSet"></param>
    void BuildFdSets(
        SOCKET listener,
        SOCKET client,
        const std::deque<std::vector<uint8_t>>& pending,
        fd_set& readSet,
        fd_set& writeSet)
    {
        FD_ZERO(&readSet);
        FD_ZERO(&writeSet);

        FD_SET(listener, &readSet);

        if (client != INVALID_SOCKET)
        {
            FD_SET(client, &readSet);

            if (!pending.empty())
            {
                FD_SET(client, &writeSet);
            }
        }
    }

    /// <summary>
    /// クライアント接続受付
    /// </summary>
    /// <param name="listener"></param>
    /// <param name="client"></param>
    /// <param name="pending"></param>
    /// <param name="sendOffset"></param>
    /// <param name="receiveBuffer"></param>
    void AcceptClient(
        SOCKET& listener,
        SOCKET& client,
        std::deque<std::vector<uint8_t>>& pending,
        size_t& sendOffset,
        std::vector<uint8_t>& receiveBuffer)
    {
        while (true)
        {
            /////////////////////////////////////////////////////////////////
            // 接続要求が来ているか確認してすぐ戻る
            /////////////////////////////////////////////////////////////////
            SOCKET accepted = ::accept(listener, nullptr, nullptr);

            /////////////////////////////////////////////////////////////////
            // 無効なソケットならソケットを一旦クローズ
            /////////////////////////////////////////////////////////////////
            if (accepted == INVALID_SOCKET)
            {
                const int error = ::WSAGetLastError();

                if (error == WSAEWOULDBLOCK)
                {
                    break;
                }
				ResetClient(client, pending, sendOffset, receiveBuffer);
                CloseSocket(listener);
                break;
            }

            if (client != INVALID_SOCKET)
            {
                ::closesocket(accepted);
                continue;
            }

            if (!SetNonBlocking(accepted))
            {
                ::closesocket(accepted);
                continue;
            }

            client = accepted;
            m_connected = true;

            pending.clear();
            sendOffset = 0;
            receiveBuffer.clear();
        }
    }

    /// <summary>
    /// 切断時の後処理
    /// </summary>
    /// <param name="client"></param>
    /// <param name="pending"></param>
    /// <param name="sendOffset"></param>
    /// <param name="receiveBuffer"></param>
    void ResetClient(
        SOCKET& client,
        std::deque<std::vector<uint8_t>>& pending,
        size_t& sendOffset,
        std::vector<uint8_t>& receiveBuffer)
    {
        DisconnectClient(client);

        pending.clear();
        sendOffset = 0;
        receiveBuffer.clear();
    }

    /// <summary>
    /// 受信処理
    /// </summary>
    /// <param name="client"></param>
    /// <param name="pending"></param>
    /// <param name="sendOffset"></param>
    /// <param name="receiveBuffer"></param>
    /// <returns></returns>
    bool ProcessReceivePackets(
        SOCKET& client,
        std::deque<std::vector<uint8_t>>& pending,
        size_t& sendOffset,
        std::vector<uint8_t>& receiveBuffer)
    {
        uint8_t temp[8192];

        while (true)
        {
            const int received = ::recv(
                client,
                reinterpret_cast<char*>(temp),
                sizeof(temp),
                0);

            /////////////////////////////////////////////////////////////////
            // データを受信
            /////////////////////////////////////////////////////////////////
            if (received > 0)
            {
                /////////////////////////////////////////////////////////////////
                // insert処理はsizeをインクリメントする。
                /////////////////////////////////////////////////////////////////
                receiveBuffer.insert(
                    receiveBuffer.end(),
                    temp,
                    temp + received);

                while (true)
                {
                    if (receiveBuffer.size() < HeaderSize)
                    {
                        break;
                    }

                    const uint32_t frameSize =
                        ReadU32(receiveBuffer.data());

                    if (frameSize < HeaderSize ||
                        frameSize > MaxFrameSize)
                    {
                        ResetClient(
                            client,
                            pending,
                            sendOffset,
                            receiveBuffer);

                        return false;
                    }

                    if (receiveBuffer.size() < frameSize)
                    {
                        break;
                    }

					const uint32_t commandId = ReadU32(receiveBuffer.data() + 4);
					const uint32_t requestId = ReadU32(receiveBuffer.data() + 8);
					const uint32_t messageType = ReadU32(receiveBuffer.data() + 12);

                    std::vector<uint8_t> payload(
                        receiveBuffer.begin() + HeaderSize,
                        receiveBuffer.begin() + frameSize);

                    /////////////////////////////////////////////////////////////////
                    // erase処理はsizeをデクリメントし、削除された部分を、
                    // それより後ろの要素でコピーして、先頭側に詰める。
                    /////////////////////////////////////////////////////////////////
                    receiveBuffer.erase(
                        receiveBuffer.begin(),
                        receiveBuffer.begin() + frameSize);

                    try
                    {
                        /////////////////////////////////////////////////////////////////
                        // 受信パケットに対する処理
                        /////////////////////////////////////////////////////////////////
                        if (m_handler)
                        {
							m_handler(commandId, requestId, messageType, std::move(payload));
						}
                    }
                    catch (...)
                    {
                        // コールバックの例外を通信スレッドの外へ出さない。
                    }

                    if (m_stop)
                    {
						ResetClient(client, pending, sendOffset, receiveBuffer);
                        return false;
                    }
                }

                continue;
            }

            /////////////////////////////////////////////////////////////////
            // 切断を検知
            /////////////////////////////////////////////////////////////////
            if (received == 0)
            {
				ResetClient(client, pending, sendOffset, receiveBuffer);
                return false;
            }

            const int error = ::WSAGetLastError();
            if (error == WSAEWOULDBLOCK)
            {
                return true;
            }
			ResetClient(client, pending, sendOffset, receiveBuffer);
            return false;
        }
    }

    /// <summary>
    /// 送信処理
    /// </summary>
    /// <param name="client"></param>
    /// <param name="pending"></param>
    /// <param name="sendOffset"></param>
    /// <param name="receiveBuffer"></param>
    void ProcessSend(
        SOCKET& client,
        std::deque<std::vector<uint8_t>>& pending,
        size_t& sendOffset,
        std::vector<uint8_t>& receiveBuffer)
    {
        while (!pending.empty())
        {
            auto& frame = pending.front();

            const size_t remaining = frame.size() - sendOffset;

            const int toSend = static_cast<int>(
                std::min(
                    remaining,
                    static_cast<size_t>(
                        (std::numeric_limits<int>::max)())));

            const int sent = ::send(
                client,
                reinterpret_cast<const char*>(
                    frame.data() + sendOffset),
                toSend,
                0);

            if (sent > 0)
            {
                sendOffset += static_cast<size_t>(sent);

                if (sendOffset == frame.size())
                {
                    pending.pop_front();
                    sendOffset = 0;
                }
                {
                
                    std::lock_guard<std::mutex> lock(m_sendMutex);
                    m_queuedBytes -= static_cast<size_t>(sent);
                }
                continue;
            }

			if (sent == SOCKET_ERROR && ::WSAGetLastError() == WSAEWOULDBLOCK)
            {
                return;
            }

			ResetClient(client, pending, sendOffset, receiveBuffer);

            return;
        }
    }
};