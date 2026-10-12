
#pragma once

#include <windows.h>
#include <atlstr.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <ctime>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <iostream>

class Logger
{
public:
    enum class Level
    {
        Debug,
        Info,
        Warning,
        Error
    };

    struct LogEntry
    {
        // ログ発生時刻を保持し、キュー処理が日付をまたいでも
        // 発生した日付のファイルに記録できるようにする。
        std::chrono::system_clock::time_point time;
        Level level;
        std::wstring message;
    };

    static Logger& Instance()
    {
        // 関数内staticにより、ロガーのインスタンスを1つに限定する。
        static Logger instance;
        return instance;
    }

    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    bool Start(
        const std::filesystem::path& directory = L"logs",
        const std::wstring& filePrefix = L"application")
    {
        std::lock_guard<std::mutex> lock(m_mutex);

        if (m_started)
        {
            return false;
        }

        try
        {
            m_logDirectory = directory;
            m_filePrefix = filePrefix;

            // 初回のログ出力時にディレクトリが存在しない問題を防ぐ。
            std::filesystem::create_directories(m_logDirectory);

            m_stopping = false;
            m_started = true;

            try
            {
                m_worker = std::thread(&Logger::Worker, this);
            }
            catch (...)
            {
                m_started = false;
                throw;
            }
        }
        catch (const std::exception& ex)
        {
            std::cerr << "Logger start failed: "
                << ex.what() << '\n';
            return false;
        }

        return true;
    }

    void Stop()
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);

            if (!m_started)
            {
                return;
            }

            // Workerに終了を知らせ、キューに残ったログを処理させる。
            m_stopping = true;
        }

        m_cv.notify_all();

        // Workerがファイルを使い終わるまで待ってから停止状態にする。
        if (m_worker.joinable())
        {
            m_worker.join();
        }

        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_started = false;
            m_stopping = false;
        }
    }

    void Debug(const std::wstring& message)
    {
        Write(Level::Debug, message);
    }

    void Info(const std::wstring& message)
    {
        Write(Level::Info, message);
    }

    void Warning(const std::wstring& message)
    {
        Write(Level::Warning, message);
    }

    void Error(const std::wstring& message)
    {
        Write(Level::Error, message);
    }

    // CStringからwstringへの変換では文字コード変換は発生しないが、
    // キューに独立した文字列を保持するためのコピーは発生する。
    void Debug(const CString& message)
    {
        Debug(ToWString(message));
    }

    void Info(const CString& message)
    {
        Info(ToWString(message));
    }

    void Warning(const CString& message)
    {
        Warning(ToWString(message));
    }

    void Error(const CString& message)
    {
        Error(ToWString(message));
    }

private:
    using SystemClock = std::chrono::system_clock;
    using SteadyClock = std::chrono::steady_clock;

    static constexpr auto FlushInterval = std::chrono::seconds(1);

    Logger() = default;

    ~Logger()
    {
        Stop();
    }

    static std::wstring ToWString(const CString& message)
    {
        // UnicodeビルドではCStringの文字列はwchar_tとして取得できる。
        return std::wstring(message.GetString(),
            static_cast<size_t>(message.GetLength()));
    }

    void Write(Level level, const std::wstring& message)
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);

            // 開始前または停止要求後のログは受け付けない。
            if (!m_started || m_stopping)
            {
                return;
            }

            // キューには呼び出し元から独立した文字列を格納する。
            m_queue.push_back(
                LogEntry{ SystemClock::now(), level, message });
        }

        // 待機中のWorkerにログの追加を通知する。
        m_cv.notify_one();
    }

    static std::wstring GetDateString(SystemClock::time_point time)
    {
        const std::time_t value = SystemClock::to_time_t(time);

        std::tm localTime{};
        localtime_s(&localTime, &value);

        std::wostringstream stream;
        stream << std::put_time(&localTime, L"%Y%m%d");
        return stream.str();
    }

    static SystemClock::time_point GetNextMidnight(
        SystemClock::time_point now)
    {
        const std::time_t value = SystemClock::to_time_t(now);

        std::tm localTime{};
        localtime_s(&localTime, &value);

        // 翌日の午前0時を求める。月末・年末もmktimeに計算させる。
        localTime.tm_mday += 1;
        localTime.tm_hour = 0;
        localTime.tm_min = 0;
        localTime.tm_sec = 0;

        const std::time_t next = std::mktime(&localTime);

        if (next == static_cast<std::time_t>(-1))
        {
            // 時刻変換に失敗した場合のフォールバック。
            return now + std::chrono::hours(24);
        }

        return SystemClock::from_time_t(next);
    }

    static const wchar_t* GetLevelString(Level level)
    {
        switch (level)
        {
        case Level::Debug:   return L"DEBUG";
        case Level::Info:    return L"INFO";
        case Level::Warning: return L"WARNING";
        case Level::Error:   return L"ERROR";
        default:             return L"UNKNOWN";
        }
    }

    // UTF-16のwstringをUTF-8へ変換する。
    // ログファイルをUTF-8に統一し、文字化けを避ける。
    static std::string WideToUtf8(const std::wstring& text)
    {
        if (text.empty())
        {
            return {};
        }

        const int required = WideCharToMultiByte(
            CP_UTF8,
            WC_ERR_INVALID_CHARS,
            text.data(),
            static_cast<int>(text.size()),
            nullptr,
            0,
            nullptr,
            nullptr);

        if (required == 0)
        {
            throw std::runtime_error("UTF-8 conversion failed");
        }

        std::string result(static_cast<size_t>(required), '\0');

        const int converted = WideCharToMultiByte(
            CP_UTF8,
            WC_ERR_INVALID_CHARS,
            text.data(),
            static_cast<int>(text.size()),
            result.data(),
            required,
            nullptr,
            nullptr);

        if (converted == 0)
        {
            throw std::runtime_error("UTF-8 conversion failed");
        }

        return result;
    }

    bool EnsureLogFile(SystemClock::time_point time)
    {
        const std::wstring date = GetDateString(time);

        if (m_file.is_open() && m_currentLogDate == date)
        {
            return true;
        }

        // ファイルを切り替える前に、旧ファイルのバッファを排出する。
        if (m_file.is_open())
        {
            m_file.flush();
            m_file.close();
        }

        const auto path = m_logDirectory /
            (m_filePrefix + L"_" + date + L".log");

        // 同じ日付のファイルがあれば追記し、既存ログを保持する。
        m_file.open(path, std::ios::out | std::ios::app);

        if (!m_file.is_open())
        {
            std::wcerr << L"Logger: failed to open "
                << path.wstring() << L'\n';
            m_currentLogDate.clear();
            return false;
        }

        m_currentLogDate = date;
        return true;
    }

    void WriteFile(const LogEntry& entry)
    {
        if (!EnsureLogFile(entry.time))
        {
            return;
        }

        const std::time_t value = SystemClock::to_time_t(entry.time);

        std::tm localTime{};
        localtime_s(&localTime, &value);

        // ログ1件をUTF-8へ変換し、バイナリストリームへ書き込む。
        std::wostringstream line;
        line << std::put_time(&localTime, L"%Y-%m-%d %H:%M:%S")
            << L" [" << GetLevelString(entry.level) << L"] "
            << entry.message << L"\n";

        const std::string utf8 = WideToUtf8(line.str());
        m_file.write(utf8.data(),
            static_cast<std::streamsize>(utf8.size()));

        if (!m_file)
        {
            std::cerr << "Logger: file write failed\n";
        }
    }

    void Worker()
    {
        auto nextMidnight = GetNextMidnight(SystemClock::now());
        auto nextFlush = SteadyClock::now() + FlushInterval;

        // 起動時点の日付に対応するログファイルを準備する。
        EnsureLogFile(SystemClock::now());

        while (true)
        {
            std::deque<LogEntry> entries;
            bool stopping = false;

            {
                std::unique_lock<std::mutex> lock(m_mutex);

                // 1秒ごとのflush期限と日付変更時刻のうち、
                // 先に到来する時刻まで待機する。
                const auto steadyNow = SteadyClock::now();
                const auto systemNow = SystemClock::now();

                const auto untilFlush = std::max(
                    SteadyClock::duration::zero(),
                    nextFlush - steadyNow);

                const auto untilMidnight = std::max(
                    SystemClock::duration::zero(),
                    nextMidnight - systemNow);

                const auto waitDuration = std::min(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        untilFlush),
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        untilMidnight));

                m_cv.wait_for(
                    lock,
                    waitDuration,
                    [this]()
                    {
                        return m_stopping || !m_queue.empty();
                    });

                // キューをローカルに移し、ログ追加側のロック待ちを短くする。
                entries.swap(m_queue);
                stopping = m_stopping;
            }

            // ファイルI/O中にmutexを保持しない。
            for (const auto& entry : entries)
            {
                try
                {
                    WriteFile(entry);
                }
                catch (const std::exception& ex)
                {
                    std::cerr << "Logger write exception: "
                        << ex.what() << '\n';
                }
            }

            const auto steadyNow = SteadyClock::now();
            const auto systemNow = SystemClock::now();

            // flushをログごとに行わず、原則1秒ごとにまとめる。
            if (steadyNow >= nextFlush)
            {
                if (m_file.is_open())
                {
                    m_file.flush();
                }

                nextFlush = steadyNow + FlushInterval;
            }

            // 日付変更時には、新しい日付のファイルを開く。
            if (systemNow >= nextMidnight)
            {
                EnsureLogFile(systemNow);
                nextMidnight = GetNextMidnight(systemNow);
            }

            // 停止要求があり、キューの処理も終わっていれば終了する。
            if (stopping && entries.empty())
            {
                break;
            }
        }

        // 正常終了時には残ったバッファを排出してファイルを閉じる。
        if (m_file.is_open())
        {
            m_file.flush();
            m_file.close();
        }
    }

private:
    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::deque<LogEntry> m_queue;
    std::thread m_worker;

    bool m_started = false;
    bool m_stopping = false;

    // ファイル関連のメンバーはWorkerスレッドだけが操作する。
    std::filesystem::path m_logDirectory = L"logs";
    std::wstring m_filePrefix = L"application";
    std::wstring m_currentLogDate;
    std::ofstream m_file;
};
