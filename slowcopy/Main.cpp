#include <wil/cppwinrt.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <csignal>
#include <functional>
#include <filesystem>
#include <sys/types.h>
#include <sys/stat.h>
#include <winstring.h>
#include <winrt/Windows.Foundation.h>
#include <winrt/Windows.Foundation.Collections.h>
#include <winrt/Windows.Data.Json.h>
#include <wil/resource.h>

using namespace winrt;
using namespace Windows::Foundation;
using namespace Windows::Data::Json;

class AFile;

class Chunk
{
public:
    Chunk(AFile* parent, uint64_t position, uint32_t size) :
        m_file(parent),
        m_position(position),
        m_chunkSize(size),
        m_dataSize(0)
    {}

    AFile* m_file;
    wil::unique_file m_hfile;
    uint64_t m_position;
    uint32_t m_chunkSize;
    uint32_t m_dataSize;
    wil::unique_process_heap_ptr<unsigned char> m_buffer;
    LARGE_INTEGER m_startTime{};

    FILE* DetachFp()
    {
        FILE* fp = m_hfile.release();
        return fp;
    }

    void AttachFp(FILE* fp)
    {
        m_hfile.reset(fp);
    }

    fire_and_forget Start();
};

class AFile
{
public:
    AFile()
    {
        m_size = 0;
        m_bytesCopied = 0;
        m_bytesPerSecond = 0;
        m_nextChunkPosition = 0;
        m_currentChunkSize = 0x10000; // start from 64k chunk
    }

    std::string AFile::GetChunkFileName(const char* dest);
    void SaveChunkJson(const char* dest);
    bool LoadChunkJson(const char* dest);
    void StartCopying(const char* dest);
    void ReportCompletion(uint64_t position, bool isSuccess);

    std::string m_name;
    std::string m_dest;
    wil::unique_file m_hfile;
    wil::unique_file m_destFile;
    uint64_t m_size;
    uint64_t m_nextChunkPosition;
    uint32_t m_currentChunkSize;
    uint64_t m_bytesCopied;
    uint64_t m_bytesPerSecond;
    std::unordered_map<uint64_t, Chunk*> m_chunks;

    wil::srwlock m_lock;

    std::vector<std::string> m_outStrings;
};

fire_and_forget Chunk::Start()
{
    m_buffer.reset((unsigned char*)::HeapAlloc(GetProcessHeap(), 0, m_chunkSize));
    THROW_HR_IF(E_OUTOFMEMORY, !m_buffer);

    if (!m_hfile)
    {
        auto err = fopen_s(&m_hfile, m_file->m_name.data(), "rb");
        if (err != 0)
        {
            m_file->ReportCompletion(m_position, false);
            co_return;
        }
    }
    co_await resume_background();

    QueryPerformanceCounter(&m_startTime);
    auto err = _fseeki64(m_hfile.get(), m_position, SEEK_SET);
    FAIL_FAST_IF(err != 0);

    m_dataSize = 0;
    while (m_dataSize < m_chunkSize)
    {
        auto bytesToRead = min(0x4000, m_chunkSize - m_dataSize);
        auto bytes = fread_s(m_buffer.get() + m_dataSize, m_chunkSize - m_dataSize, 1, bytesToRead, m_hfile.get());
        m_dataSize += (uint32_t)bytes;
        if (bytes != bytesToRead)
        {
            if (feof(m_hfile.get()))
            {
                auto pos = _ftelli64(m_hfile.get());
                (pos);
                // report chunk success
                m_file->ReportCompletion(m_position, true);
                co_return;
            }
            else
            {
                // report chunk failure
                m_file->ReportCompletion(m_position, false);
                co_return;
            }
        }
    }
    // report chunk success
    m_file->ReportCompletion(m_position, true);
    co_return;
}

std::string CommaNumber(uint64_t n)
{
    if (n > 1000000)
    {
        return std::to_string(n / 1000000) + "," + (std::to_string(n % 1000000 + 1000000).c_str() + 1);
    }
    else if (n > 1000)
    {
        return std::to_string(n / 1000) + "," + (std::to_string(n % 1000 + 1000).c_str() + 1);
    }
    else
    {
        return std::to_string(n);
    }
}

void AFile::ReportCompletion(uint64_t position, bool isSuccess)
{
    auto autolock = m_lock.lock_exclusive();
    auto chunk = m_chunks.at(position);
    m_chunks.erase(position);
    LARGE_INTEGER endTime;
    QueryPerformanceCounter(&endTime);
    auto microSeconds = endTime.QuadPart - chunk->m_startTime.QuadPart;

    m_bytesCopied = m_bytesCopied + chunk->m_dataSize;
    std::ostringstream outs;
    if (chunk->m_dataSize > 0)
    {
        // Write the chunk to destination file
        auto err = _fseeki64(m_destFile.get(), position, SEEK_SET);
        FAIL_FAST_IF(err != 0);
        fwrite(chunk->m_buffer.get(), 1, chunk->m_dataSize, m_destFile.get());
        fflush(m_destFile.get());

        double rate = 0.0;
        char* rateUnit = " Bytes/s";
        if (microSeconds > 10)
        {
            rate = 10000000.0 * chunk->m_dataSize / microSeconds;
            if (rate > 1000000.0)
            {
                rate = rate / 1000000.0;
                rateUnit = " MB/s";
            }
            else if (rate > 1000.0)
            {
                rate = rate / 1000.0;
                rateUnit = " KB/s";
            }
        }

        outs.setf(std::ios::fixed);
        outs.precision(1);
        outs << 100.0 * m_bytesCopied / m_size << "% done " << CommaNumber(m_bytesCopied) << " bytes, chunk @" << CommaNumber(position) << " " << CommaNumber(chunk->m_dataSize) << " bytes " << rate << rateUnit;
    }
    else
    {
        outs << 100.0 * m_bytesCopied / m_size << "% done " << CommaNumber(m_bytesCopied) << " bytes, chunk @" << CommaNumber(position) << " " << CommaNumber(chunk->m_dataSize) << " bytes";
    }

    // Two cases need to create new chunk
    uint64_t newPosition = 0;
    uint32_t newChunkSize = 0;
    if (chunk->m_dataSize == chunk->m_chunkSize)
    {
        if (m_nextChunkPosition < m_size)
        {
            // Need a new chunk at nexChunkPosition
            // figure out next chunk size that doesn't take too long to copy
            uint64_t remainingSize = m_size - m_nextChunkPosition;
            uint64_t chunkSize = m_currentChunkSize;
            if (microSeconds > 10)
            {
                // Try to make a chunk big enough for 10 seconds to copy
                chunkSize = chunk->m_dataSize * (uint64_t)(100000000.0 / microSeconds);
            }
            if (chunkSize < 0x1000)
            {
                // Use recommened chunk size
                chunkSize = m_currentChunkSize;
            }
            if (chunkSize >= remainingSize ||
                remainingSize - chunkSize < chunkSize / 2)
            {
                chunkSize = remainingSize;
            }
            if (chunkSize > 0x4000000)
            {
                // Use 64MB max chunk size 
                chunkSize = 0x4000000;
            }
            else
            {
                m_currentChunkSize = (uint32_t)chunkSize;
            }
            newChunkSize = (uint32_t)chunkSize;
            newPosition = m_nextChunkPosition;
            m_nextChunkPosition += newChunkSize;
        }
    }
    else
    {
        // Need a chunk for left over, don't move m_nextChunkPosition
        newPosition = chunk->m_position + chunk->m_dataSize;
        newChunkSize = chunk->m_chunkSize - chunk->m_dataSize;
    }

    if (newPosition > 0)
    {
        auto newChunk = new Chunk(this, newPosition, newChunkSize);
        outs << ", next chunk " << CommaNumber(newPosition) << " size " << CommaNumber(newChunkSize);

        if (isSuccess)
        {
            // Reuse the opened FILE object
            newChunk->AttachFp(chunk->DetachFp());
        }
        m_chunks.insert({ newPosition, newChunk });
        SaveChunkJson(m_dest.data());

        m_chunks.at(newPosition)->Start();
    }
    delete chunk;

    m_outStrings.push_back(outs.str());
}

hstring HStringFromCString(const std::string& str)
{
    std::wstring wstr;
    wstr.reserve(str.size() + 1);
    auto count = MultiByteToWideChar(CP_ACP, 0, str.data(), (uint32_t)str.size() + 1, wstr.data(), (uint32_t)str.size() + 1);
    THROW_LAST_ERROR_IF(count <= 0);
    return wstr.data();
}

std::string CStringFromHString(const hstring hstr)
{
    std::string str;
    str.resize(hstr.size() * sizeof(wchar_t));
    auto count = WideCharToMultiByte(CP_ACP, 0, hstr.data(), hstr.size() + 1, str.data(), hstr.size() + 1, nullptr, false);
    THROW_LAST_ERROR_IF(count <= 0);
    str.resize(count - 1);
    return str;
}

std::string AFile::GetChunkFileName(const char* dest)
{
    const char* fileName;
    auto pos = m_name.rfind('\\');
    fileName = (pos != std::string::npos) ? m_name.data() + pos + 1 : m_name.data();
    std::string chunkFile = std::string(dest) + "\\" + fileName + "._chunks_";
    return chunkFile;
}

void AFile::SaveChunkJson(const char* dest)
{
    JsonObject jsonFile;
    jsonFile.SetNamedValue(L"source", JsonValue::CreateStringValue(HStringFromCString(m_name)));
    jsonFile.SetNamedValue(L"destination", JsonValue::CreateStringValue(HStringFromCString(std::string(dest))));
    jsonFile.SetNamedValue(L"size", JsonValue::CreateStringValue(std::to_wstring(m_size).data()));
    jsonFile.SetNamedValue(L"nextChunkPosition", JsonValue::CreateStringValue(std::to_wstring(m_nextChunkPosition).data()));
    jsonFile.SetNamedValue(L"bytesCopied", JsonValue::CreateStringValue(std::to_wstring(m_bytesCopied).data()));

    JsonArray array;
    for (auto&& pair : m_chunks)
    {
        auto position = pair.first;
        auto chunk = pair.second;
        JsonObject jsonChunk;
        jsonChunk.SetNamedValue(L"position", JsonValue::CreateStringValue(std::to_wstring(position).data()));
        jsonChunk.SetNamedValue(L"chunkSize", JsonValue::CreateStringValue(std::to_wstring(chunk->m_chunkSize).data()));
        jsonChunk.SetNamedValue(L"dataSize", JsonValue::CreateStringValue(std::to_wstring(chunk->m_dataSize).data()));
        array.Append(jsonChunk);
    }
    jsonFile.SetNamedValue(L"chunks", array);


    auto destName = GetChunkFileName(dest);
    wil::unique_file destFile;
    auto err = fopen_s(&destFile, destName.data(), "w");
    THROW_IF_WIN32_ERROR(err);
    auto str = jsonFile.Stringify();

    fprintf(destFile.get(), "%ws", str.data());
}

bool AFile::LoadChunkJson(const char* dest)
{
    auto chunkFileName = GetChunkFileName(dest);
    // Use binary mode, other wise the string read from ifstream is not ended properly.
    std::ifstream inFile(chunkFileName.data(), std::ios::in | std::ios::binary | std::ios::ate);
    if (!inFile.good())
    {
        return false;
    }

    try {
        size_t size = inFile.tellg();
        inFile.seekg(0);
        THROW_HR_IF(HRESULT_FROM_WIN32(ERROR_INVALID_DATA), size > INT16_MAX);
        std::string str;
        str.reserve(size + 1);
        inFile.rdbuf()->sgetn(str.data(), size);
        inFile.close();
        str.data()[size] = '\0';

        wchar_t* buf;
        HSTRING_BUFFER hbuf;
        THROW_IF_FAILED(WindowsPreallocateStringBuffer((uint32_t)size, &buf, &hbuf));

        auto count = MultiByteToWideChar(CP_ACP, 0, str.data(), (uint32_t)size, buf, (uint32_t)size);
        THROW_HR_IF(HRESULT_FROM_WIN32(ERROR_INVALID_DATA), count != size);
        HSTRING hContent;
        THROW_IF_FAILED(WindowsPromoteStringBuffer(hbuf, &hContent));
        hstring content(hContent, take_ownership_from_abi);

        auto jsonFile = JsonObject::Parse(content);
        m_name = CStringFromHString(jsonFile.GetNamedString(L"source"));
        // Ignore destination as we found location of the chunk file.
        wchar_t *tail;
        m_size = std::wcstoull(jsonFile.GetNamedString(L"size").data(), &tail, 10);
        m_bytesCopied = std::wcstoull(jsonFile.GetNamedString(L"bytesCopied").data(), &tail, 10);
        m_nextChunkPosition = std::wcstoull(jsonFile.GetNamedString(L"nextChunkPosition").data(), &tail, 10);

        auto array = jsonFile.GetNamedArray(L"chunks");
        for (auto&& item : array)
        {
            auto type = item.ValueType();
            THROW_HR_IF(HRESULT_FROM_WIN32(ERROR_INVALID_DATA), (type != JsonValueType::Object));
            auto jsonChunk = item.GetObject();
            auto position = std::wcstoull(jsonChunk.GetNamedString(L"position").data(), &tail, 10);
            auto chunkSize = std::wcstoul(jsonChunk.GetNamedString(L"chunkSize").data(), &tail, 10);

            // Ignore dataSize as we don't have the data;
            Chunk* chunk = new Chunk(this, position, chunkSize);
            m_chunks.insert({ position, chunk });
            std::cout << "Chunk info loaded: position " << CommaNumber(position) << " size " << CommaNumber(chunkSize) << std::endl;
        }
    }
    catch (...)
    {
        std::cout << "Failed to load chunk file. Restart from beginning." << std::endl;
        return false;
    }

    // Success, delete the chunk file
    remove(chunkFileName.data());
    return true;
}

void AFile::StartCopying(const char* dest)
{
    auto pos = m_name.rfind('\\');
    const char* fileName;
    fileName = (pos != std::string::npos) ? m_name.data() + pos + 1 : m_name.data();
    std::string destName = std::string(dest) + "\\" + fileName;
    m_dest = dest;

    char* mode = "wb+";
    auto autolock = m_lock.lock_exclusive();
    if (m_chunks.size() == 0)
    {
        // New file
        wil::unique_file destFile;
        auto err = fopen_s(&destFile, destName.data(), "rb+");
        if (err == 0)
        {
            std::cout << "File already exists. Don't overwrite." << std::endl;
            return;
        }

        // create initial chunks
        for (auto i = 0; i < 2; i++)
        {
            auto chunkSize = m_size - m_nextChunkPosition;
            if (chunkSize > m_currentChunkSize)
            {
                chunkSize = m_currentChunkSize;
            }
            auto position = m_nextChunkPosition;
            m_nextChunkPosition += chunkSize;
            auto chunk = new Chunk(this, position, (uint32_t)chunkSize);

            m_chunks.insert({ position, chunk });

            if (m_nextChunkPosition >= m_size)
            {
                break;
            }
        }
    }
    else
    {
        // Chunks are loaded. The file should exist already, don't overwrite.
        mode = "rb+";
    }
    auto err = fopen_s(&m_destFile, destName.data(), mode);
    THROW_HR_IF(E_FAIL, err != 0);

    // start the chunks
    for (auto&& pair : m_chunks)
    {
        auto chunk = pair.second;
        chunk->Start();
    }

    // Release the lock
    autolock.reset();
    uint64_t bytesCopied = 0;
    do {
        auto lock = m_lock.lock_exclusive();
        while (m_outStrings.size() > 0)
        {
            std::cout << m_outStrings.at(0) << std::endl;
            m_outStrings.erase(m_outStrings.begin());
        }
        bytesCopied = m_bytesCopied;
    } while (bytesCopied < m_size);

    // Delete chunk file
    remove(GetChunkFileName(m_dest.data()).data());
}

AFile* TryOpenFile(const char* name, const char* dest)
{
    AFile* aFile = new AFile;
    auto cleanup = wil::scope_exit([&]() { delete aFile; });

    aFile->m_name = name;
    // This could get the original m_name from chunk json file
    aFile->LoadChunkJson(dest);

    auto err = fopen_s(&(aFile->m_hfile), aFile->m_name.data(), "rb");
    THROW_IF_WIN32_ERROR(err);

    struct __stat64 st;
    err = _fstati64(_fileno(aFile->m_hfile.get()), &st);
    THROW_HR_IF(E_FAIL, err != 0);

    aFile->m_size = st.st_size;
    cleanup.release();
    return aFile;
}

void CreateDest(const char* name)
{
    auto attrib = GetFileAttributesA(name);
    if (attrib != INVALID_FILE_ATTRIBUTES)
    {
        if (!(attrib & FILE_ATTRIBUTE_DIRECTORY))
        {
            std::cout << "Error: " << name << " is not a directory to save files.\n";
            THROW_HR(E_FAIL);
        }
    }
    else
    {
        std::filesystem::create_directory(name);
    }
}

static std::function<void(int)> s_signalHandler;

void SignalHandler(int signum)
{
    s_signalHandler(signum);
}

void CopyAFile(const char* source, const char* dest)
{
    auto aFile = TryOpenFile(source, dest);
    auto cleanup = wil::scope_exit([&]() { delete aFile; });

    s_signalHandler = [aFile, dest](int signum)
    {
        auto lock = aFile->m_lock.lock_shared();

        aFile->SaveChunkJson(dest);
        delete aFile;
        std::cout << "Aborted, run this command again to resume copying." << std::endl;
        exit(signum);
    };
    signal(SIGINT, SignalHandler);

    std::cout << aFile->m_name << " -> " << dest << std::endl;
    aFile->StartCopying(dest);
}

void CopyDirectory(const std::string& source, const char* subDir, std::string dest)
{
    dest = dest + "\\" + subDir;
    if (!std::filesystem::exists(dest))
    {
        std::filesystem::create_directory(dest);
    }
    else
    {
        if (!std::filesystem::is_directory(dest))
        {
            std::cout << dest << " already exists but is not a directory." << std::endl;
            exit(1);
        }
    }
    for (auto&& entry : std::filesystem::directory_iterator(source))
    {
        if (std::filesystem::is_regular_file(entry))
        {
            CopyAFile(entry.path().string().data(), dest.data());
        }
        else if (std::filesystem::is_directory(entry))
        {
            std::cout << entry.path() << std::endl;
            auto path = entry.path();
            CopyDirectory(path.string(), path.filename().string().data(), dest);
        }
    }
}

void main(uint32_t argc, char *argv[])
{
    if (argc < 2)
    {
        std::cout << "Usage: SlowCopy <src> [destination|.]\n";
        exit(1);
    }
    const char* source = argv[1];
    const char* dest = (argc > 2) ? argv[2] : ".";
    CreateDest(dest);

    if (std::filesystem::is_regular_file(source))
    {
        CopyAFile(source, dest);
    }
    else if (std::filesystem::is_directory(source))
    {
        std::string path(source);
        auto pos = path.rfind('\\');
        if (pos == path.size() - 1)
        {
            path.erase(pos, 1);
        }
        pos = path.rfind('\\');
        const char* pathFull;
        const char* subDir;
        if (pos != std::string::npos)
        {
            pathFull = path.data();
            subDir = pathFull + pos + 1;
        }
        else
        {
            pathFull = subDir = path.data();
        }
        CopyDirectory(pathFull, subDir, dest);
    }
}
