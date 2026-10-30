// Сервіс — це наш control plane. LiteLLM тільки ходить у модель, а рішення
// (яку модель брати, коли ретраїти, скільки коштує) робимо тут.
// MODEL=mock — дефолт, грошей не треба. MODEL=gpt-5-mini або claude-haiku + ключ у gateway/.env — реальна модель.

using System.Text;
using System.Text.Json;
using Npgsql;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using StackExchange.Redis;
using System.Text.RegularExpressions;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpClient();
var app = builder.Build();

// налаштування беремо з оточення (задаються в docker-compose.yml)
var gateway = Environment.GetEnvironmentVariable("GATEWAY_URL") ?? "http://gateway:4000";
var dbConn = Environment.GetEnvironmentVariable("DB_CONN")
    ?? "Host=postgres;Database=llmops;Username=llmops;Password=llmops";
var defaultModel = Environment.GetEnvironmentVariable("MODEL") ?? "mock";
var embeddingModel = Environment.GetEnvironmentVariable("EMBEDDING_MODEL") ?? "text-embedding-3-small";

// cost (W2): ціна в USD за 1k токенів. (in, out) — output дорожчий за input,
// strong помітно дорожча за mini. null-тариф → cost_usd лишиться null.
var prices = new Dictionary<string, (decimal In, decimal Out)>
{
    ["mock-mini"]   = (0.00015m, 0.00060m),
    ["mock-strong"] = (0.00300m, 0.01500m),
};

var cache = new ConcurrentDictionary<string, CacheEntry>();
var redisConn = Environment.GetEnvironmentVariable("REDIS_CONN") ?? "redis:6379";
IDatabase? redisDb = null;
IServer? redisServer = null;
try
{
    var redis = ConnectionMultiplexer.Connect($"{redisConn},abortConnect=false,connectTimeout=2000");
    if (redis.IsConnected)
    {
        redisDb = redis.GetDatabase();
        redisServer = redis.GetServer(redisConn);
        Console.WriteLine($"[redis] ping = {redisDb.Ping().TotalMilliseconds}ms");
    }
    else
    {
        Console.WriteLine("[redis] not connected, semantic cache off");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"[redis] unavailable, semantic cache off: {ex.GetType().Name}");
}
var cacheTtl = TimeSpan.FromMinutes(10);
var cacheHits = 0;
var cacheMisses = 0;
var fallbacks = 0;
var providerFails = new ConcurrentDictionary<string, int>();
// таксономія помилок (W5 опційно): окремі лічильники за причиною збою.
// На mock реально зростають provider_429/provider_503/db_error;
// tool_timeout і guardrail_refusal на mock не виникають (exec миттєвий,
// refusal дає провайдер) — лишаються 0, чесно позначено в PR.
var errProvider429 = 0;
var errProvider503 = 0;
var errDbError = 0;
const int degradeAfter = 3;

var budgetUsd = 5.00m;

var semanticThreshold = double.TryParse(
    Environment.GetEnvironmentVariable("SEMANTIC_THRESHOLD"),
    System.Globalization.NumberStyles.Float,
    System.Globalization.CultureInfo.InvariantCulture,
    out var t) ? t : 0.85;

// tools (W3): реєстр інструментів як дані. category: read = оборотне (авто),
// irreversible = незворотне (не виконуємо, ескалюємо на людину).
var tools_registry = new Dictionary<string, ToolSpec>
{
    ["lookup_order"]  = new ToolSpec("read", 2000),
    ["create_ticket"] = new ToolSpec("irreversible", 2000),
};

// [W4] черга підтверджень (HITL): id -> (дія, результат).
// result == null => заявка ще очікує рішення оператора.
var approvals = new ConcurrentDictionary<string, Approval>();

app.MapPost("/chat", async (ChatIn body, IHttpClientFactory httpFactory) =>
{
    var requestId = Guid.NewGuid();
    var startedAt = DateTimeOffset.UtcNow;

    // guardrails (W4): маскуємо PII до моделі, кешу й логу
    var userMessage = MaskPii(body.Message);

    var degraded = false;
    try
    {
        var spentToday = await GetTodayCost(dbConn);
        degraded = spentToday >= 0.8m * budgetUsd;
    }
    catch { /* БД недоступна — без деградації, не валимо запит */ }
    var model = Route(userMessage, defaultModel, degraded);

    // промпт (W1): береться з реєстру (таблиця prompts) з версією
    string systemPrompt;
    string promptVersion;
    try
    {
        (systemPrompt, promptVersion) = await GetActivePrompt(dbConn);
    }
    catch
    {
        Interlocked.Increment(ref errDbError);
        Console.Error.WriteLine($"[{requestId}] DB unavailable, cannot load prompt");
        return Results.Json(new { error = "Service unavailable" }, statusCode: 503);
    }

    var cacheKey = CacheKey(model, systemPrompt, userMessage);

    if (cache.TryGetValue(cacheKey, out var cached)
        && DateTimeOffset.UtcNow - cached.StoredAt < cacheTtl)
    {
        Interlocked.Increment(ref cacheHits);
        var hitMs = (int)(DateTimeOffset.UtcNow - startedAt).TotalMilliseconds;
        Console.WriteLine($"[cache] HIT  {cacheKey[..8]} hits={cacheHits} miss={cacheMisses}");
        await LogRequest(dbConn, requestId, model, promptVersion, hitMs, 0, 0, 0m, 200);
        return Results.Json(new { request_id = requestId, content = cached.Answer, tool = (string?)null, latency_ms = hitMs, cache = "hit" });
    }
    Interlocked.Increment(ref cacheMisses);
    Console.WriteLine($"[cache] MISS {cacheKey[..8]} hits={cacheHits} miss={cacheMisses}");

        // --- семантичний кеш (другий ешелон після точного; лише якщо Redis доступний) ---
    float[]? queryVector = null;
    if (redisDb != null && redisServer != null)
    {
        var httpEmb = httpFactory.CreateClient();
        queryVector = await GetEmbedding(userMessage, gateway, embeddingModel, httpEmb);

        var (semAnswer, semScore) = await SearchSemanticCache(redisDb, redisServer, model, queryVector, cacheTtl);

        if (semAnswer is not null && semScore >= semanticThreshold)
        {
            Interlocked.Increment(ref cacheHits);
            var semMs = (int)(DateTimeOffset.UtcNow - startedAt).TotalMilliseconds;
            Console.WriteLine($"[semantic] HIT score={semScore:F4} >= {semanticThreshold}");
            await LogRequest(dbConn, requestId, model, promptVersion, semMs, 0, 0, 0m, 200);
            return Results.Json(new { request_id = requestId, content = semAnswer, tool = (string?)null, latency_ms = semMs, cache = "semantic" });
        }
        Console.WriteLine($"[semantic] MISS best_score={semScore:F4} < {semanticThreshold}");
    }

    // fallback (W4): пробуємо моделі по черзі. На mock другий елемент — той самий
    // процес (справжнього fallback на незалежну модель лаба не відтворює), але
    // перебір кандидатів і обробку повної відмови це перевіряє. З реальним ключем
    // другим стане інша модель, і порядок із Теми 3 вставиться сюди без змін.
    var chain = defaultModel == "mock"
        ? new[] { model, "mock" }
        : new[] { model, "azure-gpt-5" };

    var http = httpFactory.CreateClient();
    var answer = "";
    string? toolCall = null;
    int promptTokens = 0, completionTokens = 0, status = 0;
    var ok = false;

    for (int i = 0; i < chain.Length && !ok; i++)
    {
        if (i > 0) Interlocked.Increment(ref fallbacks);

        var res = await CallGateway(http, gateway, chain[i], systemPrompt, userMessage, tools_registry);

             if (res.ok)
        {
            ok = true;
            model = chain[i];
            answer = res.answer;
            toolCall = res.tool;
            promptTokens = res.pt;
            completionTokens = res.ct;
            status = res.status;
            providerFails[chain[i]] = 0;                         // успіх → лічильник збоїв нуль
        }
        else
        {
            status = res.status;
            providerFails.AddOrUpdate(chain[i], 1, (_, n) => n + 1);  // збій → +1 поспіль
            if (res.status == 429) Interlocked.Increment(ref errProvider429);
            else if (res.status == 503 || res.status == 0) Interlocked.Increment(ref errProvider503);
        }
     }       

    // контрольована відмова (W4): весь ланцюг упав. Ввічлива заглушка користувачу,
    // чесний 503 у лог. status==0 (мережі не було) теж стає 503 — у лозі не має бути 0.
    if (!ok)
    {
        answer = "Вибачте, тимчасові проблеми на нашому боці. Спробуйте, будь ласка, трохи згодом.";
        status = (status == 200 || status == 0) ? 503 : status;
    }

    // tools + HITL (W4): read-only виконуємо одразу; незворотну дію — через заявку.
    // Навіть якщо injection змусив модель запропонувати незворотну дію — вона
    // не виконається без підтвердження людини (обмеження радіуса ураження).
    if (ok && toolCall is not null)
    {
        tools_registry.TryGetValue(toolCall, out var spec);

        if (spec is not null && spec.Category == "irreversible")
        {
            var id = Guid.NewGuid().ToString("N")[..6];
            approvals[id] = new Approval(toolCall, "pending", null);
            answer += $" (очікує підтвердження оператора, id={id})";
        }
        else
        {
            var toolResult = await RunTool(toolCall, tools_registry);
            answer += $" ({toolResult})";
        }
    }

    var latencyMs = (int)(DateTimeOffset.UtcNow - startedAt).TotalMilliseconds;

    // cost (W2): порахувати tokens * ціна і покласти в cost_usd
    decimal? costUsd = null;  // null, якщо для моделі немає тарифу
    if (status is >= 200 and < 300 && prices.TryGetValue(model, out var price))
    {
        costUsd = promptTokens / 1000m * price.In
                + completionTokens / 1000m * price.Out;
    }

    // cache (W3): зберігаємо лише успішні відповіді без tool-виклику.
    // персональний стан / результат інструмента не кешуємо (межа — п.4).
    if (status is >= 200 and < 300 && toolCall is null)
    {
        // ключ рахуємо з моделі, що РЕАЛЬНО відповіла (model уже = chain[i] після fallback),
        // а не з початкового вибору роутера — інакше відповідь резервної моделі
        // осіла б під ключем основної і віддавалась би як її, коли основна оживе.
        var writeKey = CacheKey(model, systemPrompt, userMessage);
        cache[writeKey] = new CacheEntry(answer, DateTimeOffset.UtcNow);
        if (redisDb != null && queryVector != null)
            await SaveToSemanticCache(redisDb, model, queryVector, answer);
    }

    // лог кожного запиту — з цього живе observability (W1) і cost (W2)
    await LogRequest(dbConn, requestId, model, promptVersion, latencyMs, promptTokens, completionTokens, costUsd, status);

    return Results.Json(new { request_id = requestId, content = answer, tool = toolCall, latency_ms = latencyMs });
});

// ці ендпоінти читає готова консоль. поверни потрібну форму — картки оживуть.
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));
app.MapGet("/observability", async () =>
{
    int requests = 0, p95 = 0;
    double errorRate = 0;
    try
    {
        await using var db = new NpgsqlConnection(dbConn);
        await db.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT count(*), "
            + "COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms), 0), "
            + "COALESCE(AVG(CASE WHEN status <> '200' THEN 1.0 ELSE 0 END) * 100, 0) "
            + "FROM requests WHERE created_at::date = CURRENT_DATE", db);
        await using var reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            requests = reader.GetInt32(0);
            p95 = (int)reader.GetDouble(1);
            errorRate = reader.GetDouble(2);
        }
    }
    catch { /* БД недоступна — віддаємо нулі, не валимо ендпоінт */ }

    var totalCacheLookups = cacheHits + cacheMisses;
    var cacheHitPct = totalCacheLookups == 0 ? 0 : Math.Round(cacheHits * 100.0 / totalCacheLookups, 1);

    return Results.Json(new
    {
        p95_ms = p95,
        requests,
        cache_hit_pct = cacheHitPct,
        error_rate_pct = Math.Round(errorRate, 1),
        fallback_events = fallbacks,
        errors = new
        {
            provider_429 = errProvider429,
            provider_503 = errProvider503,
            db_error = errDbError,
            tool_timeout = 0,        // на mock не виникає (exec миттєвий)
            guardrail_refusal = 0,   // refusal дає провайдер, не власний детект
        },
    });
});
app.MapGet("/cost", async () =>
{
    var todayUsd = await GetTodayCost(dbConn);
    return Results.Json(new { today_usd = todayUsd, budget_usd = budgetUsd });
});

app.MapGet("/prompts", async () =>
{
    var result = new List<object>();
    await using var db = new NpgsqlConnection(dbConn);
    await db.OpenAsync();
    await using var cmd = new NpgsqlCommand(
        "SELECT name, version, active FROM prompts WHERE name = 'support-system' ORDER BY version", db);
    await using var reader = await cmd.ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        result.Add(new {
            name = reader.GetString(0),
            version = reader.GetString(1),
            active = reader.GetBoolean(2)
        });
    }
    return Results.Json(result);
});

app.MapGet("/providers", () =>
{
    var models = defaultModel == "mock"
        ? new[] { "mock-mini", "mock-strong", "mock" }
        : new[] { defaultModel, "azure-gpt-5" };

    var providers = models.Select(m => new
    {
        name = m,
        status = providerFails.GetValueOrDefault(m, 0) >= degradeAfter ? "degraded" : "ok"
    });

    return Results.Json(new { providers });
});
// [W4] HITL: оператор бачить заявки, що очікують рішення (result == null).
// Форма { pending: [{ id, action }] } — саме її читає картка Approvals у консолі.
app.MapGet("/approvals", () =>
{
    var pending = approvals
        .Where(kv => kv.Value.Result == null)
        .Select(kv => new { id = kv.Key, action = kv.Value.Action })
        .ToList();
    return Results.Json(new { pending });
});

app.MapPost("/prompts/{version}/activate", async (string version, HttpRequest request) =>
{
    await using var db = new NpgsqlConnection(dbConn);
    await db.OpenAsync();
    await using var tx = await db.BeginTransactionAsync();

    await using var cmd = new NpgsqlCommand(
        "UPDATE prompts SET active = (version = @v) "
        + "WHERE name = 'support-system' "
        + "AND EXISTS (SELECT 1 FROM prompts WHERE name = 'support-system' AND version = @v)", db, tx);
    cmd.Parameters.AddWithValue("v", version);
    var rowsAffected = await cmd.ExecuteNonQueryAsync();

    if (rowsAffected == 0)
    {
        await tx.RollbackAsync();
        return Results.NotFound(new { error = $"version {version} not found" });
    }

    var actor = request.Headers["X-Actor"].FirstOrDefault() ?? "unknown";
    await using var logCmd = new NpgsqlCommand(
        "INSERT INTO prompt_activations (name, version, actor) "
        + "VALUES ('support-system', @v, @actor)", db, tx);
    logCmd.Parameters.AddWithValue("v", version);
    logCmd.Parameters.AddWithValue("actor", actor);
    await logCmd.ExecuteNonQueryAsync();

    await tx.CommitAsync();
    return Results.Ok(new { activated = version });
});

// [W4] HITL: підтвердження заявки оператором -> виконати інструмент.
// Атомарний захват (TryUpdate pending->in-progress) проти check-then-act:
// одночасні/повторні approve не створять другий тікет.
app.MapPost("/approvals/{id}/approve", async (string id) =>
{
    if (!approvals.TryGetValue(id, out var a) || a.Result != null)
        return Results.NotFound(new { error = "already done" });

    // атомарно забираємо заявку з pending — виконає лише той, кому це вдалось
    var inProgress = a with { Status = "in-progress" };
    if (!approvals.TryUpdate(id, inProgress, a))
        return Results.NotFound(new { error = "already done" });

    var result = await RunTool(a.Action, tools_registry);
    approvals[id] = inProgress with { Status = "approved", Result = result };
    return Results.Ok(new { id, result });
});

app.Run("http://0.0.0.0:8080");

// політика на 80% бюджету ($4.00): деградація — роутер примусово віддає
// mock-mini замість mock-strong, щоб сервіс лишався живим, не перевищивши ліміт.
// fallback chain: mock-strong -> mock-mini -> контрольована помилка

// [W4] один виклик моделі через gateway. Збій повертаємо як значення (ok=false),
// а не кидаємо виняток — щоб fallback-цикл міг обробити його і піти далі.
static async Task<(bool ok, string answer, string? tool, int pt, int ct, int status)>
    CallGateway(HttpClient http, string gateway, string model, string system, string user,
        Dictionary<string, ToolSpec> registry)
{
        var payload = JsonSerializer.Serialize(new
    {
        model,
        messages = new object[]
        {
            new { role = "system", content = system },
            new { role = "user", content = user }
        }
    });

    var answer = "";
    string? toolCall = null;
    int promptTokens = 0, completionTokens = 0, status = 0;

    try
    {
        var response = await http.PostAsync(
            $"{gateway}/v1/chat/completions",
            new StringContent(payload, Encoding.UTF8, "application/json"));
        status = (int)response.StatusCode;

        if (status >= 400)
            return (false, "", null, 0, 0, status);

        var rawJson = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(rawJson);
        var message = doc.RootElement.GetProperty("choices")[0].GetProperty("message");
        answer = message.GetProperty("content").GetString() ?? "";

        if (message.TryGetProperty("tool_calls", out var tools)
            && tools.ValueKind == JsonValueKind.Array && tools.GetArrayLength() > 0)
        {
            toolCall = tools[0].GetProperty("function").GetProperty("name").GetString();
        }

        var usage = doc.RootElement.GetProperty("usage");
        promptTokens = usage.GetProperty("prompt_tokens").GetInt32();
        completionTokens = usage.GetProperty("completion_tokens").GetInt32();
    }
    catch
    {
        return (false, "", null, 0, 0, 0);
    }

    return (true, answer, toolCall, promptTokens, completionTokens, status);
}

static async Task<string> RunTool(string name, Dictionary<string, ToolSpec> registry)
{
    if (!registry.TryGetValue(name, out var spec))
        return $"unknown tool: {name}";


    // саме виконання інструмента (на W4 — реальний виклик у зовнішню систему)
    async Task<string> Execute() => name switch
    {
        "lookup_order" => "статус: оплачено, доставку призначено",
        "create_ticket" => $"тікет #T-{Guid.NewGuid().ToString("N")[..4]}",
        _ => $"no handler: {name}"
    };

    // таймаут: не чекаємо довше за TimeoutMs — інакше контрольована помилка,
    // а не зависання всього запиту
    try
    {
        return await Execute().WaitAsync(TimeSpan.FromMilliseconds(spec.TimeoutMs));
    }
    catch (TimeoutException)
    {
        return $"tool timeout after {spec.TimeoutMs}ms: {name}";
    }
}

// [W4] guardrail: маскуємо email ДО моделі, кешу й логу — PII не має
// витікати провайдеру чи в сховища. Regex — навчальне спрощення.
static string MaskPii(string s) =>
    Regex.Replace(s, @"[\w.\-]+@[\w.\-]+", "[email]");

static string CacheKey(string model, string prompt, string message)
{
    var raw = $"{model}\n{prompt}\n{message.Trim()}";
    var hash = SHA256.HashData(Encoding.UTF8.GetBytes(raw));
    return Convert.ToHexString(hash);
}

static async Task<float[]> GetEmbedding(string text, string gateway, string model, HttpClient http)
{
    var payload = JsonSerializer.Serialize(new
    {
        model,
        input = text
    });

    var response = await http.PostAsync(
        $"{gateway}/v1/embeddings",
        new StringContent(payload, Encoding.UTF8, "application/json"));

    var rawJson = await response.Content.ReadAsStringAsync();
    using var doc = JsonDocument.Parse(rawJson);

    var arr = doc.RootElement.GetProperty("data")[0].GetProperty("embedding");
    var vector = new float[arr.GetArrayLength()];
    var i = 0;
    foreach (var x in arr.EnumerateArray())
        vector[i++] = x.GetSingle();
    return vector;
}

static double Cosine(float[] a, float[] b)
{
    double dot = 0, normA = 0, normB = 0;
    for (var i = 0; i < a.Length; i++)
    {
        dot   += a[i] * b[i];
        normA += a[i] * a[i];
        normB += b[i] * b[i];
    }
    return dot / (Math.Sqrt(normA) * Math.Sqrt(normB));
}

static async Task<(string? answer, double score)> SearchSemanticCache(
    IDatabase db, IServer server, string model,  float[] queryVector, TimeSpan ttl)
{
    string? bestAnswer = null;
    double bestScore = -1;

    foreach (var key in server.Keys(pattern: "semcache:*"))
    {
        var raw = await db.StringGetAsync(key);
        if (raw.IsNullOrEmpty) continue;

        using var doc = JsonDocument.Parse(raw.ToString());
        var root = doc.RootElement;

        var storedAt = root.GetProperty("storedAt").GetDateTimeOffset();
        if (DateTimeOffset.UtcNow - storedAt > ttl) continue;

        var entryModel = root.GetProperty("model").GetString();
        if (entryModel != model) continue;

        var arr = root.GetProperty("vector");
        var vector = new float[arr.GetArrayLength()];
        var i = 0;
        foreach (var x in arr.EnumerateArray())
            vector[i++] = x.GetSingle();

        var score = Cosine(queryVector, vector);
        if (score > bestScore)
        {
            bestScore = score;
            bestAnswer = root.GetProperty("answer").GetString();
        }
    }

    return (bestAnswer, bestScore);
}

static async Task SaveToSemanticCache(IDatabase db, string model, float[] vector, string answer)
{
    var entry = JsonSerializer.Serialize(new
    {
        model,
        vector,
        answer,
        storedAt = DateTimeOffset.UtcNow
    });
    var key = $"semcache:{Guid.NewGuid()}";
    await db.StringSetAsync(key, entry);
}

static string Route(string message, string defaultModel, bool degraded)
{
    if (defaultModel != "mock")
        return defaultModel;

    // політика: на 80%+ бюджету не підіймаємось до strong — деградація
    if (degraded)
        return "mock-mini";

    string[] markers = { "поверн", "терміново", "скарг", "refund" };

    if (string.IsNullOrWhiteSpace(message))
        return "mock-mini";

    var normalized = message.ToLowerInvariant();

    foreach (var marker in markers)
    {
        if (normalized.Contains(marker))
            return "mock-strong";
    }

    return "mock-mini";
}

static async Task<(string body, string version)> GetActivePrompt(string conn)
{
    await using var db = new NpgsqlConnection(conn);
    await db.OpenAsync();
    await using var cmd = new NpgsqlCommand(
        "SELECT body, version FROM prompts WHERE active = true LIMIT 1", db);

    await using var reader = await cmd.ExecuteReaderAsync();
    if (await reader.ReadAsync())
    {
        var body = reader.GetString(0);
        var version = reader.GetString(1);
        return (body, version);
    }

    return ("You are an assistant.", "none");
}

static async Task<decimal> GetTodayCost(string conn)
{
    await using var db = new NpgsqlConnection(conn);
    await db.OpenAsync();
    await using var cmd = new NpgsqlCommand(
        "SELECT COALESCE(SUM(cost_usd), 0) FROM requests WHERE created_at::date = CURRENT_DATE", db);
    var result = await cmd.ExecuteScalarAsync();
    return result != null && result != DBNull.Value ? Convert.ToDecimal(result) : 0m;
}

static async Task LogRequest(string conn, Guid id, string model, string promptVersion, int latency,
    int promptTokens, int completionTokens, decimal? cost, int status)
{
    try
    {
        await using var db = new NpgsqlConnection(conn);
        await db.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "INSERT INTO requests (request_id, model, prompt_version, latency_ms, prompt_tokens, completion_tokens, cost_usd, status) "
            + "VALUES (@id, @model, @pv, @lat, @pt, @ct, @cost, @status)", db);
        cmd.Parameters.AddWithValue("id", id);
        cmd.Parameters.AddWithValue("model", model);
        cmd.Parameters.AddWithValue("pv", promptVersion);
        cmd.Parameters.AddWithValue("lat", latency);
        cmd.Parameters.AddWithValue("pt", promptTokens);
        cmd.Parameters.AddWithValue("ct", completionTokens);
        cmd.Parameters.AddWithValue("cost", (object?)cost ?? DBNull.Value);
        cmd.Parameters.AddWithValue("status", status.ToString());
        await cmd.ExecuteNonQueryAsync();
    }
    catch { /* не валимо запит через лог */ }
}

record ChatIn(string Message);
record CacheEntry(string Answer, DateTimeOffset StoredAt);
record ToolSpec(string Category, int TimeoutMs);
record Approval(string Action, string Status, string? Result);