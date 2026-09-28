// Сервіс — це наш control plane. LiteLLM тільки ходить у модель, а рішення
// (яку модель брати, коли ретраїти, скільки коштує) робимо тут.
// MODEL=mock — дефолт, грошей не треба. MODEL=gpt-5-mini або claude-haiku + ключ у gateway/.env — реальна модель.

using System.Text;
using System.Text.Json;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpClient();
var app = builder.Build();

// налаштування беремо з оточення (задаються в docker-compose.yml)
var gateway = Environment.GetEnvironmentVariable("GATEWAY_URL") ?? "http://gateway:4000";
var dbConn = Environment.GetEnvironmentVariable("DB_CONN")
    ?? "Host=postgres;Database=llmops;Username=llmops;Password=llmops";
var defaultModel = Environment.GetEnvironmentVariable("MODEL") ?? "mock";

app.MapPost("/chat", async (ChatIn body, IHttpClientFactory httpFactory) =>
{
    var requestId = Guid.NewGuid();
    var startedAt = DateTimeOffset.UtcNow;

    // guardrails (W4): тут перевірити вхід на PII / інʼєкції. поки нічого.
    // TODO(student, W4)

    // routing (W2): поки одна модель, а треба обирати за задачею
    var model = defaultModel;  // TODO(student, W2)

    // промпт (W1): захардкодив — має братися з реєстру (таблиця prompts) з версією
    string systemPrompt;

    string promptVersion;

    try
{
    (systemPrompt, promptVersion) = await GetActivePrompt(dbConn);
}
catch
{
    Console.Error.WriteLine($"[{requestId}] DB unavailable, cannot load prompt");
    return Results.Json(new { error = "Service unavailable" }, statusCode: 503);
}

    // cache (W3): перед викликом глянути в Redis — раптом вже відповідали
    // TODO(student, W3)

    // fallback (W4): якщо тут 429/5xx — піти на іншого провайдера. поки один виклик.
    // TODO(student, W4)
    var payload = JsonSerializer.Serialize(new
    {
        model,
        messages = new object[]
        {
            new { role = "system", content = systemPrompt },
            new { role = "user", content = body.Message }
        }
    });

    var http = httpFactory.CreateClient();
    var answer = "";
    string? toolCall = null;
    int promptTokens = 0, completionTokens = 0, status = 0; // 0 = відповіді не було
    try
    {
        var response = await http.PostAsync(
            $"{gateway}/v1/chat/completions",
            new StringContent(payload, Encoding.UTF8, "application/json"));
        status = (int)response.StatusCode;
        var rawJson = await response.Content.ReadAsStringAsync();

        using var doc = JsonDocument.Parse(rawJson);
        var message = doc.RootElement.GetProperty("choices")[0].GetProperty("message");
        answer = message.GetProperty("content").GetString() ?? "";

        // tools + HITL (W3/W4): якщо модель попросила інструмент — виконати;
        // перед незворотною дією (створити тікет) спитати людину. поки лише читаємо назву.
        // TODO(student, W3/W4)
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
        // мережа/gateway недоступні або відповідь не розпарсилась (status лишиться 0/5xx).
        // TODO(student, W4): тут краще graceful degradation
        answer = "Сервіс тимчасово недоступний.";
    }

    var latencyMs = (int)(DateTimeOffset.UtcNow - startedAt).TotalMilliseconds;

    // cost (W2): порахувати tokens * ціна і покласти в cost_usd
    decimal? costUsd = null;  // TODO(student, W2)

    // лог кожного запиту — з цього живе observability (W1) і cost (W2)
    await LogRequest(dbConn, requestId, model, promptVersion, latencyMs, promptTokens, completionTokens, costUsd, status);

    return Results.Json(new { request_id = requestId, content = answer, tool = toolCall, latency_ms = latencyMs });
});

// ці ендпоінти читає готова консоль. поверни потрібну форму — картки оживуть.
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));                                    // ліфнес, не для консолі
app.MapGet("/observability", () => Results.Json(new { todo = "aggregate from requests table" }));  // W5: { p95_ms, requests, cache_hit_pct, error_rate_pct, fallback_events }
app.MapGet("/cost", () => Results.Json(new { todo = "sum cost_usd for today + budget" }));         // W2/W5: { today_usd, budget_usd }
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
app.MapPost("/prompts/{version}/activate", async (string version) =>
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

    await using var logCmd = new NpgsqlCommand(
        "INSERT INTO prompt_activations (name, version) "
        + "VALUES ('support-system', @v)", db, tx);
    logCmd.Parameters.AddWithValue("v", version);
    await logCmd.ExecuteNonQueryAsync();

    await tx.CommitAsync();
    return Results.Ok(new { activated = version });
});

app.Run("http://0.0.0.0:8080");

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
