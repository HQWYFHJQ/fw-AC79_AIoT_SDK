/*
 * Feedcoop Agent 流式对话 —— HTTPS / SSE 协议层
 *
 * 本文件只负责:
 *   1. 用 libcurl 发起 HTTPS POST（Bearer 鉴权 + ServiceName 头）
 *   2. 流式接收 SSE，按行解析 "data:" 帧
 *   3. 从 choices[0].delta.content 提取增量文本并回调
 *
 * 具体业务流程、事件队列都在 main.c。
 */

#include "chat_completion.h"

#include <stdlib.h>
#include <string.h>

#include <curl/curl.h>

#include "cJSON.h"
#include "system/includes.h"

#define CHAT_AUTH_HEADER_SIZE           256

/* SSE 解析上下文。
 * 累计文本直接写入调用方的 chat_result_t，避免再复制一份 4KB；
 * 结构约 2.1KB，仍然 heap 分配，不放在线程栈上。
 */
typedef struct {
    char line[CHAT_SSE_LINE_SIZE];      /* 当前未完成的行 */
    int line_len;
    int is_line_overflow;
    int is_done;                        /* 收到 [DONE] 或 finish_reason */
    int is_error;                       /* 服务端/协议错误 */
    int is_body_logged;                 /* 非 SSE 响应体只打印一次 */
    int code;                           /* 服务端错误码 */
    chat_result_t *out;                 /* 累计文本写在这里 */
    chat_stream_cb_t cb;
    void *priv;
} chat_sse_ctx_t;

/* 跨线程停止标志: chat_agent_stop() 写，libcurl 进度回调读 */
static volatile u8 s_chat_stop_req;
static u8 s_chat_curl_inited;

/* ------------------------- 基础工具 ------------------------- */

void chat_completion_global_init(void)
{
    if (s_chat_curl_inited) {
        return;
    }
    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
        printf("[CHAT] curl_global_init failed\n");
        return;
    }
    s_chat_curl_inited = 1;
}

void chat_completion_stop(void)
{
    s_chat_stop_req = 1;
}

static void chat_sse_append_text(chat_sse_ctx_t *ctx, const char *text)
{
    char *dst = ctx->out->text;
    int used = (int)strlen(dst);
    int avail = (int)sizeof(ctx->out->text) - 1 - used;
    int len = (int)strlen(text);

    if (avail <= 0) {
        return;
    }
    if (len > avail) {
        len = avail;
    }
    memcpy(dst + used, text, len);
    dst[used + len] = '\0';
}

/* 追加文本并通知业务层 */
static void chat_sse_emit(chat_sse_ctx_t *ctx, const char *delta)
{
    chat_sse_append_text(ctx, delta);
    if (ctx->cb) {
        ctx->cb(delta, ctx->out->text, ctx->code, ctx->is_done, ctx->priv);
    }
}

/* ------------------------- SSE 解析 ------------------------- */

static void chat_sse_parse_error(chat_sse_ctx_t *ctx, cJSON *root)
{
    cJSON *code = cJSON_GetObjectItem(root, "code");
    cJSON *error = cJSON_GetObjectItem(root, "error");
    cJSON *message = cJSON_GetObjectItem(root, "message");
    cJSON *err_code = NULL;
    cJSON *err_msg = NULL;

    if (cJSON_IsNumber(code) && code->valueint != 0) {
        ctx->code = code->valueint;
        ctx->is_error = 1;
    }
    if (cJSON_IsString(error) && (!error->valuestring || !error->valuestring[0])) {
        error = NULL;   /* 空串表示无错误 */
    }
    if (!cJSON_IsObject(error) && !cJSON_IsString(error)) {
        return;
    }
    ctx->is_error = 1;

    if (cJSON_IsObject(error)) {
        err_code = cJSON_GetObjectItem(error, "code");
        err_msg = cJSON_GetObjectItem(error, "message");
        if (cJSON_IsNumber(err_code) && ctx->code == 0) {
            ctx->code = err_code->valueint;
        }
    } else {
        err_msg = error;
    }

    if (cJSON_IsString(err_msg) && err_msg->valuestring) {
        printf("[CHAT] server error: %s\n", err_msg->valuestring);
    } else if (cJSON_IsString(message) && message->valuestring) {
        printf("[CHAT] server error: %s\n", message->valuestring);
    }
}

static void chat_sse_parse_choice(chat_sse_ctx_t *ctx, cJSON *root)
{
    cJSON *choices = cJSON_GetObjectItem(root, "choices");
    cJSON *choice = cJSON_IsArray(choices) ? cJSON_GetArrayItem(choices, 0) : NULL;
    cJSON *delta = NULL;
    cJSON *message = NULL;
    cJSON *content = NULL;
    cJSON *finish = NULL;
    int has_finish = 0;

    if (!cJSON_IsObject(choice)) {
        return;
    }

    delta = cJSON_GetObjectItem(choice, "delta");
    if (cJSON_IsObject(delta)) {
        content = cJSON_GetObjectItem(delta, "content");
    }
    if (!cJSON_IsString(content)) {
        message = cJSON_GetObjectItem(choice, "message");
        content = cJSON_IsObject(message) ? cJSON_GetObjectItem(message, "content") : NULL;
    }

    finish = cJSON_GetObjectItem(choice, "finish_reason");
    if (cJSON_IsString(finish) && finish->valuestring && finish->valuestring[0]) {
        ctx->is_done = 1;
        has_finish = 1;
    }

    if (cJSON_IsString(content) && content->valuestring) {
        chat_sse_emit(ctx, content->valuestring);
    } else if (has_finish) {
        chat_sse_emit(ctx, "");     /* 结束帧可能没有 content */
    }
}

static void chat_sse_parse_json(chat_sse_ctx_t *ctx, const char *data)
{
    cJSON *root = cJSON_Parse(data);

    if (!root) {
        printf("[CHAT] bad sse json: %.120s\n", data);
        return;
    }
    chat_sse_parse_error(ctx, root);
    chat_sse_parse_choice(ctx, root);
    cJSON_Delete(root);
}

static void chat_sse_handle_data(chat_sse_ctx_t *ctx, const char *data)
{
    if (strcmp(data, "[DONE]") == 0 || strcmp(data, "done") == 0) {
        ctx->is_done = 1;
        return;
    }
    if (strstr(data, "invalid_request")) {
        printf("[CHAT] invalid_request: %.160s\n", data);
        ctx->is_error = 1;
        return;
    }
    chat_sse_parse_json(ctx, data);
}

/* 处理一行（已保证在 line 缓冲内以 '\0' 结尾） */
static void chat_sse_handle_line(chat_sse_ctx_t *ctx)
{
    char *line = ctx->line;
    char *data = NULL;

    if (ctx->is_line_overflow) {
        return;     /* 超长行整行丢弃 */
    }
    line[ctx->line_len] = '\0';
    if (ctx->line_len == 0) {
        return;     /* SSE 事件之间的空行 */
    }

    if (strncmp(line, "data:", 5) == 0) {
        data = line + 5;
        while (*data == ' ' || *data == '\t') {
            data++;
        }
        chat_sse_handle_data(ctx, data);
        return;
    }
    if (strncmp(line, "event:", 6) == 0 || line[0] == ':') {
        return;     /* SSE 注释 / 事件名 */
    }
    if (!ctx->is_body_logged) {
        ctx->is_body_logged = 1;
        printf("[CHAT] non-sse body: %.160s\n", line);
    }
}

/* 逐字节拆行，兼容单行被 TCP 拆包 / 粘包 */
static void chat_sse_feed(chat_sse_ctx_t *ctx, const char *data, int len)
{
    int i;

    for (i = 0; i < len; i++) {
        char c = data[i];

        if (c == '\n') {
            chat_sse_handle_line(ctx);
            ctx->line_len = 0;
            ctx->is_line_overflow = 0;
            continue;
        }
        if (c == '\r') {
            continue;
        }
        if (ctx->line_len < (int)sizeof(ctx->line) - 1) {
            ctx->line[ctx->line_len++] = c;
        } else {
            ctx->is_line_overflow = 1;
        }
    }
}

/* ------------------------- libcurl 回调 ------------------------- */

static size_t chat_curl_write_cb(char *ptr, size_t size, size_t nmemb, void *user_data)
{
    chat_sse_ctx_t *ctx = (chat_sse_ctx_t *)user_data;
    size_t total = size * nmemb;

    chat_sse_feed(ctx, ptr, (int)total);
    if (ctx->is_done) {
        /* [DONE] 之后服务端可能迟迟不关连接，返回 0 让 libcurl 立即结束 */
        return 0;
    }
    return total;
}

static int chat_curl_xferinfo_cb(void *clientp, curl_off_t dltotal, curl_off_t dlnow,
                                 curl_off_t ultotal, curl_off_t ulnow)
{
    (void)clientp;
    (void)dltotal;
    (void)dlnow;
    (void)ultotal;
    (void)ulnow;

    return s_chat_stop_req ? 1 : 0;
}

/* ------------------------- 请求构造 ------------------------- */

static char *chat_build_body(const char *bot_id, const char *question)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *messages = NULL;
    cJSON *message = NULL;
    char *body = NULL;

    if (!root) {
        return NULL;
    }

    //其他请求体参数自行添加，目前仅包含基础参数

    cJSON_AddStringToObject(root, "bot_id", bot_id);
    // cJSON_AddStringToObject(root, "model", "symptom_check"); //根据需要更改model

    //当前近支持单轮对话，无上下文记忆。如需要上下文记忆需要更改此处messages数组成员，具体修改参考官方文档。
    //同时考虑到791的RAM容量，如果需要上下文功能建议将文本信息写入卡中。
    messages = cJSON_AddArrayToObject(root, "messages");
    if (!messages) {
        goto exit;
    }
    message = cJSON_CreateObject();
    if (!message) {
        goto exit;
    }
    cJSON_AddStringToObject(message, "role", "user");
    cJSON_AddStringToObject(message, "content", question);
    if (!cJSON_AddItemToArray(messages, message)) {
        cJSON_Delete(message);
        goto exit;
    }


    cJSON_AddBoolToObject(root, "stream", 1);
    body = cJSON_PrintUnformatted(root);

exit:
    cJSON_Delete(root);
    return body;
}

static chat_sse_ctx_t *chat_sse_ctx_new(chat_stream_cb_t cb, void *priv,
                                        chat_result_t *out)
{
    chat_sse_ctx_t *ctx = (chat_sse_ctx_t *)malloc(sizeof(chat_sse_ctx_t));

    if (!ctx) {
        return NULL;
    }
    memset(ctx, 0, sizeof(*ctx));
    ctx->cb = cb;
    ctx->priv = priv;
    ctx->out = out;
    return ctx;
}

static int chat_curl_add_header(struct curl_slist **headers, const char *text)
{
    struct curl_slist *list = curl_slist_append(*headers, text);

    if (!list) {
        return -1;
    }
    *headers = list;
    return 0;
}

static CURL *chat_curl_create(const chat_agent_config_t *cfg, const char *body,
                              chat_sse_ctx_t *ctx, struct curl_slist **headers)
{
    const char *service = (cfg->service_name && cfg->service_name[0])
                          ? cfg->service_name : CHAT_API_SERVICE_NAME;
    char header[CHAT_AUTH_HEADER_SIZE];
    CURL *curl = NULL;
    int n;

    n = snprintf(header, sizeof(header), "Authorization: Bearer %s", cfg->api_key);
    if (n < 0 || n >= (int)sizeof(header)) {
        return NULL;
    }

    if (chat_curl_add_header(headers, header) != 0 ||
        chat_curl_add_header(headers, "Content-Type: application/json") != 0 ||
        chat_curl_add_header(headers, "Accept: text/event-stream") != 0 ||
        chat_curl_add_header(headers, "Accept-Encoding: identity") != 0 ||
        chat_curl_add_header(headers, "Connection: close") != 0) {
        return NULL;
    }

    n = snprintf(header, sizeof(header), "ServiceName: %s", service);
    if (n <= 0 || n >= (int)sizeof(header)) {
        return NULL;
    }
    if (chat_curl_add_header(headers, header) != 0) {
        return NULL;
    }

    curl = curl_easy_init();
    if (!curl) {
        return NULL;
    }

    curl_easy_setopt(curl, CURLOPT_URL, CHAT_API_URL);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, *headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)strlen(body));
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, chat_curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, ctx);
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, chat_curl_xferinfo_cb);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, (long)CHAT_CONNECT_TIMEOUT_S);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)CHAT_TOTAL_TIMEOUT_S);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_1_1);
    /* 设备 lwIP 默认关闭 IPv6，显式指定 IPv4 避免双栈解析等待 */
    curl_easy_setopt(curl, CURLOPT_IPRESOLVE, (long)CURL_IPRESOLVE_V4);
    /* 设备端无受信任 CA 存储，与工程内其它 HTTPS 请求保持一致 */
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
    curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, (long)CHAT_HTTP_BUFFER_SIZE);

    return curl;
}

/* ------------------------- 对外接口 ------------------------- */

int chat_completion_request(const chat_agent_config_t *cfg, const char *question,
                            chat_stream_cb_t cb, void *priv, chat_result_t *out)
{
    chat_sse_ctx_t *ctx = NULL;
    struct curl_slist *headers = NULL;
    chat_result_t *tmp_out = NULL;
    CURL *curl = NULL;
    char *body = NULL;
    long http_code = 0;
    CURLcode res;
    int ret = CHAT_OK;

    if (out) {
        memset(out, 0, sizeof(*out));
    }
    if (!cfg || !cfg->bot_id || !cfg->bot_id[0] ||
        !cfg->api_key || !cfg->api_key[0] || !question || !question[0]) {
        printf("[CHAT] config or question is empty\n");
        return CHAT_ERR_PARAM;
    }

    /* 回调模式也要有累计文本缓冲，这里按需临时分配，正常调用方直接传 out */
    if (!out) {
        tmp_out = (chat_result_t *)malloc(sizeof(*tmp_out));
        if (!tmp_out) {
            return CHAT_ERR_MEM;
        }
        memset(tmp_out, 0, sizeof(*tmp_out));
        out = tmp_out;
    }

    chat_completion_global_init();
    s_chat_stop_req = 0;

    body = chat_build_body(cfg->bot_id, question);
    ctx = chat_sse_ctx_new(cb, priv, out);
    if (!body || !ctx) {
        ret = CHAT_ERR_MEM;
        goto exit;
    }

    curl = chat_curl_create(cfg, body, ctx, &headers);
    if (!curl) {
        ret = CHAT_ERR_MEM;
        goto exit;
    }

    res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    if (s_chat_stop_req) {
        printf("[CHAT] request stopped by user\n");
        ret = CHAT_ERR_STOPPED;
    } else if (res != CURLE_OK && !(res == CURLE_WRITE_ERROR && ctx->is_done)) {
        /* 收到 [DONE] 后主动中止写回调，属于正常结束 */
        printf("[CHAT] curl failed: %s\n", curl_easy_strerror(res));
        ret = CHAT_ERR_NET;
    } else if (http_code != 200) {
        printf("[CHAT] http status %ld\n", http_code);
        ret = CHAT_ERR_HTTP;
    } else if (ctx->is_error) {
        ret = CHAT_ERR_SERVER;
    }

exit:
    if (ctx) {
        out->code = ctx->code;
        out->is_error = (ret != CHAT_OK) ? 1 : 0;
        out->is_last = ctx->is_done ? 1 : 0;
        free(ctx);
    }
    if (curl) {
        curl_easy_cleanup(curl);
    }
    if (headers) {
        curl_slist_free_all(headers);
    }
    free(body);
    free(tmp_out);
    return ret;
}
