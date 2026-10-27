/*
 * Volc TTS —— HTTP Chunked / 协议层
 *
 * 本文件只负责:
 *   1. 用 libcurl 发起 HTTPS POST（X-Api-Key / Resource-Id / Request-Id）
 *   2. 流式解析 chunked 响应中的逐行 JSON
 *   3. 对 data 字段做流式 base64 解码，把 PCM 通过回调交给应用层
 *
 * 注意: 服务端单块音频的 base64 可能远超几十 KB，不能把整行缓存下来，
 * 这里采用状态机：定位到 "data":" 后边收边解码，行缓冲只保存 JSON 头尾。
 *
 * 具体业务流程与 DAC 播放都在 main.c。
 */

#include "volc_tts.h"

#include "asm/cpu.h"
#include "cJSON.h"

#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>

#define VOLC_TTS_SUCCESS_CODE           0
#define VOLC_TTS_SUCCESS_FINISH_CODE    20000000

enum {
    TTS_JSON_PREFIX = 0,                /* 正在收 JSON 头/尾 */
    TTS_JSON_B64,                       /* 正在收 base64 音频 */
};

/* 单次请求的流式解析上下文 */
typedef struct {
    char line[VOLC_TTS_LINE_BUF_SIZE];  /* 只保存 JSON 头尾，不保存音频 */
    int line_len;
    int line_overflow;
    int state;
    int data_seen;

    char b64_quad[4];
    int b64_len;
    u8 pcm[VOLC_TTS_PCM_BUF_SIZE];      /* 解码后的小块音频 */
    int pcm_len;

    int code;                           /* 服务端错误码 */
    int is_error;
    int audio_bytes;
    volc_tts_audio_cb_t audio_cb;
    void *priv;
} tts_http_ctx_t;

static u8 s_curl_inited;
static volatile u8 s_stop_req;

/* ------------------------- 基础工具 ------------------------- */

static void tts_gen_uuid(char *out, int size)
{
    u32 a = (u32)CPU_RAND();
    u32 b = (u32)CPU_RAND();
    u32 c = (u32)CPU_RAND();
    u32 d = (u32)CPU_RAND();

    snprintf(out, size, "%08x-%04x-4%03x-%04x-%08x%04x",
             a,
             b & 0xffff,
             c & 0x0fff,
             ((d >> 16) & 0x3fff) | 0x8000,
             d & 0xffff,
             (a ^ c) & 0xffff);
}

static void tts_curl_global_init(void)
{
    if (s_curl_inited) {
        return;
    }
    if (curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK) {
        s_curl_inited = 1;
    }
}

static int tts_b64_val(char c)
{
    if (c >= 'A' && c <= 'Z') {
        return c - 'A';
    }
    if (c >= 'a' && c <= 'z') {
        return c - 'a' + 26;
    }
    if (c >= '0' && c <= '9') {
        return c - '0' + 52;
    }
    if (c == '+') {
        return 62;
    }
    if (c == '/') {
        return 63;
    }
    if (c == '=') {
        return 0;
    }
    return -1;
}

/* ------------------------- 请求体 ------------------------- */

static char *tts_build_body(const char *text)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *params = NULL;
    cJSON *audio = NULL;
    char *body = NULL;

    if (!root) {
        return NULL;
    }

    params = cJSON_AddObjectToObject(root, "req_params");
    if (!params) {
        goto exit;
    }
    cJSON_AddStringToObject(params, "text", text);
    cJSON_AddStringToObject(params, "speaker", VOLC_TTS_SPEAKER);

    audio = cJSON_AddObjectToObject(params, "audio_params");
    if (!audio) {
        goto exit;
    }
    cJSON_AddStringToObject(audio, "format", VOLC_TTS_FORMAT);
    cJSON_AddNumberToObject(audio, "sample_rate", VOLC_TTS_SAMPLE_RATE);
    cJSON_AddNumberToObject(audio, "speech_rate", VOLC_TTS_SPEECH_RATE);

    body = cJSON_PrintUnformatted(root);

exit:
    cJSON_Delete(root);
    return body;
}

/* ------------------------- 音频输出 ------------------------- */

static void tts_flush_pcm(tts_http_ctx_t *ctx)
{
    if (ctx->pcm_len <= 0) {
        return;
    }
    if (ctx->audio_cb && ctx->audio_cb(ctx->pcm, ctx->pcm_len, ctx->priv) < 0) {
        printf("[TTS] audio callback aborted\n");
        ctx->is_error = 1;
    } else {
        ctx->audio_bytes += ctx->pcm_len;
    }
    ctx->pcm_len = 0;
}

static void tts_pcm_put(tts_http_ctx_t *ctx, u8 data)
{
    ctx->pcm[ctx->pcm_len++] = data;
    if (ctx->pcm_len >= (int)sizeof(ctx->pcm)) {
        tts_flush_pcm(ctx);
    }
}

/* 4 个 base64 字符解出 1~3 字节，边解边写 PCM 缓冲 */
static void tts_b64_emit(tts_http_ctx_t *ctx, const char *quad)
{
    int v0 = tts_b64_val(quad[0]);
    int v1 = tts_b64_val(quad[1]);
    int v2 = tts_b64_val(quad[2]);
    int v3 = tts_b64_val(quad[3]);

    tts_pcm_put(ctx, (u8)((v0 << 2) | (v1 >> 4)));
    if (quad[2] != '=') {
        tts_pcm_put(ctx, (u8)((v1 << 4) | (v2 >> 2)));
    }
    if (quad[3] != '=') {
        tts_pcm_put(ctx, (u8)((v2 << 6) | v3));
    }
}

/* ------------------------- 响应解析 ------------------------- */

/* 行内是否已经出现 "data":"（允许冒号后带空格）*/
static int tts_line_has_data_open(tts_http_ctx_t *ctx)
{
    int i;
    int n = ctx->line_len;

    for (i = 0; i + 6 <= n; i++) {
        int j;

        if (memcmp(ctx->line + i, "\"data\"", 6) != 0) {
            continue;
        }
        j = i + 6;
        while (j < n && (ctx->line[j] == ' ' || ctx->line[j] == '\t')) {
            j++;
        }
        if (j >= n || ctx->line[j] != ':') {
            continue;
        }
        j++;
        while (j < n && (ctx->line[j] == ' ' || ctx->line[j] == '\t')) {
            j++;
        }
        if (j < n && ctx->line[j] == '"') {
            return 1;
        }
    }
    return 0;
}

static void tts_reset_line(tts_http_ctx_t *ctx)
{
    ctx->line_len = 0;
    ctx->line_overflow = 0;
    ctx->data_seen = 0;
    ctx->state = TTS_JSON_PREFIX;
}

/* 解析一行 JSON（data 字段已被替换为空串），检查错误码和用量 */
static void tts_parse_line(tts_http_ctx_t *ctx)
{
    cJSON *root;
    cJSON *code;
    cJSON *message;
    cJSON *usage;
    int code_val;

    if (ctx->line_len == 0) {
        return;
    }
    if (ctx->line_overflow) {
        printf("[TTS] json line too long without data field\n");
        return;
    }
    ctx->line[ctx->line_len] = '\0';

    root = cJSON_Parse(ctx->line);
    if (!root) {
        printf("[TTS] bad json: %.80s\n", ctx->line);
        return;
    }

    code = cJSON_GetObjectItem(root, "code");
    message = cJSON_GetObjectItem(root, "message");
    usage = cJSON_GetObjectItem(root, "usage");

    code_val = cJSON_IsNumber(code) ? code->valueint : VOLC_TTS_SUCCESS_CODE;

    if (code_val != VOLC_TTS_SUCCESS_CODE &&
        code_val != VOLC_TTS_SUCCESS_FINISH_CODE) {
        printf("[TTS] server error code=%d message=%s\n", code_val,
               cJSON_IsString(message) ? message->valuestring : "");
        ctx->code = code_val;
        ctx->is_error = 1;
    }

    if (cJSON_IsObject(usage)) {
        cJSON *words = cJSON_GetObjectItem(usage, "text_words");

        if (cJSON_IsNumber(words)) {
            printf("[TTS] usage text_words=%d\n", words->valueint);
        }
    }

    cJSON_Delete(root);
}

/* 逐字节状态机：JSON 头 -> base64 音频 -> JSON 尾 -> 换行 */
static void tts_feed_data(tts_http_ctx_t *ctx, const char *data, int len)
{
    int i;

    for (i = 0; i < len; i++) {
        char c = data[i];

        if (ctx->state == TTS_JSON_B64) {
            if (c == '"') {
                if (ctx->b64_len != 0) {
                    printf("[TTS] bad base64 tail\n");
                }
                ctx->b64_len = 0;
                tts_flush_pcm(ctx);
                if (ctx->line_len < (int)sizeof(ctx->line) - 1) {
                    ctx->line[ctx->line_len++] = '"';
                } else {
                    ctx->line_overflow = 1;
                }
                ctx->state = TTS_JSON_PREFIX;
                continue;
            }
            if (c == '\n' || c == '\r') {
                ctx->b64_len = 0;
                tts_flush_pcm(ctx);
                tts_parse_line(ctx);
                tts_reset_line(ctx);
                continue;
            }
            if (tts_b64_val(c) < 0) {
                continue;       /* 忽略 base64 中的空白等字符 */
            }
            ctx->b64_quad[ctx->b64_len++] = c;
            if (ctx->b64_len == 4) {
                tts_b64_emit(ctx, ctx->b64_quad);
                ctx->b64_len = 0;
            }
            continue;
        }

        if (c == '\n') {
            tts_parse_line(ctx);
            tts_reset_line(ctx);
            continue;
        }
        if (c == '\r') {
            continue;
        }
        if (ctx->line_len < (int)sizeof(ctx->line) - 1) {
            ctx->line[ctx->line_len++] = c;
        } else {
            ctx->line_overflow = 1;
        }

        if (!ctx->data_seen && !ctx->line_overflow && tts_line_has_data_open(ctx)) {
            ctx->data_seen = 1;
            ctx->state = TTS_JSON_B64;
            ctx->b64_len = 0;
        }
    }
}

/* ------------------------- libcurl 回调 ------------------------- */

static size_t tts_curl_write_cb(char *ptr, size_t size, size_t nmemb, void *user_data)
{
    tts_http_ctx_t *ctx = (tts_http_ctx_t *)user_data;
    size_t total = size * nmemb;

    if (s_stop_req || ctx->is_error) {
        return 0;
    }

    tts_feed_data(ctx, ptr, (int)total);

    if (s_stop_req || ctx->is_error) {
        return 0;
    }
    return total;
}

static int tts_curl_xferinfo_cb(void *clientp, curl_off_t dltotal, curl_off_t dlnow,
                                curl_off_t ultotal, curl_off_t ulnow)
{
    (void)clientp;
    (void)dltotal;
    (void)dlnow;
    (void)ultotal;
    (void)ulnow;

    return s_stop_req ? 1 : 0;
}

/* ------------------------- 对外接口 ------------------------- */

void volc_tts_stop(void)
{
    s_stop_req = 1;
}

int volc_tts_request(const char *text, volc_tts_audio_cb_t audio_cb, void *priv)
{
    tts_http_ctx_t *ctx = NULL;
    struct curl_slist *headers = NULL;
    CURL *curl = NULL;
    char *body = NULL;
    char uuid[40] = {0};
    char header[256];
    CURLcode res;
    long http_code = 0;
    int ret = -1;

    if (!text || !text[0]) {
        return -1;
    }

    tts_curl_global_init();

    body = tts_build_body(text);
    if (!body) {
        return -1;
    }

    ctx = (tts_http_ctx_t *)calloc(1, sizeof(*ctx));
    if (!ctx) {
        goto exit;
    }
    ctx->audio_cb = audio_cb;
    ctx->priv = priv;
    ctx->state = TTS_JSON_PREFIX;

    tts_gen_uuid(uuid, sizeof(uuid));

    snprintf(header, sizeof(header), "X-Api-Key: %s", VOLC_TTS_API_KEY);
    headers = curl_slist_append(headers, header);
    snprintf(header, sizeof(header), "X-Api-Resource-Id: %s", VOLC_TTS_RESOURCE_ID);
    headers = curl_slist_append(headers, header);
    snprintf(header, sizeof(header), "X-Api-Request-Id: %s", uuid);
    headers = curl_slist_append(headers, header);
    headers = curl_slist_append(headers, "Content-Type: application/json");
    headers = curl_slist_append(headers, "Connection: keep-alive");
    headers = curl_slist_append(headers, "X-Control-Require-Usage-Tokens-Return: *");

    curl = curl_easy_init();
    if (!curl) {
        printf("[TTS] curl init failed\n");
        goto exit;
    }

    s_stop_req = 0;

    curl_easy_setopt(curl, CURLOPT_URL, VOLC_TTS_HTTP_URL);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, (long)strlen(body));
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, tts_curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, ctx);
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, tts_curl_xferinfo_cb);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, (long)VOLC_TTS_CONNECT_TIMEOUT_S);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, (long)VOLC_TTS_TOTAL_TIMEOUT_S);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_1_1);
    curl_easy_setopt(curl, CURLOPT_IPRESOLVE, (long)CURL_IPRESOLVE_V4);
    curl_easy_setopt(curl, CURLOPT_BUFFERSIZE, (long)VOLC_TTS_HTTP_BUF_SIZE);
    /* 设备端无受信任 CA 存储，与工程内其它 HTTPS 请求保持一致 */
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

    printf("[TTS] http request: %s\n", VOLC_TTS_HTTP_URL);
    printf("[TTS] body: %s\n", body);

    res = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    /* 收尾：可能停在 base64 中（异常）或最后一行没有换行 */
    if (ctx->state == TTS_JSON_B64) {
        ctx->b64_len = 0;
        tts_flush_pcm(ctx);
        tts_parse_line(ctx);
        tts_reset_line(ctx);
    } else if (ctx->line_len > 0) {
        tts_parse_line(ctx);
    }

    if (s_stop_req) {
        printf("[TTS] request stopped\n");
        ret = -2;
    } else if (res != CURLE_OK && !(res == CURLE_WRITE_ERROR && ctx->is_error)) {
        printf("[TTS] curl failed: %s\n", curl_easy_strerror(res));
        ret = -1;
    } else if (http_code != 200) {
        printf("[TTS] http status %ld\n", http_code);
        ret = -1;
    } else if (ctx->is_error) {
        ret = -3;
    } else {
        ret = 0;
    }

    printf("[TTS] http done, ret=%d, audio=%d bytes\n", ret, ctx->audio_bytes);

exit:
    if (curl) {
        curl_easy_cleanup(curl);
    }
    if (headers) {
        curl_slist_free_all(headers);
    }
    free(ctx);
    free(body);
    return ret;
}
