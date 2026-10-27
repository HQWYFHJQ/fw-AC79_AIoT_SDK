/*
 * SAUC 流式语音识别 —— websocket / 协议层
 *
 * 本文件只负责:
 *   1. wss:// websocket 建立/关闭
 *   2. SAUC 首帧(full client request) 与音频帧(audio-only request) 的发送
 *   3. gzip 压缩/解压、服务端响应解析（协议加解密）
 *
 * 具体业务流程、事件队列、文件读取都在 main.c。
 */

#include "asr.h"
#include "asm/cpu.h"
#include "cJSON.h"
#include "zliblite/zlib.h"

#include <stdlib.h>
#include <string.h>

/* SAUC 协议常量 (protocol.py) */
#define SAUC_PROTOCOL_VERSION       0x01
#define SAUC_MSG_FULL_REQUEST       0x01
#define SAUC_MSG_AUDIO_ONLY         0x02
#define SAUC_MSG_FULL_RESPONSE      0x09
#define SAUC_MSG_ERROR_RESPONSE     0x0f
#define SAUC_FLAG_POS_SEQUENCE      0x01
#define SAUC_FLAG_LAST_PACKAGE      0x02
#define SAUC_FLAG_EVENT             0x04
#define SAUC_FLAG_NEG_WITH_SEQUENCE 0x03
#define SAUC_SERIALIZATION_JSON     0x01
#define SAUC_COMPRESSION_GZIP       0x01

static struct websocket_struct *g_asr_ws;
static asr_recv_cb_t g_asr_recv_cb;
static char g_asr_url[128];
static char g_asr_headers[256];

/* 服务端响应 gzip 解压缓冲:
 * 小响应优先用静态缓冲，大响应按 gzip ISIZE/实际大小临时 malloc。
 * 只在 websocket recv 线程的 asr_proto_parse_response() 中使用。
 */
static u8 g_asr_json_small[ASR_JSON_SMALL_SIZE];

/* ------------------------- 基础工具 ------------------------- */

static u32 asr_rd_le32(const u8 *p)
{
    return (u32)p[0] | ((u32)p[1] << 8) | ((u32)p[2] << 16) | ((u32)p[3] << 24);
}

static u32 asr_rd_be32(const u8 *p)
{
    return ((u32)p[0] << 24) | ((u32)p[1] << 16) | ((u32)p[2] << 8) | (u32)p[3];
}

static void asr_wr_be32(u8 *p, u32 v)
{
    p[0] = (v >> 24) & 0xff;
    p[1] = (v >> 16) & 0xff;
    p[2] = (v >> 8) & 0xff;
    p[3] = v & 0xff;
}

static void asr_gen_uuid(char *out, int size)
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

/* ------------------------- gzip ------------------------- */

static voidpf asr_zalloc(voidpf opaque, uInt items, uInt size)
{
    (void)opaque;
    return malloc((size_t)items * size);
}

static void asr_zfree(voidpf opaque, voidpf ptr)
{
    (void)opaque;
    free(ptr);
}

/* gzip 压缩: windowBits = 15 + 16 -> gzip header/trailer */
int asr_gzip_compress(const u8 *src, int src_len, u8 *dst, int dst_cap)
{
    static const u8 empty = 0;
    z_stream strm;
    int ret;

    if (!src || src_len <= 0) {
        src = &empty;
        src_len = 0;
    }

    memset(&strm, 0, sizeof(strm));
    strm.zalloc = asr_zalloc;
    strm.zfree = asr_zfree;

    ret = deflateInit2(&strm, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
                       15 + 16, 8, Z_DEFAULT_STRATEGY);
    if (ret != Z_OK) {
        return -1;
    }

    strm.next_in = (Bytef *)src;
    strm.avail_in = (uInt)src_len;
    strm.next_out = dst;
    strm.avail_out = (uInt)dst_cap;

    ret = deflate(&strm, Z_FINISH);
    if (ret != Z_STREAM_END) {
        deflateEnd(&strm);
        return -1;
    }

    ret = dst_cap - (int)strm.avail_out;
    deflateEnd(&strm);
    return ret;
}

int asr_gzip_decompress(const u8 *src, int src_len, u8 *dst, int dst_cap)
{
    z_stream strm;
    int ret;

    memset(&strm, 0, sizeof(strm));
    strm.zalloc = asr_zalloc;
    strm.zfree = asr_zfree;

    ret = inflateInit2(&strm, 15 + 16);
    if (ret != Z_OK) {
        return -1;
    }

    strm.next_in = (Bytef *)src;
    strm.avail_in = (uInt)src_len;
    strm.next_out = dst;
    strm.avail_out = (uInt)dst_cap;

    ret = inflate(&strm, Z_FINISH);
    if (ret != Z_STREAM_END) {
        inflateEnd(&strm);
        return (ret == Z_OK || ret == Z_BUF_ERROR) ? -2 : -1;
    }

    ret = dst_cap - (int)strm.avail_out;
    inflateEnd(&strm);
    return ret;
}

/* ---------------------- SAUC 协议打包 ---------------------- */

/*
 * 4 字节头:
 *   [0] = (protocol_version << 4) | header_size
 *   [1] = (message_type << 4) | message_type_specific_flags
 *   [2] = (serialization_type << 4) | compression_type
 *   [3] = reserved
 * 之后: seq(4, big-endian) + payload_size(4, big-endian) + payload(gzip)
 */
static u8 *asr_build_packet(int msg_type, int flags, int seq,
                            const u8 *payload, int payload_len, int *out_len)
{
    int cap;
    int gz_len;
    u8 *pkt;

    /* 压缩后最坏情况略大于原数据, 留 1% + 64 字节 */
    cap = 12 + payload_len + (payload_len / 100) + 64;
    pkt = (u8 *)malloc(cap);
    if (!pkt) {
        return NULL;
    }

    pkt[0] = (SAUC_PROTOCOL_VERSION << 4) | 1;      /* header_size = 1*4 */
    pkt[1] = (msg_type << 4) | (flags & 0x0f);
    pkt[2] = (SAUC_SERIALIZATION_JSON << 4) | SAUC_COMPRESSION_GZIP;
    pkt[3] = 0x00;

    asr_wr_be32(pkt + 4, (u32)seq);

    gz_len = asr_gzip_compress(payload, payload_len, pkt + 12, cap - 12);
    if (gz_len <= 0) {
        free(pkt);
        return NULL;
    }

    asr_wr_be32(pkt + 8, (u32)gz_len);
    *out_len = 12 + gz_len;
    return pkt;
}

/* --------------------- SAUC 响应解析 --------------------- */

int asr_proto_parse_response(const u8 *msg, int len, asr_result_t *out)
{
    int header_size;
    int msg_type;
    int flags;
    int serialization;
    int compression;
    u32 payload_size;
    const u8 *p;
    int remain;

    if (!msg || !out || len < 4) {
        return -1;
    }

    out->code = 0;
    out->is_last = 0;
    out->event = 0;
    out->text[0] = '\0';

    header_size = (msg[0] & 0x0f) * 4;
    msg_type = (msg[1] >> 4) & 0x0f;
    flags = msg[1] & 0x0f;
    serialization = (msg[2] >> 4) & 0x0f;
    compression = msg[2] & 0x0f;

    if (header_size > len) {
        return -1;
    }
    p = msg + header_size;
    remain = len - header_size;

    out->is_last = (flags & SAUC_FLAG_LAST_PACKAGE) ? 1 : 0;

    if (flags & SAUC_FLAG_POS_SEQUENCE) {
        if (remain < 4) {
            return -1;
        }
        p += 4;
        remain -= 4;
    }
    if (flags & SAUC_FLAG_EVENT) {
        if (remain < 4) {
            return -1;
        }
        out->event = (int)asr_rd_be32(p);
        p += 4;
        remain -= 4;
    }

    if (msg_type == SAUC_MSG_FULL_RESPONSE) {
        if (remain < 4) {
            return -1;
        }
        payload_size = asr_rd_be32(p);
        p += 4;
        remain -= 4;
    } else if (msg_type == SAUC_MSG_ERROR_RESPONSE) {
        if (remain < 8) {
            return -1;
        }
        out->code = (int)asr_rd_be32(p);
        payload_size = asr_rd_be32(p + 4);
        p += 8;
        remain -= 8;
    } else {
        return -1;
    }

    if (payload_size == 0 || remain <= 0) {
        return 0;
    }
    if ((int)payload_size > remain) {
        payload_size = (u32)remain;
    }

    if (compression == SAUC_COMPRESSION_GZIP) {
        u8 *json_buf = g_asr_json_small;
        int json_cap = sizeof(g_asr_json_small);
        int json_heap = 0;
        u32 isize = 0;
        int n;

        /* gzip 最后 4 字节是原始数据大小(ISIZE)，据此决定缓冲大小 */
        if (payload_size >= 18) {
            isize = asr_rd_le32(p + payload_size - 4);
        }
        if (isize == 0 || isize > ASR_JSON_MAX_SIZE) {
            isize = ASR_JSON_MAX_SIZE;
        }
        if ((int)isize + 1 > json_cap) {
            json_cap = (int)isize + 1;
            json_buf = (u8 *)malloc(json_cap);
            if (!json_buf) {
                return -1;
            }
            json_heap = 1;
        }

        n = asr_gzip_decompress(p, (int)payload_size, json_buf, json_cap - 1);
        if (n == -2 && !json_heap) {
            /* 静态缓冲不够，改用最大缓冲再试一次 */
            json_buf = (u8 *)malloc(ASR_JSON_MAX_SIZE);
            if (!json_buf) {
                return -1;
            }
            json_heap = 1;
            n = asr_gzip_decompress(p, (int)payload_size,
                                    json_buf, ASR_JSON_MAX_SIZE - 1);
        }
        if (n <= 0) {
            if (json_heap) {
                free(json_buf);
            }
            return -1;
        }
        json_buf[n] = '\0';

        if (serialization == SAUC_SERIALIZATION_JSON) {
            cJSON *root = cJSON_Parse((const char *)json_buf);
            if (!root) {
                if (json_heap) {
                    free(json_buf);
                }
                return -1;
            }

            cJSON *code = cJSON_GetObjectItem(root, "code");
            if (cJSON_IsNumber(code) && code->valueint != 0) {
                out->code = code->valueint;
            }

            cJSON *result = cJSON_GetObjectItem(root, "result");
            if (cJSON_IsObject(result)) {
                cJSON *text = cJSON_GetObjectItem(result, "text");
                /* 只接受非空文本，避免最后的空响应覆盖已识别内容 */
                if (cJSON_IsString(text) && text->valuestring && text->valuestring[0]) {
                    strncpy(out->text, text->valuestring, sizeof(out->text) - 1);
                    out->text[sizeof(out->text) - 1] = '\0';
                }
            }

            cJSON_Delete(root);
        }

        if (json_heap) {
            free(json_buf);
        }
    } else {
        u8 *json_buf = g_asr_json_small;
        int json_heap = 0;

        if (payload_size >= ASR_JSON_MAX_SIZE) {
            return -1;
        }
        if ((int)payload_size + 1 > (int)sizeof(g_asr_json_small)) {
            json_buf = (u8 *)malloc(payload_size + 1);
            if (!json_buf) {
                return -1;
            }
            json_heap = 1;
        }
        memcpy(json_buf, p, payload_size);
        json_buf[payload_size] = '\0';

        if (serialization == SAUC_SERIALIZATION_JSON) {
            cJSON *root = cJSON_Parse((const char *)json_buf);
            if (!root) {
                if (json_heap) {
                    free(json_buf);
                }
                return -1;
            }

            cJSON *code = cJSON_GetObjectItem(root, "code");
            if (cJSON_IsNumber(code) && code->valueint != 0) {
                out->code = code->valueint;
            }

            cJSON *result = cJSON_GetObjectItem(root, "result");
            if (cJSON_IsObject(result)) {
                cJSON *text = cJSON_GetObjectItem(result, "text");
                if (cJSON_IsString(text) && text->valuestring && text->valuestring[0]) {
                    strncpy(out->text, text->valuestring, sizeof(out->text) - 1);
                    out->text[sizeof(out->text) - 1] = '\0';
                }
            }

            cJSON_Delete(root);
        }

        if (json_heap) {
            free(json_buf);
        }
    }

    return 0;
}

/* ------------------------ websocket 层 ------------------------ */

/* websocket 收包回调: 运行在 websocket recv 线程，转交给应用层 */
static void asr_ws_internal_recv_cb(u8 *buf, u32 len, u8 type)
{
    if (g_asr_recv_cb) {
        g_asr_recv_cb(buf, (int)len, type);
    }
}

static void asr_ws_reg(struct websocket_struct *ws, char mode)
{
    memset(ws, 0, sizeof(struct websocket_struct));

    ws->_init         = websockets_client_socket_init;
    ws->_exit         = websockets_client_socket_exit;
    ws->_handshack    = webcockets_client_socket_handshack;
    ws->_send         = websockets_client_socket_send;
    ws->_recv_thread  = websockets_client_socket_recv_thread;
    ws->_heart_thread = websockets_client_socket_heart_thread;
    ws->_recv_cb      = asr_ws_internal_recv_cb;
    ws->_recv         = NULL;
    ws->websocket_mode = mode;
}

/* 生成新版控制台鉴权头。
 * SDK 的 websocket 握手没有自定义 header 接口，但 user_agent_str 会被原样
 * 拼进 HTTP 请求头，因此这里利用它注入 SAUC 需要的三个头。
 */
static int asr_build_auth_headers(char *out, int size)
{
    char uuid[40] = {0};

    if (SAUC_API_KEY[0] == '\0') {
        printf("[ASR] SAUC_API_KEY is empty, fill your API Key first\n");
        return -1;
    }

    asr_gen_uuid(uuid, sizeof(uuid));

    snprintf(out, size,
             "X-Api-Resource-Id: %s\r\n"
             "X-Api-Request-Id: %s\r\n"
             "X-Api-Key: %s\r\n",
             SAUC_RESOURCE_ID, uuid, SAUC_API_KEY);

    return 0;
}

int asr_ws_open(const char *url, asr_recv_cb_t cb)
{
    const char *target = (url && url[0]) ? url : SAUC_WS_URL;
    int inited = 0;

    if (g_asr_ws) {
        asr_ws_close();
    }

    if (asr_build_auth_headers(g_asr_headers, sizeof(g_asr_headers)) != 0) {
        return -1;
    }

    memset(g_asr_url, 0, sizeof(g_asr_url));
    strncpy(g_asr_url, target, sizeof(g_asr_url) - 1);

    g_asr_ws = (struct websocket_struct *)malloc(sizeof(struct websocket_struct));
    if (!g_asr_ws) {
        return -1;
    }

    g_asr_recv_cb = cb;

    asr_ws_reg(g_asr_ws, WEBSOCKETS_MODE);
    g_asr_ws->ip_or_url = (u8 *)g_asr_url;
    g_asr_ws->origin_str = NULL;
    g_asr_ws->user_agent_str = g_asr_headers;
    g_asr_ws->recv_time_out = 1000;

    /* 缩小库默认的 23KB 接收缓存；帧更大时库内部会自动 realloc */
    g_asr_ws->recv_buf_size = ASR_WS_RECV_BUF_SIZE;
    g_asr_ws->recv_buf = (u8 *)malloc(ASR_WS_RECV_BUF_SIZE);
    if (!g_asr_ws->recv_buf) {
        printf("[ASR] malloc recv buf failed\n");
        goto fail;
    }

    if (websockets_struct_check(sizeof(struct websocket_struct)) == FALSE) {
        printf("[ASR] websocket struct version mismatch\n");
        goto fail;
    }

    if (g_asr_ws->_init(g_asr_ws) == FALSE) {
        printf("[ASR] websocket init failed\n");
        goto fail;
    }
    inited = 1;

    if (g_asr_ws->_handshack(g_asr_ws) == FALSE) {
        printf("[ASR] websocket handshake failed\n");
        goto fail;
    }

    thread_fork("asr_ws_recv", 18, ASR_RECV_THREAD_STACK_WORDS, 0,
                &g_asr_ws->recv_thread_id,
                g_asr_ws->_recv_thread, g_asr_ws);
    thread_fork("asr_ws_heart", 19, ASR_HEART_THREAD_STACK_WORDS, 0,
                &g_asr_ws->ping_thread_id,
                g_asr_ws->_heart_thread, g_asr_ws);

    printf("[ASR] websocket connected: %s\n", g_asr_url);
    return 0;

fail:
    if (inited) {
        g_asr_ws->_exit(g_asr_ws);
    } else if (g_asr_ws->recv_buf) {
        free(g_asr_ws->recv_buf);
        g_asr_ws->recv_buf = NULL;
    }
    free(g_asr_ws);
    g_asr_ws = NULL;
    g_asr_recv_cb = NULL;
    return -1;
}

int asr_ws_send_full_request(int *seq, const asr_audio_info_t *info)
{
    char json[512];
    u8 *pkt;
    int pkt_len = 0;
    int json_len;
    int ret;

    if (!g_asr_ws || !seq || !info) {
        return -1;
    }

    json_len = snprintf(json, sizeof(json),
                        "{\"user\":{\"uid\":\"jl791_asr_agent\"},"
                        "\"audio\":{\"format\":\"%s\",\"codec\":\"raw\","
                        "\"rate\":%d,\"bits\":%d,\"channel\":%d},"
                        "\"request\":{\"model_name\":\"bigmodel\","
                        "\"enable_itn\":true,\"enable_punc\":true,"
                        "\"enable_ddc\":true,\"show_utterances\":false,"
                        "\"enable_nonstream\":false}}",
                        info->format[0] ? info->format : "pcm",
                        info->sample_rate, info->bits, info->channels);
    if (json_len <= 0 || json_len >= (int)sizeof(json)) {
        return -1;
    }

    pkt = asr_build_packet(SAUC_MSG_FULL_REQUEST, SAUC_FLAG_POS_SEQUENCE,
                           *seq, (const u8 *)json, json_len, &pkt_len);
    if (!pkt) {
        return -1;
    }

    ret = g_asr_ws->_send(g_asr_ws, pkt, pkt_len, WCT_BINDATA);
    free(pkt);

    if (ret <= 0) {
        printf("[ASR] send full request failed\n");
        return -1;
    }

    printf("[ASR] full request sent, seq=%d\n", *seq);
    (*seq)++;
    return 0;
}

int asr_ws_send_audio(int seq, const u8 *data, int len, int is_last)
{
    u8 *pkt;
    int pkt_len = 0;
    int ret;
    int s = is_last ? -seq : seq;
    int flags = is_last ? SAUC_FLAG_NEG_WITH_SEQUENCE : SAUC_FLAG_POS_SEQUENCE;

    if (!g_asr_ws) {
        return -1;
    }

    pkt = asr_build_packet(SAUC_MSG_AUDIO_ONLY, flags, s,
                           data, len, &pkt_len);
    if (!pkt) {
        printf("[ASR] build audio packet failed (len=%d)\n", len);
        return -1;
    }

    ret = g_asr_ws->_send(g_asr_ws, pkt, pkt_len, WCT_BINDATA);
    free(pkt);

    if (ret <= 0) {
        printf("[ASR] send audio failed, seq=%d\n", s);
        return -1;
    }
    return 0;
}

void asr_ws_close(void)
{
    if (!g_asr_ws) {
        return;
    }

    if (g_asr_ws->recv_thread_id) {
        thread_kill(&g_asr_ws->recv_thread_id, KILL_WAIT);
    }
    if (g_asr_ws->ping_thread_id) {
        thread_kill(&g_asr_ws->ping_thread_id, KILL_WAIT);
    }

    g_asr_ws->_exit(g_asr_ws);
    free(g_asr_ws);
    g_asr_ws = NULL;
    g_asr_recv_cb = NULL;
}

int asr_ws_is_connected(void)
{
    return (g_asr_ws && g_asr_ws->websocket_valid == ESTABLISHED) ? 1 : 0;
}
