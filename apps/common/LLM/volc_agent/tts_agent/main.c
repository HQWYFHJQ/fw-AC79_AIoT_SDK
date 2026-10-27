/*
 * Volc TTS Agent —— 应用层（HTTP 流式合成 + DAC 播放）
 *
 * 数据流:
 *   tts_agent_speak(text)
 *        └─> s_tts_q ─> tts_agent_task
 *                         └─ tts_do_speak()
 *                              ├─ tts_pcm_open(): 打开 PCM -> DAC
 *                              ├─ volc_tts_request(): HTTPS + chunked 响应
 *                              │      └─ base64 解码后的 PCM 通过回调写入播放缓冲
 *                              └─ 等待播放缓存排空后关闭 DAC
 *
 * 音频参数:
 *   TTS 请求 PCM(16bit/单声道)，直接走 audio_server 的 PCM 解码器输出到 DAC。
 *
 * 为什么不用 audio_music/pcm_play_api.c:
 *   该接口固定带 AUDIO_ATTR_REAL_TIME，混音器会通过 dec_sync 按缓冲水位
 *   动态调整重采样率。TTS 与开机提示音(16kHz)等混播时，本路 48kHz 会被
 *   压到 16kHz，此时 sync 调整步长(sample_rate/5)相对输出采样率过大，
 *   会把语速瞬间拉快、丢掉音频。这里只做固定比例重采样，靠写缓冲阻塞
 *   反压网络数据，保证语速和音调稳定。
 *
 * 资源占用（估算）:
 *   线程栈: tts_agent_task 16KB
 *   静态  : 状态/队列 约 0.1KB
 *   heap  : 播放缓存 0.5s + 解码输出缓存 0.5s + PCM 小块缓冲
 *           + libcurl/mbedtls TLS 收发缓冲
 */

#include "system/includes.h"
#include "app_config.h"
#include "wifi/wifi_connect.h"
#include "server/audio_server.h"
#include "server/server_core.h"
#include "generic/circular_buf.h"
#include "os/os_api.h"

#include "volc_tts.h"

#include <stdlib.h>
#include <string.h>

/* 置 1: 联网后自动朗读演示文本，单独验证 TTS 链路时使用；
 * 由 volc_agent 的对话业务驱动时置 0，改由 tts_agent_speak() 提供文本。 */
#define TTS_AGENT_AUTO_START            0
#define TTS_AGENT_DEFAULT_TEXT          "你好呀，很高兴和你打招呼。如果你有任何想了解的问题、需要帮忙处理的事务或者只是想闲聊分享日常，都可以直接和我说。"

/* 联网后等待一段时间再演示，避开开机/联网提示音，便于单独验证 TTS */
#define TTS_AGENT_DEMO_DELAY_MS         6000

#define TTS_WAIT_STEP_MS                10

/* ======================== 数据结构 ======================== */

enum {
    TTS_EVT_SPEAK = 1,
};

typedef struct {
    int type;
    char *text;                         /* TTS_EVT_SPEAK 携带，malloc */
} tts_msg_t;

/* PCM -> DAC 播放句柄 */
typedef struct {
    void *dec_server;
    u8 *cache_buf;
    u32 cache_size;
    cbuffer_t cbuf;
    OS_SEM w_sem;                       /* 缓冲有空位时唤醒写者 */
    OS_SEM r_sem;                       /* 缓冲有数据时唤醒解码线程 */
    u8 run_flag;
    u8 channel;                         /* 源 PCM 声道数 */
    u32 out_bytes;                      /* 已交给解码器的字节数(展开后) */
    u32 out_log_bytes;
} tts_pcm_t;

/* ------------------------- 全局状态 ------------------------- */

static OS_QUEUE s_tts_q;
static u8 s_tts_inited;
static tts_pcm_t *s_play_hdl;           /* PCM -> DAC 播放句柄 */
static tts_agent_done_cb_t s_done_cb;   /* 播放结束通知应用层 */

/* ------------------------- PCM 播放器 ------------------------- */

/* 混音器/DAC 通路按双声道取数，PCM 解码器不会自动展开单声道，
 * 这里把 16bit 单声道数据原地复制成 L/R 交错(从尾部往前，避免覆盖) */
static void tts_mono_to_stereo(s16 *data, int mono_bytes)
{
    s16 *start = (s16 *)((u8 *)data + mono_bytes);
    s16 *end = (s16 *)((u8 *)data + mono_bytes * 2);
    u32 i;

    for (i = 0; i < (u32)mono_bytes / 2; i++) {
        *--end = *--start;
        *--end = *start;
    }
}

/* 解码线程回调：从播放缓存取数据；无数据时阻塞等待 */
static int tts_pcm_fread(void *priv, void *data, u32 len)
{
    tts_pcm_t *hdl = (tts_pcm_t *)priv;
    u32 rlen = 0;
    u32 mono_len = (hdl->channel == 1) ? (len >> 1) : len;

    while (hdl->run_flag) {
        rlen = cbuf_get_data_size(&hdl->cbuf);
        rlen = rlen > mono_len ? mono_len : rlen;
        if (rlen > 0) {
            cbuf_read(&hdl->cbuf, data, rlen);
            os_sem_set(&hdl->w_sem, 0);
            os_sem_post(&hdl->w_sem);
            break;
        }
        os_sem_pend(&hdl->r_sem, 0);
    }

    if (hdl->channel == 1) {
        rlen &= ~1u;                    /* 16bit 对齐 */
        tts_mono_to_stereo((s16 *)data, (int)rlen);
        rlen <<= 1;
    }
    hdl->out_bytes += rlen;
    if (hdl->out_bytes - hdl->out_log_bytes >= (u32)VOLC_TTS_SAMPLE_RATE * 4) {
        hdl->out_log_bytes = hdl->out_bytes;
    }

    return (int)rlen;
}

static const struct audio_vfs_ops tts_pcm_vfs_ops = {
    .fread = tts_pcm_fread,
};

static void tts_pcm_close(tts_pcm_t *hdl)
{
    union audio_req req = {0};

    if (!hdl) {
        return;
    }

    printf("[TTS] pcm out: %u bytes\n", hdl->out_bytes);

    hdl->run_flag = 0;
    os_sem_post(&hdl->w_sem);
    os_sem_post(&hdl->r_sem);

    if (hdl->dec_server) {
        req.dec.cmd = AUDIO_DEC_STOP;
        server_request(hdl->dec_server, AUDIO_REQ_DEC, &req);
        server_close(hdl->dec_server);
        hdl->dec_server = NULL;
    }

    os_sem_del(&hdl->w_sem, OS_DEL_ALWAYS);
    os_sem_del(&hdl->r_sem, OS_DEL_ALWAYS);

    if (hdl->cache_buf) {
        free(hdl->cache_buf);
        hdl->cache_buf = NULL;
    }
    free(hdl);
}

static tts_pcm_t *tts_pcm_open(int sample_rate, u8 channel, u8 volume)
{
    union audio_req req = {0};
    tts_pcm_t *hdl = NULL;
    u32 cache_size;

    hdl = (tts_pcm_t *)zalloc(sizeof(*hdl));
    if (!hdl) {
        return NULL;
    }

    /* 先创建信号量，保证后续任一错误分支调用 tts_pcm_close() 都是安全的 */
    os_sem_create(&hdl->w_sem, 0);
    os_sem_create(&hdl->r_sem, 0);

    /* 16bit 数据缓存约 0.5 秒，缓冲太大听感上会有延迟 */
    cache_size = (u32)sample_rate * channel;
    hdl->cache_buf = malloc(cache_size);
    if (!hdl->cache_buf) {
        goto err;
    }
    hdl->cache_size = cache_size;
    cbuf_init(&hdl->cbuf, hdl->cache_buf, cache_size);

    hdl->run_flag = 1;

    hdl->channel = channel;

    hdl->dec_server = server_open("audio_server", "dec");
    if (!hdl->dec_server) {
        goto err;
    }

    req.dec.cmd             = AUDIO_DEC_OPEN;
    req.dec.volume          = volume;
    req.dec.sample_rate     = sample_rate;
    req.dec.channel         = 2;    /* 混音器/DAC 只接受双声道 */
    req.dec.output_buf_len  = cache_size;
    req.dec.vfs_ops         = &tts_pcm_vfs_ops;
    req.dec.dec_type        = "pcm";
    req.dec.sample_source   = "dac";
    req.dec.file            = (FILE *)hdl;
    /* 不设置 AUDIO_ATTR_REAL_TIME: 避免混音器按缓冲水位动态改变重采样率 */

    if (server_request(hdl->dec_server, AUDIO_REQ_DEC, &req)) {
        goto err;
    }

    req.dec.cmd = AUDIO_DEC_START;
    if (server_request(hdl->dec_server, AUDIO_REQ_DEC, &req)) {
        goto err;
    }

    return hdl;

err:
    tts_pcm_close(hdl);
    return NULL;
}

/* 写入 PCM 数据；缓存满时阻塞，形成对网络数据的反压 */
static int tts_pcm_write(tts_pcm_t *hdl, const u8 *data, u32 size)
{
    const u8 *p = data;
    u32 wlen;

    while (size > 0) {
        if (!hdl->run_flag) {
            return -1;
        }
        wlen = cbuf_write(&hdl->cbuf, (void *)p, size);
        if (wlen > 0) {
            os_sem_set(&hdl->r_sem, 0);
            os_sem_post(&hdl->r_sem);
            p += wlen;
            size -= wlen;
            continue;
        }
        os_sem_pend(&hdl->w_sem, 0);
    }

    return 0;
}

static u32 tts_pcm_pending(tts_pcm_t *hdl)
{
    return cbuf_get_data_size(&hdl->cbuf);
}

/* ------------------------- 音频回调 ------------------------- */

static u32 s_in_bytes;                  /* 累计收到的单声道 PCM 字节 */
static u32 s_in_log_bytes;
static u32 s_in_peak;

/* 每秒打印一次输入峰值：峰值长时间为 0 说明解码出的音频本身是静音 */
static void tts_stat_pcm(const u8 *data, int len)
{
    const s16 *pcm = (const s16 *)data;
    int i;

    for (i = 0; i < len / 2; i++) {
        int v = pcm[i] < 0 ? -pcm[i] : pcm[i];

        if ((u32)v > s_in_peak) {
            s_in_peak = (u32)v;
        }
    }
    s_in_bytes += (u32)len;
    if (s_in_bytes - s_in_log_bytes >= (u32)VOLC_TTS_SAMPLE_RATE * 2) {
        s_in_log_bytes = s_in_bytes;
        s_in_peak = 0;
    }
}

/* 运行在 tts_agent_task（libcurl 写回调内），把 PCM 写入播放缓冲 */
static int tts_audio_cb(const u8 *data, int len, void *priv)
{
    (void)priv;

    if (!s_play_hdl || !data || len <= 0) {
        return -1;
    }
    tts_stat_pcm(data, len);
    if (tts_pcm_write(s_play_hdl, data, (u32)len) < 0) {
        printf("[TTS] pcm write failed\n");
        return -1;
    }
    return 0;
}

/* ------------------------- 合成流程 ------------------------- */

/* 等播放缓存和音频服务内部缓冲播完，避免尾部被截断 */
static void tts_drain(void)
{
    u32 waited = 0;

    while (tts_pcm_pending(s_play_hdl) > 0 && waited < VOLC_TTS_DRAIN_TIMEOUT_MS) {
        msleep(TTS_WAIT_STEP_MS);
        waited += TTS_WAIT_STEP_MS;
    }
    if (waited >= VOLC_TTS_DRAIN_TIMEOUT_MS) {
        printf("[TTS] drain timeout, left=%u\n", tts_pcm_pending(s_play_hdl));
    }
    msleep(VOLC_TTS_DRAIN_TAIL_MS);
}

static int tts_do_speak(const char *text)
{
    int ret;

    s_play_hdl = tts_pcm_open(VOLC_TTS_SAMPLE_RATE, VOLC_TTS_CHANNELS, VOLC_TTS_VOLUME);
    if (!s_play_hdl) {
        printf("[TTS] open pcm player failed\n");
        return -1;
    }

    printf("[TTS] speak: %s\n", text);
    s_in_bytes = 0;
    s_in_log_bytes = 0;
    s_in_peak = 0;
    ret = volc_tts_request(text, tts_audio_cb, NULL);

    if (ret == 0) {
        tts_drain();
    }

    tts_pcm_close(s_play_hdl);
    s_play_hdl = NULL;

    printf("[TTS] speak done, ret=%d\n", ret);
    if (s_done_cb) {
        s_done_cb();
    }
    return ret;
}

/* ------------------------- 主任务 ------------------------- */

static void tts_agent_task(void *priv)
{
    int msg[16];

    (void)priv;

    while (1) {
        void *ev;
        tts_msg_t *m;

        if (os_q_pend(&s_tts_q, 0, msg) != 0) {
            continue;
        }
        ev = (void *)msg[0];
        if (!ev) {
            continue;
        }

        m = (tts_msg_t *)ev;
        switch (m->type) {
        case TTS_EVT_SPEAK:
            tts_do_speak(m->text ? m->text : TTS_AGENT_DEFAULT_TEXT);
            break;
        default:
            break;
        }
        free(m->text);
        free(m);
    }
}

/* ------------------------- 对外接口 ------------------------- */

int tts_agent_init(void)
{
    if (s_tts_inited) {
        return 0;
    }

    if (os_q_create(&s_tts_q, VOLC_TTS_EVENT_QUEUE_DEPTH) != 0) {
        printf("[TTS] create event queue failed\n");
        return -1;
    }
    if (thread_fork("tts_agent_task", 10, VOLC_TTS_TASK_STACK_WORDS, 0,
                    NULL, tts_agent_task, NULL) != OS_NO_ERR) {
        printf("[TTS] fork tts task failed\n");
        os_q_del(&s_tts_q, 1);
        return -1;
    }

    s_tts_inited = 1;
    return 0;
}

int tts_agent_speak(const char *text)
{
    const char *src = (text && text[0]) ? text : TTS_AGENT_DEFAULT_TEXT;
    tts_msg_t *msg = NULL;
    int len;

    if (tts_agent_init() != 0) {
        return -1;
    }

    msg = (tts_msg_t *)malloc(sizeof(*msg));
    if (!msg) {
        return -1;
    }
    memset(msg, 0, sizeof(*msg));
    msg->type = TTS_EVT_SPEAK;

    len = (int)strlen(src);
    msg->text = (char *)malloc(len + 1);
    if (!msg->text) {
        free(msg);
        return -1;
    }
    memcpy(msg->text, src, len + 1);

    if (os_q_post(&s_tts_q, msg) != 0) {
        printf("[TTS] post speak event failed\n");
        free(msg->text);
        free(msg);
        return -1;
    }
    return 0;
}

int tts_agent_stop(void)
{
    volc_tts_stop();
    return 0;
}

void tts_agent_set_done_cb(tts_agent_done_cb_t cb)
{
    s_done_cb = cb;
}

/* ------------------------- 自动演示入口 ------------------------- */

#if TTS_AGENT_AUTO_START
static void tts_demo_start_task(void *priv)
{
    (void)priv;

    while (wifi_get_sta_connect_state() != WIFI_STA_NETWORK_STACK_DHCP_SUCC) {
        printf("[TTS] waiting network...\n");
        msleep(1000);
    }
    printf("[TTS] network ready\n");
    msleep(TTS_AGENT_DEMO_DELAY_MS);
    tts_agent_speak(TTS_AGENT_DEFAULT_TEXT);
}

static int tts_demo_init(void)
{
    if (tts_agent_init() != 0) {
        return -1;
    }
    thread_fork("tts_demo_start", 10, 512, 0, NULL, tts_demo_start_task, NULL);
    return 0;
}

late_initcall(tts_demo_init);
#endif
