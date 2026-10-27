/*
 * Volc Voice Agent —— 语音/文本链路总调度
 *
 * 三种应用逻辑，由文件顶部的 VOLC_AGENT_* 宏选择(互斥，只能打开一个):
 *
 * 1. VOLC_AGENT_ASR_CHAT (默认关闭)
 *      mic -> VAD 录音 -> ASR -> chat -> 串口打印回答
 *
 * 2. VOLC_AGENT_ASR_CHAT_TTS (默认开启)
 *      mic -> VAD 录音 -> ASR -> chat -> tts_agent -> DAC 播放回答
 *
 * 3. VOLC_AGENT_CHAT_TTS (默认关闭)
 *      文本输入 -> chat -> tts_agent -> DAC 播放回答
 *      文本由 volc_agent_chat() 输入；VOLC_AGENT_CHAT_AUTO_START 打开时
 *      联网后自动用默认问题演示一次。
 *
 * 说明:
 *   ASR 启动是非阻塞的，识别结果通过 asr_agent 回调投递到同一个事件队列，
 *   因此不再单独创建 volc_asr_task，音频采集/事件分发保持单线程。
 *
 * 音频参数:
 *   mic 采集 16000 Hz / 单声道 / 16bit PCM，与 asr_agent 的 WAV 解析
 *   (16k/16bit/mono)以及 ASR 上传参数保持一致；chat_agent 只处理文本；
 *   tts_agent 的 PCM 参数见 tts_agent/volc_tts.h。
 *
 * 资源占用（估算，不含 ASR/CHAT/TTS 自身的 TLS/curl 峰值）:
 *   ASR_CHAT     : 线程栈 12KB + PCM cbuffer 16KB + 待识别录音队列
 *   ASR_CHAT_TTS : 在 ASR_CHAT 基础上增加 TTS 播放缓存(见 tts_agent)
 *   CHAT_TTS     : 只有事件队列，不占用麦克风/录音相关资源
 */

/* ======================== 应用模式选择 ======================== */

#define VOLC_AGENT_ASR_CHAT             0
#define VOLC_AGENT_ASR_CHAT_TTS         0
#define VOLC_AGENT_CHAT_TTS             1

#if (VOLC_AGENT_ASR_CHAT + VOLC_AGENT_ASR_CHAT_TTS + VOLC_AGENT_CHAT_TTS) != 1
#error "exactly one VOLC_AGENT_* application mode must be enabled"
#endif


#include "system/includes.h"
#include "app_config.h"
#include "wifi/wifi_connect.h"
#include "chat_agent/chat_completion.h"
#include "server/audio_server.h"
#include "server/server_core.h"
#include "generic/circular_buf.h"
#include "fs/fs.h"
#include "asr_agent/asr.h"
#include "tts_agent/volc_tts.h"

#include <stdlib.h>
#include <string.h>

/* ======================== 线程 / 队列配置 ======================== */
#define VOLC_TASK_STACK_WORDS           3072    /* 12KB: 录音 + 事件分发 */
#define VOLC_EVENT_QUEUE_DEPTH          8
#define VOLC_POLL_MS                    10

/* 录音文件路径长度，事件结构体在所有模式下都会用到 */
#define VOLC_RECORD_PATH_LEN            128

#if (VOLC_AGENT_ASR_CHAT || VOLC_AGENT_ASR_CHAT_TTS)
/* ======================== 音频 / 录音配置 ======================== */
#define VOLC_AUDIO_SAMPLE_RATE          16000
#define VOLC_AUDIO_CHANNELS             1
#define VOLC_AUDIO_BITS                 16
#define VOLC_AUDIO_FRAME_MS             20
#define VOLC_AUDIO_GAIN                 100     /* ADC 增益 0~100 */
/* VAD 阈值单位是"持续时间"，不是能量值：
 * start 越大越能过滤短噪声，但开口后丢的语音也越多；
 * stop 越大越不容易把一句话从中间截断。 */
#define VOLC_AUDIO_VAD_START_MS         600     /* 连续说话 600ms 判定开始 */
#define VOLC_AUDIO_VAD_STOP_MS          800     /* 连续静音 800ms 判定结束 */
#define VOLC_AUDIO_DNS_ENABLE           0       /* 1: 打开降噪，占用额外内存/CPU */

#ifdef CONFIG_AEC_ENC_ENABLE
/* AEC 使能位，定义与 bt_decode.c / audio_input.c 保持一致 */
#define AEC_EN                          BIT(0)
#define NLP_EN                          BIT(1)
#define ANS_EN                          BIT(2)
#define AEC_MODE_ADVANCE                (AEC_EN | NLP_EN | ANS_EN)
#endif

#define VOLC_AUDIO_FRAME_SIZE \
    (VOLC_AUDIO_SAMPLE_RATE / 1000 * VOLC_AUDIO_FRAME_MS * (VOLC_AUDIO_BITS / 8) * VOLC_AUDIO_CHANNELS)
#define VOLC_AUDIO_CBUF_SIZE            (16 * 1024)     /* 约 0.5 秒 PCM 缓冲 */
#define VOLC_AUDIO_READ_SIZE            1024

/* 跟随机型配置选择 MIC ADC 通道；未定义时与其它 LLM 录音模块一致用 BIT(1) */
#ifndef CONFIG_AISP_MIC0_ADC_CHANNEL
#define CONFIG_AISP_MIC0_ADC_CHANNEL    1
#endif
#define VOLC_MIC_ADC_CHANNEL_BIT        BIT(CONFIG_AISP_MIC0_ADC_CHANNEL)

/* VAD stop 后等待最后一帧再落盘 */
#define VOLC_RECORD_TAIL_MS             (VOLC_AUDIO_FRAME_MS + 10)
#define VOLC_MAX_RECORD_MS              30000           /* 单次最长录音，防 VAD 不触发 stop */
#define VOLC_MIN_ASR_BYTES              3200            /* 小于 100ms 的音频不送识别 */
#define VOLC_WAV_HEADER_SIZE            44

/* 文件系统自带自动编号：文件名里的 *** 会被替换成未占用的序号，
 * 用 "w+" 创建即可，不需要自己扫描文件名，也不会覆盖旧文件。
 * 实际序号通过 get_last_num() 取回，拼出完整路径给 ASR 使用。 */
#define VOLC_RECORD_PATH_PATTERN        CONFIG_ROOT_PATH "volc_***.wav"

#define VOLC_ASR_TIMEOUT_MS             60000   /* ASR 未回调的超时保护 */
#define VOLC_PENDING_ASR_MAX            4       /* ASR 忙时最多缓存的待识别录音数 */
#endif

#if VOLC_AGENT_CHAT_TTS
/* 文本输入演示: 联网后自动发一句话，验证 chat -> tts 链路 */
#define VOLC_AGENT_CHAT_AUTO_START      1
#define VOLC_AGENT_CHAT_DELAY_MS        3000
#define VOLC_AGENT_CHAT_DEFAULT_TEXT    "你好。请用一句话介绍你自己。"
#endif

/* ======================== 数据结构 ======================== */

enum {
    VOLC_EVT_VAD_START = 1,
    VOLC_EVT_VAD_STOP,
    VOLC_EVT_AUDIO_ERR,
    VOLC_EVT_ASR_DONE,
    VOLC_EVT_CHAT_DONE,
    VOLC_EVT_TEXT_INPUT,                /* CHAT_TTS: 外部输入的文本 */
    VOLC_EVT_TTS_DONE,                  /* TTS 播放结束 */
};

typedef struct {
    int type;
    int code;
    char *text;                         /* ASR/CHAT/TEXT 结果，malloc；其它事件为 NULL */
    char path[VOLC_RECORD_PATH_LEN];    /* VAD_STOP 时携带录音文件路径 */
} volc_msg_t;

/* ------------------------- 全局状态 ------------------------- */

static OS_QUEUE s_volc_q;               /* volc_agent_task 事件队列 */

#if (VOLC_AGENT_ASR_CHAT || VOLC_AGENT_ASR_CHAT_TTS)
static struct server *s_enc_server;
static u8 s_enc_opened;
static u8 *s_pcm_pool;
static cbuffer_t s_pcm_cbuf;
static u8 s_pcm_read_buf[VOLC_AUDIO_READ_SIZE];

#ifdef CONFIG_AEC_ENC_ENABLE
/* 音频服务只保存指针，不拷贝，必须保持到编码器关闭 */
static struct aec_s_attr s_aec_param;
#endif

static volatile u8 s_recording;
static FILE *s_rec_fp;
static int s_rec_bytes;
static int s_rec_ms;
static u8 s_rec_failed;
static char s_record_path[VOLC_RECORD_PATH_LEN];
static u8 s_wav_hdr[VOLC_WAV_HEADER_SIZE];

/* ASR 串行化: 同一时刻只跑一句；忙时把新录音按顺序排队 */
static volatile u8 s_asr_busy;
static int s_asr_ms;
static char s_pending_asr[VOLC_PENDING_ASR_MAX][VOLC_RECORD_PATH_LEN];
static u8 s_pending_head;
static u8 s_pending_tail;
static u8 s_pending_cnt;

/* device_mount.c 提供: SD 在线 + 挂载成功返回 true */
extern int storage_device_ready(void);

/* 等待 SD 挂载完成；超时返回 -1 */
static int volc_wait_storage(int timeout_ms)
{
    int i;

    for (i = 0; i < timeout_ms / 100; i++) {
        if (storage_device_ready()) {
            return 0;
        }
        msleep(100);
    }
    return -1;
}

/* ------------------------- 小工具 ------------------------- */

static void volc_wr_le16(u8 *p, u16 v)
{
    p[0] = v & 0xff;
    p[1] = (v >> 8) & 0xff;
}

static void volc_wr_le32(u8 *p, u32 v)
{
    p[0] = v & 0xff;
    p[1] = (v >> 8) & 0xff;
    p[2] = (v >> 16) & 0xff;
    p[3] = (v >> 24) & 0xff;
}

/* 生成 44 字节 PCM WAV 头 */
static void volc_wav_header(u8 *h, u32 data_len)
{
    u32 byte_rate = VOLC_AUDIO_SAMPLE_RATE * VOLC_AUDIO_CHANNELS * (VOLC_AUDIO_BITS / 8);
    u16 block_align = VOLC_AUDIO_CHANNELS * (VOLC_AUDIO_BITS / 8);

    memcpy(h, "RIFF", 4);
    volc_wr_le32(h + 4, 36 + data_len);
    memcpy(h + 8, "WAVE", 4);
    memcpy(h + 12, "fmt ", 4);
    volc_wr_le32(h + 16, 16);
    volc_wr_le16(h + 20, 1);            /* PCM */
    volc_wr_le16(h + 22, VOLC_AUDIO_CHANNELS);
    volc_wr_le32(h + 24, VOLC_AUDIO_SAMPLE_RATE);
    volc_wr_le32(h + 28, byte_rate);
    volc_wr_le16(h + 32, block_align);
    volc_wr_le16(h + 34, VOLC_AUDIO_BITS);
    memcpy(h + 36, "data", 4);
    volc_wr_le32(h + 40, data_len);
}

#endif

/* ------------------------- 事件投递 ------------------------- */

static int volc_post_msg(int type, const char *path, const char *text, int code)
{
    volc_msg_t *msg = (volc_msg_t *)malloc(sizeof(*msg));

    if (!msg) {
        return -1;
    }
    memset(msg, 0, sizeof(*msg));
    msg->type = type;
    msg->code = code;

    if (path && path[0]) {
        strncpy(msg->path, path, sizeof(msg->path) - 1);
    }
    if (text) {
        int len = (int)strlen(text);

        msg->text = (char *)malloc(len + 1);
        if (!msg->text) {
            free(msg);
            return -1;
        }
        memcpy(msg->text, text, len + 1);
    }

    if (os_q_post(&s_volc_q, msg) != 0) {
        printf("[VOLC] post event %d failed\n", type);
        free(msg->text);
        free(msg);
        return -1;
    }
    return 0;
}

#if (VOLC_AGENT_ASR_CHAT || VOLC_AGENT_ASR_CHAT_TTS)
/* VAD 回调运行在 app_core，只做轻量的事件投递 */
static void volc_enc_event_handler(void *priv, int argc, int *argv)
{
    (void)priv;
    (void)argc;

    switch (argv[0]) {
    case AUDIO_SERVER_EVENT_SPEAK_START:
        printf("[VOLC] vad start\n");
        volc_post_msg(VOLC_EVT_VAD_START, NULL, NULL, 0);
        break;
    case AUDIO_SERVER_EVENT_SPEAK_STOP:
        printf("[VOLC] vad stop, file=%s\n",
               s_record_path[0] ? s_record_path : "(none)");
        volc_post_msg(VOLC_EVT_VAD_STOP, s_record_path, NULL, 0);
        break;
    case AUDIO_SERVER_EVENT_END:
    case AUDIO_SERVER_EVENT_ERR:
        printf("[VOLC] audio server event %d\n", argv[0]);
        volc_post_msg(VOLC_EVT_AUDIO_ERR, NULL, NULL, argv[0]);
        break;
    default:
        break;
    }
}

/* ------------------------- 音频采集 ------------------------- */

/* 编码器回调: 只写入 cbuffer，不做耗时操作 */
static int volc_audio_fwrite(void *file, void *data, u32 len)
{
    cbuffer_t *cbuf = (cbuffer_t *)file;

    if (cbuf_write(cbuf, data, len) == 0) {
        /* 上层取数不及时就丢弃旧数据，避免编码器被阻塞 */
        cbuf_clear(cbuf);
    }
    return len;
}

static const struct audio_vfs_ops s_volc_audio_vfs_ops = {
    .fwrite = volc_audio_fwrite,
};

static void volc_audio_close(void)
{
    union audio_req req = { 0 };

    if (s_enc_server) {
        if (s_enc_opened) {
            req.enc.cmd = AUDIO_ENC_CLOSE;
            server_request(s_enc_server, AUDIO_REQ_ENC, &req);
            s_enc_opened = 0;
        }
        server_close(s_enc_server);
        s_enc_server = NULL;
    }
    if (s_pcm_pool) {
        free(s_pcm_pool);
        s_pcm_pool = NULL;
    }
}

static int volc_audio_open(void)
{
    union audio_req req;
    int err;
#ifdef CONFIG_AEC_ENC_ENABLE
    extern void get_cfg_file_aec_config(struct aec_s_attr * aec_param);
#endif

    s_enc_server = server_open("audio_server", "enc");
    if (!s_enc_server) {
        printf("[VOLC] open audio enc server failed\n");
        return -1;
    }
    server_register_event_handler_to_task(s_enc_server, NULL,
                                          volc_enc_event_handler, "app_core");

    s_pcm_pool = (u8 *)malloc(VOLC_AUDIO_CBUF_SIZE);
    if (!s_pcm_pool) {
        printf("[VOLC] no mem for pcm buffer\n");
        volc_audio_close();
        return -1;
    }
    cbuf_init(&s_pcm_cbuf, s_pcm_pool, VOLC_AUDIO_CBUF_SIZE);

    memset(&req, 0, sizeof(req));
    req.enc.cmd = AUDIO_ENC_OPEN;
    req.enc.channel = VOLC_AUDIO_CHANNELS;
    req.enc.channel_bit_map = VOLC_MIC_ADC_CHANNEL_BIT;
    req.enc.frame_size = VOLC_AUDIO_FRAME_SIZE;
    req.enc.output_buf_len = req.enc.frame_size * 3;
    req.enc.volume = VOLC_AUDIO_GAIN;
    req.enc.sample_rate = VOLC_AUDIO_SAMPLE_RATE;
    req.enc.format = "pcm";
    req.enc.sample_source = "mic";
    req.enc.vfs_ops = &s_volc_audio_vfs_ops;
    req.enc.file = (FILE *)&s_pcm_cbuf;
    req.enc.use_vad = 1;
    req.enc.vad_auto_refresh = 1;
    req.enc.vad_start_threshold = VOLC_AUDIO_VAD_START_MS;
    req.enc.vad_stop_threshold = VOLC_AUDIO_VAD_STOP_MS;
#if VOLC_AUDIO_DNS_ENABLE
    req.enc.dns_enable = 1;
#endif

#ifdef CONFIG_AEC_ENC_ENABLE
    memset(&s_aec_param, 0, sizeof(s_aec_param));
    /* 读取板级 AEC 参数；配置里没开时强制打开 AEC+NLP+ANS */
    get_cfg_file_aec_config(&s_aec_param);
    if (s_aec_param.EnableBit == 0) {
        s_aec_param.EnableBit = AEC_MODE_ADVANCE;
    }

#if defined CONFIG_ALL_ADC_CHANNEL_OPEN_ENABLE && \
    defined CONFIG_AISP_LINEIN_ADC_CHANNEL && \
    defined CONFIG_AEC_LINEIN_CHANNEL_ENABLE
    /* 硬件回采：额外打开 LINEIN 参考通道 */
    s_aec_param.output_way = 1;
    req.enc.channel_bit_map |= BIT(CONFIG_AISP_LINEIN_ADC_CHANNEL);
    if (CONFIG_AISP_LINEIN_ADC_CHANNEL < CONFIG_PHONE_CALL_ADC_CHANNEL) {
        req.enc.ch_data_exchange = 1;
    }
#else
    /* 软件回采：使用 DAC 播放数据作为参考 */
    s_aec_param.output_way = 0;
#endif

    if (VOLC_AUDIO_SAMPLE_RATE == 16000) {
        s_aec_param.wideband = 1;
        s_aec_param.hw_delay_offset = 50;
    } else {
        s_aec_param.wideband = 0;
        s_aec_param.hw_delay_offset = 75;
    }

    req.enc.aec_enable = 1;
    req.enc.aec_attr = &s_aec_param;
    printf("[VOLC] aec enabled, output_way=%d\n", s_aec_param.output_way);
#endif

    err = server_request(s_enc_server, AUDIO_REQ_ENC, &req);
    if (err != 0) {
        printf("[VOLC] audio enc open failed: %d\n", err);
        volc_audio_close();
        return -1;
    }
    s_enc_opened = 1;

    printf("[VOLC] mic opened: %dHz/%dch/%dbit, frame=%d bytes\n",
           VOLC_AUDIO_SAMPLE_RATE, VOLC_AUDIO_CHANNELS, VOLC_AUDIO_BITS,
           VOLC_AUDIO_FRAME_SIZE);
    return 0;
}

/* 把 cbuffer 中的 PCM 取出；录音时写卡，否则丢弃 */
static void volc_audio_drain(void)
{
    int avail;
    int n;

    if (!s_enc_server) {
        return;     /* TTS 播放期间麦克风已关闭 */
    }

    while ((avail = (int)cbuf_get_data_size(&s_pcm_cbuf)) > 0) {
        n = avail > (int)sizeof(s_pcm_read_buf) ? (int)sizeof(s_pcm_read_buf) : avail;
        n = cbuf_read(&s_pcm_cbuf, s_pcm_read_buf, n);
        if (n <= 0) {
            break;
        }
        if (!s_recording || !s_rec_fp || s_rec_failed) {
            continue;
        }
        if (fwrite(s_pcm_read_buf, 1, n, s_rec_fp) != n) {
            printf("[VOLC] write record file failed\n");
            s_rec_failed = 1;
            continue;
        }
        s_rec_bytes += n;
    }
}

/* ------------------------- 录音文件 ------------------------- */

/* 交给文件系统的 *** 自动编号，返回实际路径 */
static FILE *volc_create_record_file(char *path, int size)
{
    FILE *fp = NULL;
    int num;

    fp = fopen(VOLC_RECORD_PATH_PATTERN, "w+");
    if (!fp) {
        return NULL;
    }

    num = get_last_num();
    snprintf(path, size, CONFIG_ROOT_PATH "volc_%03d.wav", num);
    return fp;
}

static void volc_record_start(void)
{
    if (!s_enc_server) {
        printf("[VOLC] mic closed, ignore vad start\n");
        return;
    }
    if (s_recording) {
        printf("[VOLC] already recording, ignore vad start\n");
        return;
    }

    /* 卡未挂载时直接跳过；清空路径，避免下一次 stop 带出上一段的旧路径 */
    if (!storage_device_ready()) {
        printf("[VOLC] sd not ready (%s), skip record\n", CONFIG_STORAGE_PATH);
        s_record_path[0] = '\0';
        return;
    }

    s_rec_fp = volc_create_record_file(s_record_path, sizeof(s_record_path));
    if (!s_rec_fp) {
        printf("[VOLC] create record file failed, check sd card\n");
        s_record_path[0] = '\0';
        return;
    }
    printf("record_path: %s\n", s_record_path);
    volc_wav_header(s_wav_hdr, 0);
    if (fwrite(s_wav_hdr, 1, sizeof(s_wav_hdr), s_rec_fp) != (int)sizeof(s_wav_hdr)) {
        printf("[VOLC] write wav header failed\n");
        fclose(s_rec_fp);
        s_rec_fp = NULL;
        return;
    }

    s_rec_bytes = 0;
    s_rec_ms = 0;
    s_rec_failed = 0;
    s_recording = 1;
    printf("[VOLC] record start: %s\n", s_record_path);
}

static void volc_asr_start(const char *path);

/* 停止写卡: 等最后一帧 -> 回填 WAV 头 -> 关闭 -> 投递 ASR */
static void volc_record_stop(int is_timeout)
{
    char path[VOLC_RECORD_PATH_LEN];
    int data_len;

    if (!s_recording) {
        return;
    }

    msleep(VOLC_RECORD_TAIL_MS);
    volc_audio_drain();

    data_len = s_rec_bytes;
    strncpy(path, s_record_path, sizeof(path) - 1);
    path[sizeof(path) - 1] = '\0';

    if (s_rec_fp) {
        /* 只回填两个长度字段，和 SDK wav_pkg_close() 的做法一致；
         * 注意最后必须 seek 回文件末尾再 close，否则 JLFAT 会把
         * 目录项 fsize 同步成当前写指针位置（只剩 44 字节头）。 */
        u32 riff_size = 36 + (u32)data_len;
        u32 pcm_size = (u32)data_len;

        fseek(s_rec_fp, 4, SEEK_SET);
        fwrite(&riff_size, 1, 4, s_rec_fp);
        fseek(s_rec_fp, VOLC_WAV_HEADER_SIZE - 4, SEEK_SET);
        fwrite(&pcm_size, 1, 4, s_rec_fp);
        fseek(s_rec_fp, VOLC_WAV_HEADER_SIZE + data_len, SEEK_SET);

        printf("[VOLC] wav patched: data=%d, file=%d\n",
               data_len, flen(s_rec_fp));
        fclose(s_rec_fp);
        s_rec_fp = NULL;
    }
    s_recording = 0;

    printf("[VOLC] record stop: %s, pcm=%d bytes%s\n", path, data_len,
           is_timeout ? " (timeout)" : "");

    if (s_rec_failed) {
        printf("[VOLC] record had write error, skip asr\n");
        return;
    }
    if (data_len < VOLC_MIN_ASR_BYTES) {
        printf("[VOLC] audio too short, skip asr\n");
        return;
    }
    volc_asr_start(path);
}

/* ------------------------- ASR 启动 / 回调 ------------------------- */

/* asr_agent 完成回调: 运行在 asr_agent_task，直接投事件给 volc_agent_task */
static void volc_asr_result_cb(const char *text, int code)
{
    printf("[VOLC] asr done: code=%d text=%s\n", code, text ? text : "");
    volc_post_msg(VOLC_EVT_ASR_DONE, NULL, text ? text : "", code);
}

/* 待识别录音队列：ASR 忙时按顺序缓存，避免只保留最后一句 */
static void volc_pending_clear(void)
{
    s_pending_head = 0;
    s_pending_tail = 0;
    s_pending_cnt = 0;
}

/* 队列满时丢弃最早的一句，保留最近的对话 */
static int volc_pending_push(const char *path)
{
    int idx;

    if (!path || !path[0]) {
        return -1;
    }
    if (s_pending_cnt >= VOLC_PENDING_ASR_MAX) {
        s_pending_head = (s_pending_head + 1) % VOLC_PENDING_ASR_MAX;
        s_pending_cnt--;
        printf("[VOLC] pending asr full, drop oldest\n");
    }

    idx = s_pending_tail;
    strncpy(s_pending_asr[idx], path, VOLC_RECORD_PATH_LEN - 1);
    s_pending_asr[idx][VOLC_RECORD_PATH_LEN - 1] = '\0';
    s_pending_tail = (s_pending_tail + 1) % VOLC_PENDING_ASR_MAX;
    s_pending_cnt++;
    return 0;
}

static int volc_pending_pop(char *path)
{
    if (s_pending_cnt == 0) {
        return -1;
    }

    strncpy(path, s_pending_asr[s_pending_head], VOLC_RECORD_PATH_LEN - 1);
    path[VOLC_RECORD_PATH_LEN - 1] = '\0';
    s_pending_head = (s_pending_head + 1) % VOLC_PENDING_ASR_MAX;
    s_pending_cnt--;
    return 0;
}

/* 非阻塞启动一次识别；ASR 忙时排队，等当前结束再依次启动 */
static void volc_asr_start(const char *path)
{
    if (s_asr_busy) {
        volc_pending_push(path);
        printf("[VOLC] asr busy, pending: %s\n", path);
        return;
    }

    printf("[VOLC] asr request: %s\n", path);
    s_asr_busy = 1;
    s_asr_ms = 0;
    if (asr_agent_start(path) != 0) {
        printf("[VOLC] asr start failed\n");
        s_asr_busy = 0;
        s_asr_ms = 0;
    }
}

#endif

/* ------------------------- 麦克风 / TTS 衔接 ------------------------- */

#if (VOLC_AGENT_ASR_CHAT || VOLC_AGENT_ASR_CHAT_TTS)
/*
 * TTS 播放期间关闭麦克风编码器：
 *   - 避免 ADC(16k) 与 DAC(48k) 同时工作互相影响；
 *   - 避免喇叭声音被麦克风拾取触发 VAD/自激。
 * 播放结束后由 TTS 完成回调通知重新打开。
 */
static void volc_mic_pause(void)
{
    if (!s_enc_server || s_recording) {
        return;
    }
    printf("[VOLC] mic pause for tts\n");
    volc_audio_close();
}

static void volc_mic_resume(void)
{
    if (s_enc_server) {
        return;
    }
    printf("[VOLC] mic resume after tts\n");
    if (volc_audio_open() != 0) {
        printf("[VOLC] mic reopen failed\n");
    }
}
#endif

#if (VOLC_AGENT_ASR_CHAT_TTS || VOLC_AGENT_CHAT_TTS)
/* 运行在 tts_agent_task，仅投递事件，真正的恢复动作在 volc_agent_task 执行 */
static void volc_tts_done_cb(void)
{
    volc_post_msg(VOLC_EVT_TTS_DONE, NULL, NULL, 0);
}
#endif

/* ------------------------- CHAT 回调 ------------------------- */

/* 运行在 chat_agent_task，文本只在本回调期间有效，需拷贝后转发 */
static void volc_chat_result_cb(const char *text, int ret)
{
    printf("[VOLC] chat done, ret=%d\n", ret);
    volc_post_msg(VOLC_EVT_CHAT_DONE, NULL, text ? text : "", ret);
}

/* ------------------------- 主任务 ------------------------- */

static void volc_handle_msg(volc_msg_t *msg)
{
    switch (msg->type) {
#if (VOLC_AGENT_ASR_CHAT || VOLC_AGENT_ASR_CHAT_TTS)
    case VOLC_EVT_VAD_START:
        volc_record_start();
        break;
    case VOLC_EVT_VAD_STOP:
        if (msg->path[0]) {
            strncpy(s_record_path, msg->path, sizeof(s_record_path) - 1);
            s_record_path[sizeof(s_record_path) - 1] = '\0';
        }
        volc_record_stop(0);
        break;
    case VOLC_EVT_AUDIO_ERR:
        printf("[VOLC] audio server error event %d\n", msg->code);
        break;
    case VOLC_EVT_ASR_DONE: {
        char next[VOLC_RECORD_PATH_LEN];

        if (!s_asr_busy) {
            /* 超时后 asr_agent 可能还会回调一次，忽略迟到的结果 */
            printf("[VOLC] stale asr result, ignore\n");
            break;
        }
        s_asr_busy = 0;
        s_asr_ms = 0;

        if (msg->code == 0 && msg->text && msg->text[0]) {
            printf("[VOLC] asr text: %s\n", msg->text);
            if (chat_agent_request(msg->text) != 0) {
                printf("[VOLC] chat request failed\n");
            }
        } else {
            printf("[VOLC] asr failed or empty, code=%d\n", msg->code);
        }

        /* ASR 期间又录了几句，当前结束后按顺序继续识别 */
        if (volc_pending_pop(next) == 0) {
            volc_asr_start(next);
        }
        break;
    }
#endif
    case VOLC_EVT_TEXT_INPUT:
        if (msg->text && msg->text[0]) {
            printf("[VOLC] chat request: %s\n", msg->text);
            if (chat_agent_request(msg->text) != 0) {
                printf("[VOLC] chat request failed\n");
            }
        }
        break;
    case VOLC_EVT_CHAT_DONE:
        if (msg->code != CHAT_OK || !msg->text || !msg->text[0]) {
            printf("[VOLC] chat failed or empty, code=%d\n", msg->code);
            break;
        }
#if (VOLC_AGENT_ASR_CHAT_TTS || VOLC_AGENT_CHAT_TTS)
        printf("[VOLC] chat reply -> tts: %s\n", msg->text);
#if (VOLC_AGENT_ASR_CHAT || VOLC_AGENT_ASR_CHAT_TTS)
        volc_mic_pause();
#endif
        if (tts_agent_speak(msg->text) != 0) {
            printf("[VOLC] tts speak failed\n");
#if (VOLC_AGENT_ASR_CHAT || VOLC_AGENT_ASR_CHAT_TTS)
            volc_mic_resume();
#endif
        }
#else
        printf("[VOLC] chat reply (ret=%d):\n%s\n", msg->code, msg->text);
#endif
        break;
    case VOLC_EVT_TTS_DONE:
#if (VOLC_AGENT_ASR_CHAT || VOLC_AGENT_ASR_CHAT_TTS)
        volc_mic_resume();
#endif
        break;
    default:
        break;
    }
}

int volc_agent_init(void);

/* 文本输入入口(CHAT_TTS): 把文本交给 chat_agent，回复由回调驱动 TTS */
int volc_agent_chat(const char *text)
{
    if (!text || !text[0]) {
        return -1;
    }
    if (volc_agent_init() != 0) {
        return -1;
    }
    return volc_post_msg(VOLC_EVT_TEXT_INPUT, NULL, text, 0);
}

static void volc_agent_task(void *priv)
{
    int msg[16];

    (void)priv;

    while (wifi_get_sta_connect_state() != WIFI_STA_NETWORK_STACK_DHCP_SUCC) {
        printf("[VOLC] waiting network...\n");
        msleep(1000);
    }
    printf("[VOLC] network ready\n");

#if (VOLC_AGENT_ASR_CHAT || VOLC_AGENT_ASR_CHAT_TTS)
    /* 开机 SD 挂载是异步的，这里给 3 秒窗口；未挂载也继续开 mic，
     * 之后插卡，下一次 VAD start 会重新检测并挂载。 */
    if (volc_wait_storage(3000) != 0) {
        printf("[VOLC] sd not ready (%s), record disabled until card mounted\n",
               CONFIG_STORAGE_PATH);
    } else {
        printf("[VOLC] sd ready: %s\n", CONFIG_STORAGE_PATH);
    }

retry_audio:
    if (volc_audio_open() != 0) {
        printf("[VOLC] retry audio in 2s\n");
        msleep(2000);
        goto retry_audio;
    }
#endif

#if VOLC_AGENT_CHAT_TTS && VOLC_AGENT_CHAT_AUTO_START
    /* 文本输入演示: 联网后自动发一句话，验证 chat -> tts 链路 */
    msleep(VOLC_AGENT_CHAT_DELAY_MS);
    volc_agent_chat(VOLC_AGENT_CHAT_DEFAULT_TEXT);
#endif

    while (1) {
        while (os_q_accept(&s_volc_q, msg) == 0) {
            volc_msg_t *ev = (volc_msg_t *)msg[0];

            if (!ev) {
                continue;
            }
            volc_handle_msg(ev);
            free(ev->text);
            free(ev);
        }

#if (VOLC_AGENT_ASR_CHAT || VOLC_AGENT_ASR_CHAT_TTS)
        volc_audio_drain();

        if (s_recording) {
            s_rec_ms += VOLC_POLL_MS;
            if (s_rec_ms >= VOLC_MAX_RECORD_MS) {
                volc_record_stop(1);
            }
        }

        /* ASR 超时保护：asr_agent 没有回调时主动结束本次识别 */
        if (s_asr_busy) {
            s_asr_ms += VOLC_POLL_MS;
            if (s_asr_ms >= VOLC_ASR_TIMEOUT_MS) {
                printf("[VOLC] asr timeout\n");
                asr_agent_stop();
                s_asr_busy = 0;
                s_asr_ms = 0;
                volc_pending_clear();
            }
        }
#endif
        msleep(VOLC_POLL_MS);
    }
}

/* ------------------------- 初始化 ------------------------- */

static u8 s_volc_inited;

int volc_agent_init(void)
{
    if (s_volc_inited) {
        return 0;
    }

    if (os_q_create(&s_volc_q, VOLC_EVENT_QUEUE_DEPTH) != 0) {
        printf("[VOLC] create event queue failed\n");
        return -1;
    }

#if (VOLC_AGENT_ASR_CHAT || VOLC_AGENT_ASR_CHAT_TTS)
    asr_agent_set_result_cb(volc_asr_result_cb);
#endif
    chat_agent_set_result_cb(volc_chat_result_cb);
#if (VOLC_AGENT_ASR_CHAT_TTS || VOLC_AGENT_CHAT_TTS)
    if (tts_agent_init() != 0) {
        printf("[VOLC] init tts agent failed\n");
    }
    tts_agent_set_done_cb(volc_tts_done_cb);
#endif

    if (thread_fork("volc_agent_task", 10, VOLC_TASK_STACK_WORDS, 0,
                    NULL, volc_agent_task, NULL) != OS_NO_ERR) {
        printf("[VOLC] fork agent task failed\n");
        os_q_del(&s_volc_q, 1);
        return -1;
    }

    s_volc_inited = 1;

#if VOLC_AGENT_ASR_CHAT
    printf("[VOLC] mode: ASR -> CHAT\n");
#elif VOLC_AGENT_ASR_CHAT_TTS
    printf("[VOLC] mode: ASR -> CHAT -> TTS\n");
#else
    printf("[VOLC] mode: CHAT -> TTS\n");
#endif
    return 0;
}
