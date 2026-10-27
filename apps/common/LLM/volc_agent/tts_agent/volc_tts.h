#ifndef VOLC_TTS_H
#define VOLC_TTS_H

#include "system/includes.h"

/* ======================== 用户配置 ======================== */
/*
 * 火山引擎 HTTP Chunked 单向流式语音合成（TTS 2.0）
 *
 *   POST https://openspeech.bytedance.com/api/v3/tts/unidirectional
 *
 * 请求头:
 *   X-Api-Key / X-Api-Resource-Id / X-Api-Request-Id
 *
 * 响应:
 *   Transfer-Encoding: chunked，每行一个 JSON:
 *   {"code":0,"message":"","data":"<base64 音频>"}
 *   data 为 base64 编码的 PCM/MP3 数据，解码后直接送 DAC。
 */
#define VOLC_TTS_HTTP_URL               "https://openspeech.bytedance.com/api/v3/tts/unidirectional"
#define VOLC_TTS_API_KEY                ""
#define VOLC_TTS_RESOURCE_ID            "seed-tts-2.0"

#define VOLC_TTS_SPEAKER                "zh_female_vv_uranus_bigtts"
#define VOLC_TTS_FORMAT                 "pcm"
#define VOLC_TTS_SAMPLE_RATE            48000
#define VOLC_TTS_CHANNELS               1
#define VOLC_TTS_VOLUME                 20     /* DAC 音量 0~100 */
/* 语速: 取值 [-50, 100]，0 为正常，100 约 2 倍速，-50 约 0.5 倍速 */
#define VOLC_TTS_SPEECH_RATE            (0)

/* ======================== 超时 / 缓冲 ======================== */
#define VOLC_TTS_CONNECT_TIMEOUT_S      10
#define VOLC_TTS_TOTAL_TIMEOUT_S        120     /* 播放是实时的，长文本需要更长超时 */
#define VOLC_TTS_HTTP_BUF_SIZE          4096
/* base64 音频采用流式解码，行缓冲只保存 JSON 头尾，不再缓存整块音频 */
#define VOLC_TTS_LINE_BUF_SIZE          2048
#define VOLC_TTS_PCM_BUF_SIZE           1024    /* 解码后的小块音频缓冲 */
/* 请求结束先等播放缓存排空，再等 audio_server/DAC 内部缓冲播完 */
#define VOLC_TTS_DRAIN_TIMEOUT_MS       3000
#define VOLC_TTS_DRAIN_TAIL_MS          600

#define VOLC_TTS_TASK_STACK_WORDS       4096    /* 16KB: libcurl + TLS 握手 */
#define VOLC_TTS_EVENT_QUEUE_DEPTH      4

/* ======================== 接口 ======================== */

/*
 * 音频数据回调（运行在 tts_agent_task，数据为解码后的 PCM）。
 * 返回 0 继续接收，<0 中止本次请求。
 */
typedef int (*volc_tts_audio_cb_t)(const u8 *data, int len, void *priv);

/*
 * 同步发起一次 HTTP 流式合成，音频通过 audio_cb 返回。
 * 返回 0 成功；-1 参数/内存/网络错误；-2 被 volc_tts_stop() 中止；
 * -3 服务端返回错误码。
 */
int volc_tts_request(const char *text, volc_tts_audio_cb_t audio_cb, void *priv);

/* 请求中止当前合成，可在其它线程调用 */
void volc_tts_stop(void);

/* ================== main.c: 应用层接口 ================== */

/* 初始化事件队列与业务线程（重复调用安全） */
int tts_agent_init(void);

/* 请求合成一段文本并在 DAC 上播放；text 为空时使用默认演示文本 */
int tts_agent_speak(const char *text);

/* 停止当前合成与播放 */
int tts_agent_stop(void);

/*
 * 一次合成+播放结束回调（运行在 tts_agent_task）。
 * 应用层可用它在播放结束后恢复被暂停的麦克风等资源。
 */
typedef void (*tts_agent_done_cb_t)(void);

void tts_agent_set_done_cb(tts_agent_done_cb_t cb);

#endif /* VOLC_TTS_H */
