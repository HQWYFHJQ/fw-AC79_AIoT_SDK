#ifndef __ASR_AGENT_H__
#define __ASR_AGENT_H__

#include "system/includes.h"
#include "web_socket/websocket_api.h"

/* ======================== 用户配置 ======================== */
/* SAUC 服务地址:
 *   bigmodel_async   : 流式返回（推荐）
 *   bigmodel_nostream: 全部音频发完后返回
 */
#define SAUC_WS_URL             "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async"
#define SAUC_RESOURCE_ID        "volc.seedasr.sauc.duration"

/* 新版控制台鉴权: 语音控制台创建的 API Key (X-Api-Key)
 * 默认使用工程已有的火山 API Key；如果语音服务用的是另一个 Key，
 * 直接替换下面的字符串（也可以在编译参数中用 -D 覆盖）。
 */
#define SAUC_API_KEY            ""

/* 每包音频时长(ms)，与 Python demo 的 --seg-duration 一致 */
#define SAUC_SEGMENT_MS         200
/* 发送完最后一包后等待最终结果的超时(ms) */
#define SAUC_RECV_TIMEOUT_MS    30000

/* ==================== 线程 / 队列资源评估 ====================
 * 线程栈单位: word(4 字节)，thread_fork() 的 stk_size 参数。
 *
 * ASR_TASK_STACK_WORDS          : 应用层业务线程（单线程）
 *     任务: 等事件队列 + 建链 + 打开文件 + 分包发送
 *           + 发送/等待间隙分发 ASR 结果事件（事件类型区分任务）
 *     栈需求: TLS 握手(mbedtls) 约 5~6KB，文件/协议栈少量
 *     建议: 2048 word = 8KB
 *
 * ASR_RECV_THREAD_STACK_WORDS   : websocket 接收线程（库内部）
 *     任务: mbedtls_ssl_read + 帧解析 + 回调
 *     回调里要做 gzip 解压 + cJSON 解析，栈需求约 4KB
 *     建议: 2048 word = 8KB
 *
 * ASR_HEART_THREAD_STACK_WORDS  : websocket 心跳线程
 *     建议: 512 word = 2KB
 *
 * ASR_EVENT_QUEUE_DEPTH         : 唯一事件队列深度
 *     队列同时承载 START / STOP / RESULT 三类事件
 *     建议: 8（只有最终结果/错误才投递 RESULT）
 *
 * 动态内存(heap)估算:
 *   websocket 上下文      3768 B (实测 sizeof)
 *   websocket 接收缓存     4KB (ASR_WS_RECV_BUF_SIZE，帧更大时库内 realloc)
 *   mbedtls TLS 收发缓冲   约 32KB (库配置 IN/OUT 各 16KB)
 *   CA/证书解析            约 2~4KB
 *   SAUC 打包缓冲          约 6.4KB (每包临时 malloc)
 *   音频读取缓冲           约 6.4KB
 *   gzip 解压 JSON 缓冲    2KB 静态 + 按响应大小临时 malloc（上限 16KB）
 *   结果事件               仅最终结果才投递，4 字节
 *   合计常驻 heap 约 48~52KB，峰值约 58~74KB
 */
#define ASR_TASK_STACK_WORDS            2048
#define ASR_RECV_THREAD_STACK_WORDS     2048
#define ASR_HEART_THREAD_STACK_WORDS    512
#define ASR_EVENT_QUEUE_DEPTH           8

/* websocket 接收缓存（库内部使用，超过会 realloc） */
#define ASR_WS_RECV_BUF_SIZE            (4 * 1024)
/* gzip 解压 JSON 缓冲：小响应走静态缓冲，大响应临时 malloc */
#define ASR_JSON_SMALL_SIZE             (2 * 1024)
#define ASR_JSON_MAX_SIZE               (16 * 1024)

/* ======================== 数据结构 ======================== */

/* 音频参数（由 main.c 解析 WAV / 文件头后填写） */
typedef struct {
    int sample_rate;        /* 采样率, e.g. 16000 */
    int channels;           /* 声道数, e.g. 1 */
    int bits;               /* 位深, e.g. 16 */
    int data_len;           /* 待发送的音频数据字节数 */
    char format[8];         /* "pcm" / "mp3" */
} asr_audio_info_t;

/* SAUC 返回结果（asr.c 解析、二次打包后的事件载荷） */
typedef struct {
    int code;               /* 0: 正常; 非 0: 服务端错误码 */
    int is_last;            /* 1: 最后一包 */
    int event;              /* 服务端 event 字段 */
    char text[1024];        /* 识别文本 */
} asr_result_t;

/* websocket 数据回调（运行在 websocket recv 线程） */
typedef void (*asr_recv_cb_t)(const u8 *data, int len, u8 ws_type);

/* ===================== asr.c: websocket 接口 ===================== */

/* 建立 websocket 连接（内部完成 init + TLS 握手 + 启动收发/心跳线程） */
int asr_ws_open(const char *url, asr_recv_cb_t cb);

/* 发送首帧: full client request (JSON + gzip)
 * 成功后 *seq 自增，供后续音频帧使用
 */
int asr_ws_send_full_request(int *seq, const asr_audio_info_t *info);

/* 发送后续音频帧 (audio-only request)
 * is_last=1 时自动使用负 seq + NEG_WITH_SEQUENCE
 */
int asr_ws_send_audio(int seq, const u8 *data, int len, int is_last);

/* 关闭 websocket 并释放资源 */
void asr_ws_close(void);

/* 连接是否已建立 */
int asr_ws_is_connected(void);

/* ================== asr.c: 协议加解密接口 ================== */

/* gzip 压缩 / 解压，返回实际字节数，<0 失败 */
int asr_gzip_compress(const u8 *src, int src_len, u8 *dst, int dst_cap);
int asr_gzip_decompress(const u8 *src, int src_len, u8 *dst, int dst_cap);

/* 解析 SAUC server response（header + gzip + JSON），填充 asr_result_t
 * 返回 0: 解析成功; -1: 报文非法
 */
int asr_proto_parse_response(const u8 *msg, int len, asr_result_t *out);

/* ================== main.c: 应用层接口 ================== */

/* 初始化消息队列与线程（重复调用安全） */
int asr_agent_init(void);

/* 把"开始识别某个音频文件"投递到 websocket 线程的事件队列 */
int asr_agent_start(const char *file_path);

/* 请求停止当前识别（websocket 线程处理） */
int asr_agent_stop(void);

/*
 * 识别完成回调（在 asr 业务线程执行）。
 * text: 最终识别文本，可能为空串；code: 0 成功，非 0 为服务端/本地错误码。
 * 一次 asr_agent_start() 只会回调一次。
 */
typedef void (*asr_agent_result_cb_t)(const char *text, int code);

/* 注册识别完成回调，传 NULL 取消。建议在 asr_agent_start() 前设置 */
void asr_agent_set_result_cb(asr_agent_result_cb_t cb);

#endif /* __ASR_AGENT_H__ */
