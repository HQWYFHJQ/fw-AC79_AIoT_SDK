#ifndef CHAT_COMPLETION_H
#define CHAT_COMPLETION_H

/* ======================== 用户配置 ======================== */
/*
 * Feedcoop Agent 流式对话接口（对应 Python 文件 chat_completion_apikey.py）
 *
 *   POST https://open.feedcoopapi.com/agent_api/agent/chat/completion
 *   Authorization: Bearer <api_key>
 *   ServiceName: ask_echo
 *   body: {"bot_id":"...","messages":[...],"stream":true}
 *
 * 响应为 SSE 流：
 *   data: {"choices":[{"delta":{"content":"..."}}]}
 *   data: [DONE]
 *
 * 使用前必须填写 CHAT_BOT_ID 和 CHAT_API_KEY，也可以直接改用
 * chat_agent_config_t 在运行时传入。
 */
#define CHAT_API_HOST                   "open.feedcoopapi.com"
#define CHAT_API_PATH                   "/agent_api/agent/chat/completion"
#define CHAT_API_URL                    "https://" CHAT_API_HOST CHAT_API_PATH
#define CHAT_API_SERVICE_NAME           "ask_echo"

#define CHAT_BOT_ID                     ""
#define CHAT_API_KEY                    ""

/* ---------------------- 超时 / 缓冲配置 ---------------------- */
#define CHAT_CONNECT_TIMEOUT_S          10      /* TLS + TCP 建链超时 */
#define CHAT_TOTAL_TIMEOUT_S            120     /* 整个流式响应超时 */
#define CHAT_HTTP_BUFFER_SIZE           2048    /* libcurl 单次写回调缓冲 */
#define CHAT_SSE_LINE_SIZE              2048    /* 单行 SSE 最大长度，超出丢弃 */
#define CHAT_FULL_TEXT_SIZE             4096    /* 累计回复文本上限 */
#define CHAT_QUESTION_SIZE              512     /* 单次提问文本上限（需容纳 ASR 文本） */


/* ==================== 线程 / 队列资源 ====================
 * 完整的静态 RAM / 线程栈 / heap 统计见 main.c 顶部注释。
 *
 * CHAT_TASK_STACK_WORDS : 业务线程栈，单位 word(4B)
 *   4096 word = 16KB。libcurl 调用链 + mbedtls 握手 + cJSON 解析，
 *   相比 20KB 版本节省 4KB；若板端出现栈相关异常再调回 5120。
 *
 * CHAT_EVENT_QUEUE_DEPTH : 事件队列深度（REQUEST / STOP）
 */
#define CHAT_TASK_STACK_WORDS           4096
#define CHAT_EVENT_QUEUE_DEPTH          4

/* ======================== 数据结构 ======================== */

/* 接口配置。service_name 为 NULL 时使用 CHAT_API_SERVICE_NAME */
typedef struct {
    const char *bot_id;
    const char *api_key;
    const char *service_name;
} chat_agent_config_t;

/* chat_completion_request() 的返回值 */
enum {
    CHAT_OK = 0,
    CHAT_ERR_PARAM = -1,        /* 参数为空/非法 */
    CHAT_ERR_NET = -2,          /* DNS/TCP/TLS/收发失败 */
    CHAT_ERR_HTTP = -3,         /* HTTP 状态码非 200 */
    CHAT_ERR_SERVER = -4,       /* 服务端返回错误码/错误信息 */
    CHAT_ERR_STOPPED = -5,      /* 被 chat_completion_stop() 中止 */
    CHAT_ERR_MEM = -6,          /* 内存不足 */
    CHAT_ERR_BUSY = -7,         /* 上一轮对话尚未结束 */
};

/* 请求结束后的完整结果 */
typedef struct {
    int code;                   /* 0: 正常; 非 0: 服务端错误码 */
    int is_error;               /* 1: 本次请求失败 */
    int is_last;                /* 1: 收到 [DONE] 或 finish_reason */
    char text[CHAT_FULL_TEXT_SIZE];     /* 累计回复文本 */
} chat_result_t;

/*
 * 流式回调（运行在业务线程，非中断上下文）。
 * delta : 本帧新增文本，可能为空串
 * full  : 截至当前的累计文本
 * is_last: 1 表示本帧之后不会再有内容
 */
typedef void (*chat_stream_cb_t)(const char *delta, const char *full,
                                 int code, int is_last, void *priv);

/* ================== chat_completion.c: HTTPS / SSE 层 ================== */

/* 初始化 libcurl 全局状态，重复调用安全 */
void chat_completion_global_init(void);

/*
 * 发起一次流式对话并同步等待结束。
 * 返回 CHAT_OK 或 CHAT_ERR_*；out 为 NULL 时只走回调。
 */
int chat_completion_request(const chat_agent_config_t *cfg, const char *question,
                            chat_stream_cb_t cb, void *priv, chat_result_t *out);

/* 请求停止当前对话，可在其它线程调用 */
void chat_completion_stop(void);

/* ================== main.c: 应用层接口 ================== */

/* 初始化事件队列与业务线程（重复调用安全） */
int chat_agent_init(void);

/* 投递一次提问到业务线程。question 为 NULL 时使用默认问题 */
int chat_agent_request(const char *question);

/* 请求停止当前对话（业务线程处理） */
int chat_agent_stop(void);

/*
 * 对话完成回调（在 chat_agent_task 线程执行）。
 * text 仅在本次调用期间有效，需要保存时请自行拷贝；ret 为 CHAT_OK 或 CHAT_ERR_*。
 * 注册后应用层不再打印完整回复，由上层自行处理。
 */
typedef void (*chat_agent_result_cb_t)(const char *text, int ret);

void chat_agent_set_result_cb(chat_agent_result_cb_t cb);

#endif /* CHAT_COMPLETION_H */
