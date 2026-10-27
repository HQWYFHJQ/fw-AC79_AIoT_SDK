/*
 * Chat Agent —— 应用层（单线程事件分发）
 *
 * 结构:
 *   chat_completion.c : HTTPS POST + SSE 流式解析 + 增量文本回调
 *   chat_completion.h : 配置宏、数据结构、接口声明、线程/队列资源说明
 *   main.c            : 单一事件队列 + 一个业务线程，通过事件类型分发
 *
 * 事件流:
 *   chat_agent_request(question) / chat_agent_stop()
 *        └─> g_chat_event_q ─> chat_agent_task
 *                               ├─ CHAT_EVT_REQUEST: 同步发起 HTTPS 请求，结束后整段打印
 *                               └─ CHAT_EVT_STOP   : 置停止标志并中止 libcurl
 *
 * 与 asr_agent 的差异:
 *   HTTP 请求是同步短连接，curl_easy_perform() 内部仍在当前线程回调，
 *   所以不需要额外的邮箱/结果事件；SSE 增量文本先累加，结束后一次性输出。
 */

#include "chat_completion.h"

#include "system/includes.h"
#include "wifi/wifi_connect.h"

#include <stdlib.h>
#include <string.h>

/* ==================== CHAT 模块内存占用统计 ====================
 * 数据来源: 结构体 sizeof + SDK 配置 + 代码分析（未做板端实测，
 * 统计方式与 asr_agent/main.c 一致）
 *
 * 一、静态 RAM (.bss / .data，常驻) —— 优化后
 *   s_chat_event_q                 80 B   (OS_QUEUE, 深度 4)
 *   s_chat_config                  12 B   (bot_id/api_key/service 指针)
 *   s_chat_inited/busy/stop_req     3 B
 *   s_chat_curl_inited              4 B
 *   ------------------------------------
 *   合计                       约 0.1 KB
 *   (优化前 s_chat_last_result 4108 B 常驻，已改为请求时 heap 分配)
 *
 * 二、只读数据 (.rodata)
 *   bot_id + api_key              179 B
 *   URL / ServiceName              71 B
 *   日志与 JSON 格式串          约 0.9 KB
 *   ------------------------------------
 *   合计                       约 1.2 KB
 *
 * 三、线程栈 (thread_fork 从 heap 分配，单位 word = 4B) —— 优化后
 *   chat_agent_task   4096 word × 4 = 16.0 KB   (优化前 20.0 KB)
 *   chat_demo_start    512 word × 4 =  2.0 KB   (仅 AUTO_START=1 时创建)
 *
 * 四、heap 动态分配（请求期间） —— 优化后
 *   chat_result_t                 4108 B  (回复累积缓冲，请求结束即释放)
 *   chat_sse_ctx_t                2084 B  (SSE 行缓冲 2048 + 状态/回调)
 *   chat_msg_t                     516 B  (事件消息，持有到请求结束)
 *   请求 JSON + 头链表         约 0.4 KB
 *   cJSON 单帧解析             约 1~2 KB  (逐帧解析后立即释放)
 *   libcurl easy/connect/缓冲  约 8~12 KB
 *   mbedtls TLS 收发缓冲       约 34 KB    (IN/OUT 各 16KB + transform)
 *   ------------------------------------
 *   请求峰值 heap 约 50~55 KB
 *   其中 mbedtls 约 34KB 是 SDK 固定开销，不是本模块申请的
 *
 * 五、总计（模块被业务引用时） —— 优化后
 *   静态 0.1KB + rodata 1.2KB + 线程栈 16KB + heap 峰值 50~55KB
 *   ≈ 67~72 KB
 *
 *   若 CHAT_AGENT_AUTO_START=0 且没有任何业务调用 chat_agent_*，
 *   LTO 会把整个模块裁掉，实际占用为 0。
 *
 * 六、本次内存优化
 *   1. SSE 上下文不再复制累计文本，直接写入调用方 chat_result_t:
 *      chat_sse_ctx_t 6176 B -> 2084 B
 *   2. chat_result_t 从常驻 .bss 改为按请求 heap 分配:
 *      静态 RAM 4108 B -> 0
 *   3. 业务线程栈 5120 word(20KB) -> 4096 word(16KB)
 *   4. chat_msg_t 保持 512B 提问缓冲，兼容较长的 ASR 文本
 *   ------------------------------------
 *   闲置 RAM 约 -8.0 KB，请求峰值 RAM 约 -8.0 KB
 *
 * 七、可按需继续裁剪
 *   CHAT_FULL_TEXT_SIZE  4096 -> 2048  : 峰值再 -2KB（长回复会截断）
 *   CHAT_SSE_LINE_SIZE   2048 -> 1024  : 峰值再 -1KB（单帧 JSON 需 <1KB）
 *   CHAT_TASK_STACK_WORDS 4096 -> 3072 : 再 -4KB（需板端确认无栈溢出）
 * ============================================================ */

/* 自动演示: 联网后自动发一条问题，验证链路是否打通
 * 量产代码请置 0，改用 chat_agent_request() 由业务触发。
 * 注意: 置 0 且没有任何业务调用 chat_agent_* 时，LTO 会把整个模块裁掉，
 * 静态占用为 0；上面的统计是模块被业务引用时的占用。
 */
#define CHAT_AGENT_AUTO_START           0
#define CHAT_AGENT_DEFAULT_QUESTION     "为我介绍杰理科技"

/* 单线程事件类型 */
enum {
    CHAT_EVT_REQUEST = 1,
    CHAT_EVT_STOP,
};

typedef struct {
    int type;
    char question[CHAT_QUESTION_SIZE];
} chat_msg_t;

static OS_QUEUE s_chat_event_q;
static u8 s_chat_inited;
static volatile u8 s_chat_busy;
static chat_agent_config_t s_chat_config;
static chat_agent_result_cb_t s_chat_result_cb;

/* ------------------------- 业务流程 ------------------------- */

/* 不逐帧打印: SSE 增量只累加到 res->text，等请求结束再整段输出，
 * 避免回复被 UART 日志切成很多段。
 * res 按请求临时分配，空闲时不占常驻 RAM。
 */
static int chat_do_request(const char *question)
{
    chat_result_t *res = NULL;
    int ret;
    int len;

    s_chat_busy = 1;
    res = (chat_result_t *)malloc(sizeof(*res));
    if (!res) {
        printf("[CHAT] no mem for reply buffer\n");
        s_chat_busy = 0;
        return CHAT_ERR_MEM;
    }
    memset(res, 0, sizeof(*res));

    printf("[CHAT] question: %s\n", question);
    ret = chat_completion_request(&s_chat_config, question, NULL, NULL, res);

    len = (int)strlen(res->text);
    if (s_chat_result_cb) {
        /* 上层接管回复，回调里自行拷贝 */
        s_chat_result_cb(res->text, ret);
    } else if (len > 0) {
        printf("[CHAT] reply (%d bytes):\n%s\n", len, res->text);
    } else {
        printf("[CHAT] reply is empty\n");
    }
    printf("[CHAT] request end, ret=%d, code=%d\n", ret, res->code);

    if (ret != CHAT_OK) {
        printf("[CHAT] failed, check bot_id/api_key/network\n");
    }
    free(res);
    s_chat_busy = 0;
    return ret;
}

static void chat_agent_msg_handler(void *ev)
{
    chat_msg_t *msg = (chat_msg_t *)ev;

    switch (msg->type) {
    case CHAT_EVT_REQUEST:
        if (s_chat_busy) {
            printf("[CHAT] busy, ignore new request\n");
            if (s_chat_result_cb) {
                s_chat_result_cb("", CHAT_ERR_BUSY);
            }
            break;
        }
        chat_do_request(msg->question);
        break;
    case CHAT_EVT_STOP:
        chat_completion_stop();
        break;
    default:
        break;
    }
}

/* 唯一业务线程: 从同一个队列取事件并分发 */
static void chat_agent_task(void *priv)
{
    int msg[16];

    (void)priv;

    while (1) {
        void *ev = NULL;

        if (os_q_pend(&s_chat_event_q, 0, msg) != 0) {
            continue;
        }
        ev = (void *)msg[0];
        if (!ev) {
            continue;
        }
        chat_agent_msg_handler(ev);
        free(ev);
    }
}

/* ------------------------- 对外接口 ------------------------- */

void chat_agent_set_result_cb(chat_agent_result_cb_t cb)
{
    s_chat_result_cb = cb;
}

int chat_agent_init(void)
{
    if (s_chat_inited) {
        return 0;
    }

    s_chat_config.bot_id = CHAT_BOT_ID;
    s_chat_config.api_key = CHAT_API_KEY;
    s_chat_config.service_name = CHAT_API_SERVICE_NAME;

    if (os_q_create(&s_chat_event_q, CHAT_EVENT_QUEUE_DEPTH) != 0) {
        printf("[CHAT] create event queue failed\n");
        return -1;
    }
    if (thread_fork("chat_agent_task", 10, CHAT_TASK_STACK_WORDS, 0,
                    NULL, chat_agent_task, NULL) != OS_NO_ERR) {
        printf("[CHAT] fork chat task failed\n");
        os_q_del(&s_chat_event_q, 1);
        return -1;
    }

    chat_completion_global_init();
    s_chat_inited = 1;
    return 0;
}

int chat_agent_request(const char *question)
{
    const char *text = (question && question[0]) ? question : CHAT_AGENT_DEFAULT_QUESTION;
    chat_msg_t *msg = NULL;

    if (chat_agent_init() != 0) {
        return -1;
    }

    msg = (chat_msg_t *)malloc(sizeof(chat_msg_t));
    if (!msg) {
        return -1;
    }
    memset(msg, 0, sizeof(*msg));
    msg->type = CHAT_EVT_REQUEST;
    strncpy(msg->question, text, sizeof(msg->question) - 1);

    if (os_q_post(&s_chat_event_q, msg) != 0) {
        printf("[CHAT] post request event failed\n");
        free(msg);
        return -1;
    }
    return 0;
}

int chat_agent_stop(void)
{
    chat_msg_t *msg = NULL;

    chat_completion_stop();     /* 当前请求立即生效 */

    if (!s_chat_inited) {
        return 0;
    }

    msg = (chat_msg_t *)malloc(sizeof(chat_msg_t));
    if (!msg) {
        return -1;
    }
    memset(msg, 0, sizeof(*msg));
    msg->type = CHAT_EVT_STOP;

    if (os_q_post(&s_chat_event_q, msg) != 0) {
        free(msg);
        return -1;
    }
    return 0;
}

/* ------------------------- 自动演示入口 ------------------------- */

#if CHAT_AGENT_AUTO_START
static void chat_demo_start_task(void *priv)
{
    (void)priv;

    if (CHAT_BOT_ID[0] == '\0' || CHAT_API_KEY[0] == '\0') {
        printf("[CHAT] CHAT_BOT_ID/CHAT_API_KEY empty, edit chat_completion.h\n");
        return;
    }

    while (wifi_get_sta_connect_state() != WIFI_STA_NETWORK_STACK_DHCP_SUCC) {
        printf("[CHAT] waiting network...\n");
        msleep(1000);
    }
    printf("[CHAT] network ready\n");
    chat_agent_request(CHAT_AGENT_DEFAULT_QUESTION);
}

static int chat_demo_init(void)
{
    if (chat_agent_init() != 0) {
        return -1;
    }
    thread_fork("chat_demo_start", 10, 512, 0, NULL, chat_demo_start_task, NULL);
    return 0;
}

late_initcall(chat_demo_init);
#endif
