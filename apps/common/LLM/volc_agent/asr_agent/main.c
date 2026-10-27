/*
 * ASR Agent —— 应用层（单线程事件分发）
 *
 * 结构:
 *   asr.c  : websocket 连接 + SAUC 首帧/音频帧发送 + gzip/解析
 *   asr.h  : 配置宏、数据结构、接口声明、线程/队列资源说明
 *   main.c : 单一事件队列 + 一个业务线程，通过事件类型分发
 *
 * 事件流:
 *   asr_agent_start(path) / asr_agent_stop()
 *        └─> g_asr_event_q ─> asr_agent_task
 *                               ├─ ASR_EVT_START : 建链 -> 打开文件 -> 分包发送
 *                               ├─ ASR_EVT_STOP  : 置停止标志
 *                               └─ ASR_EVT_RESULT: 处理 websocket 回调二次打包后的结果
 *
 *   websocket recv 线程
 *        └─ asr_on_ws_data() 解析 SAUC 响应 -> g_asr_last_result 邮箱
 *               └─> g_asr_event_q（与 START/STOP 同一个队列）
 *
 *   结果通知时机:
 *     asr_do_file() 运行期间只记录结果，不回调上层。等 websocket 完全关闭、
 *     回到 asr_agent_task 主循环后再通知。否则上层可能在回调里立即提交
 *     下一次 START，而该事件会被收尾阶段的 asr_pump_events() 丢弃。
 */

/* ==================== ASR 模块内存占用统计 ====================
 * 数据来源: 当前编译产物 sdk.elf (nm -S) + mbedtls 配置 + 代码分析
 *
 * 一、静态 RAM (.bss，常驻)
 *   g_asr_parse_tmp            1036 B   (asr_result_t)
 *   g_asr_last_result          1036 B   (asr_result_t)
 *   g_asr_json_small           2048 B   (gzip 解压小缓冲)
 *   g_asr_headers               256 B
 *   g_asr_url                   128 B
 *   g_asr_event_q                80 B   (OS_QUEUE, 8 项)
 *   g_asr_ws / g_asr_recv_cb      8 B
 *   g_asr_inited/printed          8 B
 *   g_asr_first_resp/done/
 *   error/stop_req                4 B
 *   g_asr_final_code              4 B
 *   ------------------------------------
 *   合计                       约 4.6 KB
 *
 * 二、线程栈（thread_fork 从 heap 分配，单位 word=4B）
 *   asr_agent_task      2048 word × 4 = 8.0 KB
 *   asr_ws_recv        2048 word × 4 = 8.0 KB
 *   asr_ws_heart        512 word × 4 = 2.0 KB
 *   asr_demo_start      512 word × 4 = 2.0 KB (仅自动演示)
 *   ------------------------------------
 *   合计              20.0 KB（不含演示线程 18.0 KB）
 *
 * 三、heap（动态）
 *   连接期间常驻:
 *     struct websocket_struct      3768 B  (实测 sizeof)
 *     websocket recv_buf           4096 B  (帧更大时库内 realloc)
 *     mbedtls TLS 收发缓冲      约 32 KB   (IN/OUT 各 16KB，库配置)
 *     CA/证书解析               约 2~4 KB
 *     音频读取缓冲                 6400 B  (PCM 16k/16bit/mono 200ms)
 *                                 4096 B  (MP3)
 *   发送时临时:
 *     SAUC 音频包               约 6.4 KB  (PCM) / 4.2 KB (MP3)
 *     SAUC 首帧包               约 0.4 KB
 *     cJSON 解析                约 1~3 KB
 *     JSON 解压(>2KB 时)        按需，最大 16 KB
 *     START/STOP 事件              132 B
 *     最终结果通知                   4 B
 *   ------------------------------------
 *   常驻 heap 约 48~52 KB
 *   峰值 heap 约 58 KB；大 JSON 响应时可达约 74 KB
 *
 * 四、总计（不含 SDK 其它模块）
 *   静态 4.6KB + 线程栈 20KB + heap 峰值 58~74KB
 *   ≈ 83~99 KB
 *   最大单项是 mbedtls 的 32KB TLS 收发缓冲（库内部，非本模块申请）
 * ============================================================ */

#include "asr.h"
#include "app_config.h"
#include "fs/fs.h"
#include "wifi/wifi_connect.h"

#include <stdlib.h>
#include <string.h>

/* 自动演示: 联网后自动识别内部 flash 的提示音 NetCfgSucc.mp3
 * CONFIG_VOICE_PROMPT_FILE_PATH 由 app_config.h 根据资源配置生成，当前为:
 *   mnt/sdfile/EXT_RESERVED/aupackres/tone/
 */
#define ASR_AGENT_AUTO_START        0
#define ASR_AGENT_DEFAULT_FILE      CONFIG_VOICE_PROMPT_FILE_PATH"NetCfgSucc.mp3"

/* 单线程事件类型 */
enum {
    ASR_EVT_START = 1,
    ASR_EVT_STOP,
    ASR_EVT_RESULT,
};

/* 控制事件: START / STOP（type 必须是第一个成员，便于统一分发） */
typedef struct {
    int type;
    char file_path[128];
} asr_ctrl_msg_t;

/* 结果通知事件: 只携带类型，真正的结果放在 g_asr_last_result 邮箱里。
 * 一次完整上传只在最终包/错误时投递一次，避免每个中间结果都 malloc。
 */
typedef struct {
    int type;
} asr_notify_msg_t;

static OS_QUEUE g_asr_event_q;      /* 单一线程事件队列 */
static u8 g_asr_inited;             /* 初始化标志: 仅 main.c 内部使用 */

/* 跨线程状态标志: websocket recv 线程写 / 业务线程读。
 * 必须是 volatile；本平台单字节读写是原子的，用 u8 比 int 更省 RAM。
 */
static volatile u8 g_asr_first_resp;    /* 收到首包 */
static volatile u8 g_asr_done;          /* 收到最后一包 */
static volatile u8 g_asr_error;         /* 服务端/协议错误 */
static volatile u8 g_asr_stop_req;      /* 外部请求停止 */

/* 本次识别最终结果码，仅业务线程读写；-1 表示失败/无结果 */
static int g_asr_final_code = -1;

/* 只保留最后一次返回结果:
 * g_asr_parse_tmp  : recv 线程解析临时区
 * g_asr_last_result: 最终结果邮箱（文本只保留最后一次非空值）
 */
static asr_result_t g_asr_parse_tmp;
static asr_result_t g_asr_last_result;
static u8 g_asr_result_printed;

/* 上层结果回调: 一次识别只通知一次 */
static asr_agent_result_cb_t g_asr_result_cb;
static u8 g_asr_result_notified;

/* device_mount.c 提供: SD 在线 + 挂载成功返回 true */
extern int storage_device_ready(void);

/* ------------------------- 小工具 ------------------------- */

/* 等待 SD 卡挂载完成 (拔插卡/开机挂载是异步的) */
static int asr_wait_storage(int timeout_ms)
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

static u16 asr_rd_le16(const u8 *p)
{
    return (u16)(p[0] | (p[1] << 8));
}

static u32 asr_rd_le32(const u8 *p)
{
    return (u32)p[0] | ((u32)p[1] << 8) | ((u32)p[2] << 16) | ((u32)p[3] << 24);
}

/* --------------------------- WAV 解析 --------------------------- */

static int asr_file_skip(FILE *fp, u32 n)
{
    u8 tmp[64];
    while (n) {
        int r = n > sizeof(tmp) ? (int)sizeof(tmp) : (int)n;
        if (fread(tmp, 1, r, fp) != r) {
            return -1;
        }
        n -= r;
    }
    return 0;
}

/* 成功返回 0, 文件指针停在 data 区起始处 */
static int asr_wav_parse(FILE *fp, int file_size, asr_audio_info_t *info)
{
    u8 hdr[12];
    int got_fmt = 0;

    if (fread(hdr, 1, 12, fp) != 12) {
        return -1;
    }
    if (memcmp(hdr, "RIFF", 4) != 0 || memcmp(hdr + 8, "WAVE", 4) != 0) {
        return -1;      /* 不是 WAV */
    }

    while (1) {
        u8 ch[8];
        u32 sz;

        if (fread(ch, 1, 8, fp) != 8) {
            return -1;
        }
        sz = asr_rd_le32(ch + 4);

        if (memcmp(ch, "fmt ", 4) == 0) {
            u8 fmt[16];
            int n = sz < sizeof(fmt) ? (int)sz : (int)sizeof(fmt);

            if (fread(fmt, 1, n, fp) != n) {
                return -1;
            }
            info->channels    = asr_rd_le16(fmt + 2);
            info->sample_rate = (int)asr_rd_le32(fmt + 4);
            info->bits        = asr_rd_le16(fmt + 14);
            got_fmt = 1;

            if (sz > (u32)n && asr_file_skip(fp, sz - n) != 0) {
                return -1;
            }
            if (sz & 1) {
                if (asr_file_skip(fp, 1) != 0) {
                    return -1;
                }
            }
        } else if (memcmp(ch, "data", 4) == 0) {
            int pos;

            if (!got_fmt) {
                return -1;
            }
            pos = ftell(fp);
            if (pos < 0 || pos > file_size) {
                return -1;
            }
            info->data_len = (sz == 0 || sz > (u32)(file_size - pos))
                             ? (file_size - pos) : (int)sz;
            return 0;
        } else {
            if (asr_file_skip(fp, sz) != 0) {
                return -1;
            }
            if (sz & 1) {
                if (asr_file_skip(fp, 1) != 0) {
                    return -1;
                }
            }
        }
    }
}

/* ------------------------- 事件分发 ------------------------- */

/* ------------------------- 结果通知 ------------------------- */

void asr_agent_set_result_cb(asr_agent_result_cb_t cb)
{
    g_asr_result_cb = cb;
}

/* 一次识别只通知一次，重复调用被忽略 */
static void asr_agent_notify(int code)
{
    if (!g_asr_result_cb || g_asr_result_notified) {
        return;
    }
    g_asr_result_notified = 1;
    g_asr_result_cb(g_asr_last_result.text, code);
}

/* asr_do_file() 提前失败时统一走这里。
 * 只记录失败，结果通知由 asr_agent_task 在本函数返回后统一发出。 */
static int asr_fail(void)
{
    g_asr_final_code = -1;
    return -1;
}

/* 记录并打印结果；通知上层延后到 asr_do_file() 返回之后 */
static void asr_handle_result(void)
{
    asr_result_t *res = &g_asr_last_result;

    g_asr_result_printed = 1;

    if (res->code) {
        printf("[ASR] server error code=0x%x\n", res->code);
    }
    if (res->text[0]) {
        printf("[ASR] %s\n", res->text);
    }
    if (res->is_last) {
        printf("[ASR] final packet received\n");
    }

    g_asr_final_code = res->code ? res->code : (res->is_last ? 0 : -1);
}

/* websocket 回调: 运行在 websocket recv 线程
 * 每次响应只更新 g_asr_last_result 邮箱，不逐包 malloc 事件；
 * 只有最终包或错误才投递一个很小的 ASR_EVT_RESULT 通知。
 */
static void asr_on_ws_data(const u8 *data, int len, u8 ws_type)
{
    asr_result_t *tmp = &g_asr_parse_tmp;

    /* ws_type 是 websocket 首字节: 0x82 = binary final */
    if ((ws_type & 0x0f) != WCT_BINDATA) {
        return;
    }

    if (asr_proto_parse_response(data, len, tmp) != 0) {
        return;
    }

    g_asr_first_resp = 1;

    /* 文本只保留最后一次非空结果，避免空响应覆盖 */
    if (tmp->text[0]) {
        strncpy(g_asr_last_result.text, tmp->text,
                sizeof(g_asr_last_result.text) - 1);
        g_asr_last_result.text[sizeof(g_asr_last_result.text) - 1] = '\0';
    }
    g_asr_last_result.code = tmp->code;
    g_asr_last_result.event = tmp->event;
    g_asr_last_result.is_last = tmp->is_last;

    if (tmp->is_last) {
        g_asr_done = 1;
    }
    if (tmp->code) {
        g_asr_error = 1;
    }

    /* 一次完整上传只通知一次 */
    if (tmp->is_last || tmp->code) {
        asr_notify_msg_t *msg = (asr_notify_msg_t *)malloc(sizeof(asr_notify_msg_t));
        if (msg) {
            msg->type = ASR_EVT_RESULT;
            if (os_q_post(&g_asr_event_q, msg) != 0) {
                free(msg);
            }
        }
    }
}

/* 在发送/等待期间取队列并分发（只允许处理 RESULT/STOP，不递归启动新任务） */
static void asr_pump_events(void)
{
    int msg[16];

    while (os_q_accept(&g_asr_event_q, msg) == 0) {
        void *ev = (void *)msg[0];
        int type;

        if (!ev) {
            continue;
        }
        type = *(int *)ev;

        switch (type) {
        case ASR_EVT_RESULT:
            asr_handle_result();
            break;
        case ASR_EVT_STOP:
            g_asr_stop_req = 1;
            break;
        case ASR_EVT_START:
            /* 当前任务尚未结束，忽略新的 START，避免递归 */
            printf("[ASR] busy, ignore new start event\n");
            break;
        default:
            break;
        }
        free(ev);
    }
}

/* 等待 ms 毫秒，期间持续分发事件，保证结果实时打印 */
static void asr_wait_and_pump(int ms)
{
    int t;

    for (t = 0; t < ms; t += 10) {
        asr_pump_events();
        msleep(10);
    }
    asr_pump_events();
}

/* ------------------------- 识别流程 ------------------------- */

static int asr_do_file(const char *path)
{
    FILE *fp = NULL;
    int file_size;
    int seg_size;
    int remain;
    int seq = 1;
    int i;
    u8 *pcm = NULL;
    asr_audio_info_t info;

    memset(&info, 0, sizeof(info));
    memset(&g_asr_parse_tmp, 0, sizeof(g_asr_parse_tmp));
    memset(&g_asr_last_result, 0, sizeof(g_asr_last_result));
    g_asr_result_printed = 0;
    g_asr_result_notified = 0;
    g_asr_first_resp = 0;
    g_asr_done = 0;
    g_asr_error = 0;
    g_asr_stop_req = 0;
    g_asr_final_code = -1;

    /* 0. 只有文件在 SD 卡上时才检查挂载；内部 flash(mnt/sdfile) 直接可读 */
    if (strncmp(path, "storage/", 8) == 0) {
        if (asr_wait_storage(3000) != 0) {
            printf("[ASR] SD not ready: %s (check card/format)\n", CONFIG_STORAGE_PATH);
            return asr_fail();
        }
        printf("[ASR] SD ready: %s\n", CONFIG_ROOT_PATH);
    } else {
        printf("[ASR] internal flash file: %s\n", path);
    }

    fp = fopen(path, "rb");
    if (!fp) {
        printf("[ASR] open %s failed\n", path);
        return asr_fail();
    }

    file_size = flen(fp);
    if (file_size <= 0) {
        printf("[ASR] file empty: %d\n", file_size);
        fclose(fp);
        return asr_fail();
    }

    if (asr_wav_parse(fp, file_size, &info) != 0) {
        u8 magic[4] = {0};
        int n;

        /* 不是 WAV: 简单识别 MP3 / 裸 PCM */
        fseek(fp, 0, SEEK_SET);
        n = fread(magic, 1, sizeof(magic), fp);
        fseek(fp, 0, SEEK_SET);
        info.data_len = file_size;
        info.sample_rate = 16000;
        info.channels = 1;
        info.bits = 16;

        if (n == 4 && (memcmp(magic, "ID3", 3) == 0 ||
                       (magic[0] == 0xff && (magic[1] & 0xe0) == 0xe0))) {
            strcpy(info.format, "mp3");
            printf("[ASR] mp3 file, send 4KB per packet\n");
        } else {
            strcpy(info.format, "pcm");
            printf("[ASR] raw pcm file: 16k/16bit/mono\n");
        }
    } else {
        strcpy(info.format, "pcm");     /* WAV: 只发送 data 区 PCM */
    }

    if (info.sample_rate <= 0 || info.channels <= 0 || info.bits <= 0) {
        printf("[ASR] bad audio params: %d/%d/%d\n",
               info.sample_rate, info.channels, info.bits);
        fclose(fp);
        return asr_fail();
    }

    if (strcmp(info.format, "pcm") == 0) {
        int bytes_per_frame = info.channels * (info.bits / 8);
        seg_size = info.sample_rate * bytes_per_frame * SAUC_SEGMENT_MS / 1000;
    } else {
        seg_size = 4096;
    }
    if (seg_size <= 0) {
        fclose(fp);
        return asr_fail();
    }

    printf("[ASR] file=%s size=%d, format=%s, %dHz/%dch/%dbit, seg=%d bytes\n",
           path, info.data_len, info.format, info.sample_rate,
           info.channels, info.bits, seg_size);

    /* 1. 建立 websocket */
    if (asr_ws_open(SAUC_WS_URL, asr_on_ws_data) != 0) {
        fclose(fp);
        return asr_fail();
    }

    /* 2. 发送首帧 */
    if (asr_ws_send_full_request(&seq, &info) != 0) {
        goto exit_ws;
    }

    /* 3. 等首包(确认鉴权/参数)，期间处理结果事件 */
    for (i = 0; i < 30 && !g_asr_first_resp && !g_asr_error && !g_asr_stop_req; i++) {
        asr_wait_and_pump(100);
    }
    if (g_asr_error || g_asr_stop_req) {
        goto exit_ws;
    }

    /* 4. 按 200ms 节拍发送音频 */
    pcm = (u8 *)malloc(seg_size);
    if (!pcm) {
        goto exit_ws;
    }

    remain = info.data_len;
    if (remain <= 0) {
        asr_ws_send_audio(seq, NULL, 0, 1);
    }

    while (remain > 0 && !g_asr_error && !g_asr_stop_req) {
        int n = remain > seg_size ? seg_size : remain;

        if (fread(pcm, 1, n, fp) != n) {
            printf("[ASR] read audio failed, remain=%d\n", remain);
            break;
        }
        remain -= n;

        if (asr_ws_send_audio(seq, pcm, n, remain <= 0) != 0) {
            break;
        }
        if (remain > 0) {
            seq++;
            asr_wait_and_pump(SAUC_SEGMENT_MS);
        }
    }

    /* 5. 等最终结果，期间持续处理结果事件 */
    for (i = 0; i < SAUC_RECV_TIMEOUT_MS / 100 && !g_asr_done &&
         !g_asr_error && !g_asr_stop_req; i++) {
        asr_wait_and_pump(100);
    }
    if (g_asr_stop_req) {
        printf("[ASR] stopped by user\n");
    } else if (g_asr_error) {
        printf("[ASR] stopped by server error\n");
    }

exit_ws:
    if (pcm) {
        free(pcm);
    }
    fclose(fp);
    asr_ws_close();

    /* 把队列里剩余的结果事件处理掉 */
    asr_pump_events();

    /* 兜底：最终事件可能因超时/队列原因未分发，直接记录最后一次结果 */
    if (!g_asr_result_printed &&
        (g_asr_last_result.text[0] || g_asr_last_result.code)) {
        asr_handle_result();
    }

    /* 不在这里通知上层：asr_ws_close() 之后上层若立即提交下一次 START，
     * 会被收尾阶段的 asr_pump_events() 当成"忙"丢弃。通知统一由
     * asr_agent_msg_handler() 在本函数返回、ws 资源完全释放后发出。 */
    return 0;
}

/* 统一事件分发（只在 asr_agent_task 主循环中调用） */
static void asr_agent_msg_handler(void *ev)
{
    int type = *(int *)ev;

    switch (type) {
    case ASR_EVT_START:
        printf("[ASR] event START: %s\n", ((asr_ctrl_msg_t *)ev)->file_path);
        asr_do_file(((asr_ctrl_msg_t *)ev)->file_path);
        /* ws 已关闭、本线程即将回到队列等待，这时才通知上层。
         * 上层在回调中提交的下一次 START 会被主循环正常取到。 */
        asr_agent_notify(g_asr_final_code);
        break;

    case ASR_EVT_STOP:
        g_asr_stop_req = 1;
        break;

    case ASR_EVT_RESULT:
        asr_handle_result();
        /* 结果在 asr_do_file() 返回后才入队时的通知路径 */
        asr_agent_notify(g_asr_final_code);
        break;

    default:
        break;
    }
}

/* 唯一业务线程: 从同一个队列取事件并分发 */
static void asr_agent_task(void *priv)
{
    int msg[16];

    while (1) {
        void *ev;

        if (os_q_pend(&g_asr_event_q, 0, msg) != 0) {
            continue;
        }
        ev = (void *)msg[0];
        if (!ev) {
            continue;
        }

        asr_agent_msg_handler(ev);
        free(ev);
    }
}

/* --------------------------- 对外接口 --------------------------- */

int asr_agent_init(void)
{
    if (g_asr_inited) {
        return 0;
    }

    if (os_q_create(&g_asr_event_q, ASR_EVENT_QUEUE_DEPTH) != 0) {
        printf("[ASR] create event queue failed\n");
        return -1;
    }

    if (thread_fork("asr_agent_task", 10, ASR_TASK_STACK_WORDS, 0,
                    NULL, asr_agent_task, NULL) != OS_NO_ERR) {
        printf("[ASR] fork asr task failed\n");
        os_q_del(&g_asr_event_q, 1);
        return -1;
    }

    g_asr_inited = 1;
    return 0;
}

int asr_agent_start(const char *file_path)
{
    asr_ctrl_msg_t *ev;

    if (asr_agent_init() != 0) {
        return -1;
    }

    ev = (asr_ctrl_msg_t *)malloc(sizeof(asr_ctrl_msg_t));
    if (!ev) {
        return -1;
    }
    memset(ev, 0, sizeof(asr_ctrl_msg_t));
    ev->type = ASR_EVT_START;
    strncpy(ev->file_path,
            (file_path && file_path[0]) ? file_path : ASR_AGENT_DEFAULT_FILE,
            sizeof(ev->file_path) - 1);

    if (os_q_post(&g_asr_event_q, ev) != 0) {
        printf("[ASR] post start event failed\n");
        free(ev);
        return -1;
    }
    return 0;
}

int asr_agent_stop(void)
{
    asr_ctrl_msg_t *ev;

    g_asr_stop_req = 1;     /* 当前发送循环立即生效 */

    if (!g_asr_inited) {
        return 0;
    }

    ev = (asr_ctrl_msg_t *)malloc(sizeof(asr_ctrl_msg_t));
    if (!ev) {
        return -1;
    }
    memset(ev, 0, sizeof(asr_ctrl_msg_t));
    ev->type = ASR_EVT_STOP;

    if (os_q_post(&g_asr_event_q, ev) != 0) {
        free(ev);
        return -1;
    }
    return 0;
}

/* ------------------------- 自动演示入口 ------------------------- */

#if ASR_AGENT_AUTO_START
static void asr_demo_start_task(void *priv)
{
    while (wifi_get_sta_connect_state() != WIFI_STA_NETWORK_STACK_DHCP_SUCC) {
        printf("[ASR] waiting network...\n");
        msleep(500);
    }
    printf("[ASR] network ready\n");
    asr_agent_start(ASR_AGENT_DEFAULT_FILE);
}

static int asr_demo_init(void)
{
    if (asr_agent_init() != 0) {
        return -1;
    }
    thread_fork("asr_demo_start", 10, 512, 0, NULL, asr_demo_start_task, NULL);
    return 0;
}

late_initcall(asr_demo_init);
#endif
