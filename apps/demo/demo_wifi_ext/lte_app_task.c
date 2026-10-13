#include "app_config.h"
#include "init.h"
#include "system/timer.h"
#include "lwip.h"
#include "lte_module/lte_module.h"
#include "wifi/wifi_connect.h"
#include "lwip/sockets.h"
#include "netif/bridgeif.h"
#include "lwip/tcpip.h"

#ifdef CONFIG_LTE_PHY_ENABLE

static struct {
    void *dev;
    u16 check_timer;
    u8 wifi_on_flag;
    u8 hwaddr[6];
} lte_app_hdl;

#define __this	(&lte_app_hdl)

extern void ext_wifi_on(void);
extern void ext_wifi_off(void);
extern void ip_napt_enable(u32_t addr, int enable);
static void dual_ap_bridge_init(void);

#if IP_NAPT_EXT
static void check_dhcps_client_ipaddr_timer(void *p)
{
    struct ip4_addr src_ipaddr, dest_ipaddr;
    char gw_addr[32];
    struct ip4_addr ipaddr;

    extern int dhcps_get_ipaddr(u8 hwaddr[6], struct ip4_addr * ipaddr);
    if (0 == dhcps_get_ipaddr(__this->hwaddr, &dest_ipaddr)) {
        sys_timer_del(__this->check_timer);
        __this->check_timer = 0;
        get_gateway(LTE_NETIF, gw_addr);
        inet_aton(gw_addr, &src_ipaddr);
        extern void ip_napt_ext_set_forward_addr(u32_t src_addr, u32_t dest_addr);
        /* ip_napt_ext_set_forward_addr(src_ipaddr.addr, dest_ipaddr.addr); */
#if defined CONFIG_EXT_WIFI_ENABLE && defined CONFIG_UART_NET_ENABLE && IP_NAPT_EXT
        if (!__this->wifi_on_flag) {
            dhcps_uninit();
            ext_wifi_on();
            __this->wifi_on_flag = 1;
            struct lan_setting *lan_setting_info = net_get_lan_info(EXT_WIFI_NETIF);
            IP4_ADDR(&ipaddr, lan_setting_info->WIRELESS_IP_ADDR0, lan_setting_info->WIRELESS_IP_ADDR1, lan_setting_info->WIRELESS_IP_ADDR2, lan_setting_info->WIRELESS_IP_ADDR3);
            ip_napt_enable(ipaddr.addr, 1);
        }
#endif
#if defined CONFIG_WIFI_ENABLE && defined CONFIG_UART_NET_ENABLE && IP_NAPT_EXT
        if (!wifi_is_on()) {
            dhcps_uninit();
            wifi_on();
            struct lan_setting *lan_setting_info = net_get_lan_info(WIFI_NETIF);
            IP4_ADDR(&ipaddr, lan_setting_info->WIRELESS_IP_ADDR0, lan_setting_info->WIRELESS_IP_ADDR1, lan_setting_info->WIRELESS_IP_ADDR2, lan_setting_info->WIRELESS_IP_ADDR3);
            ip_napt_enable(ipaddr.addr, 1);
        }
#endif
    }
}

void check_dhcps_client_ipaddr(u8 *hwaddr)
{
    memcpy(__this->hwaddr, hwaddr, 6);
    if (!__this->check_timer) {
        __this->check_timer = sys_timer_add_to_task("sys_timer", NULL, check_dhcps_client_ipaddr_timer, 500);
    }
}
#endif


#ifdef CONFIG_UART_NET_ENABLE

#include "device/uart.h"

/* #define UART_NET_STA_MODE */

extern const struct uart_platform_data uart0_data;
void uart_ethernetif_set_dev_name(const char *name);
void uart_ethernetif_set_platform_data(const struct uart_platform_data *parm);
void lwip_uart_set_mac_addr(u8 *mac_addr);

static const u8 uart_net_ap_mac_addr[6] = {
    0x00, 0x11, 0x22, 0x33, 0x44, 0x50
};
static const u8 uart_net_sta_mac_addr[6] = {
    0x00, 0x11, 0x22, 0x33, 0x44, 0x58
};

static int uart_net_init(void)
{
    uart_ethernetif_set_dev_name("uart0");
    uart_ethernetif_set_platform_data(&uart0_data);
#ifdef UART_NET_STA_MODE
    lwip_uart_set_mac_addr(uart_net_sta_mac_addr);
#else
    lwip_uart_set_mac_addr(uart_net_ap_mac_addr);
#endif
    Init_LwIP(UART_NETIF);
    lwip_netif_set_up(UART_NETIF);
#ifdef UART_NET_STA_MODE
    lwip_renew(UART_NETIF, 1);
#else
    lwip_renew(UART_NETIF, 0);
    dhcps_init(UART_NETIF);
#endif
    return 0;
}
#ifdef UART_NET_STA_MODE
late_initcall(uart_net_init);
#endif


int uart_lwip_event_cb(void *lwip_ctx, enum LWIP_EVENT event)
{
    switch (event) {
    case LWIP_UART_DHCP_BOUND_TIMEOUT:
        break;

    case LWIP_UART_DHCP_BOUND_SUCC:
#ifdef UART_NET_STA_MODE
        extern void iperf_test(void);
        iperf_test();
        void http_get_baidu_test_start(void);
        http_get_baidu_test_start();
#endif
        break;

    default:
        break;
    }

    return 0;
}

#endif


int lte_lwip_event_cb(void *lwip_ctx, enum LWIP_EVENT event)
{
    char ip_addr[32];
    char gw_addr[32];

    switch (event) {
    case LWIP_LTE_DHCP_BOUND_TIMEOUT:
        break;

    case LWIP_LTE_DHCP_BOUND_SUCC:
        Get_IPAddress(LTE_NETIF, ip_addr);
        get_gateway(LTE_NETIF, gw_addr);
        printf("LTE DHCP SUCC, IP:[%s] , GW:[%s]", ip_addr, gw_addr);
        //此处4G网络已连通
        lwip_set_default_netif(LTE_NETIF);   //设置4G网卡为默认模块

#if IP_BRIDGE
        // 方案二：Bridge 双 AP 组网
        // WiFi 和 UART 端口通过 Bridge 桥接为统一广播域
        dual_ap_bridge_init();
#else
#if defined CONFIG_UART_NET_ENABLE && !defined UART_NET_STA_MODE
        uart_net_init();
#endif
#if defined CONFIG_EXT_WIFI_ENABLE && !defined CONFIG_UART_NET_ENABLE && IP_NAPT_EXT
        if (!__this->wifi_on_flag) {
            ext_wifi_on();
            __this->wifi_on_flag = 1;
        }
#endif
#if defined CONFIG_WIFI_ENABLE && !defined CONFIG_UART_NET_ENABLE && IP_NAPT_EXT
        if (wifi_is_on()) {
            wifi_on();
        }
#endif
#if IP_NAPT
        struct ip4_addr ipaddr;
#if defined CONFIG_UART_NET_ENABLE
        struct lan_setting *lan_setting_info = net_get_lan_info(UART_NETIF);
#elif defined CONFIG_EXT_WIFI_ENABLE
        struct lan_setting *lan_setting_info = net_get_lan_info(EXT_WIFI_NETIF);
#elif defined CONFIG_WIFI_ENABLE
        struct lan_setting *lan_setting_info = net_get_lan_info(WIFI_NETIF);
#endif
        IP4_ADDR(&ipaddr, lan_setting_info->WIRELESS_IP_ADDR0, lan_setting_info->WIRELESS_IP_ADDR1,
                 lan_setting_info->WIRELESS_IP_ADDR2, lan_setting_info->WIRELESS_IP_ADDR3);
        ip_napt_enable(ipaddr.addr, 1);
#if defined CONFIG_UART_NET_ENABLE && IP_NAPT_EXT
        check_dhcps_client_ipaddr(uart_net_sta_mac_addr);
#endif
#endif  /* IP_NAPT */
#endif  /* Bridge mode */

        break;

    default:
        break;
    }

    return 0;
}

static int lte_state_cb(void *priv, int on)
{
    if (priv && !strncmp((const char *)priv, "wireless", strlen("wireless"))) {
        if (on) {
            printf("lte on\n");
            dev_ioctl(__this->dev, LTE_NETWORK_START, 0);
        } else {
            printf("lte off\n");
            dev_ioctl(__this->dev, LTE_NETWORK_STOP, 0);
#if 0//IP_NAPT_EXT	//TODO外挂wifi卸载会异常
            ext_wifi_off();
            __this->wifi_on_flag = 0;
#endif
        }
    }

    return 0;
}

static int lte_net_init(void)
{
    __this->dev = dev_open("lte", NULL);
    return dev_ioctl(__this->dev, LTE_DEV_SET_CB, (u32)lte_state_cb);
}
late_initcall(lte_net_init);


/*****************************************************************************
 * 百度首页 HTTP GET 测试示例（基于 http_cli.h 接口）
 * 功能：通过 httpcli_get 访问 www.baidu.com 首页，验证 4G/LTE 网络连通性。
 * 使用方式：在 LTE DHCP 成功后调用 http_get_baidu_test_start() 即可。
 *****************************************************************************/

#include "http/http_cli.h"

#define BAIDU_HTTP_URL     "http://www.baidu.com"
#define BAIDU_HTTP_TIMEOUT 5000

/**
 * @brief 百度首页 HTTP GET 请求任务
 */
static void http_get_baidu_task(void *priv)
{
again:
    http_body_obj http_body;
    httpcli_ctx *ctx = (httpcli_ctx *)calloc(1, sizeof(httpcli_ctx));
    if (!ctx) {
        printf("[HTTP_TEST] calloc ctx failed!\r\n");
        return;
    }

    memset(&http_body, 0, sizeof(http_body));
    http_body.buf_len = 4 * 1024;
    http_body.buf_count = 1;
    http_body.recv_len = 0;
    http_body.p = (char *)malloc(http_body.buf_len);
    if (!http_body.p) {
        printf("[HTTP_TEST] malloc recv buf failed!\r\n");
        free(ctx);
        return;
    }

    ctx->url = BAIDU_HTTP_URL;
    ctx->timeout_millsec = BAIDU_HTTP_TIMEOUT;
    ctx->priv = (void *)&http_body;
    ctx->connection = "close";

    printf("[HTTP_TEST] HTTP GET %s start...\r\n", BAIDU_HTTP_URL);
    int ret = httpcli_get(ctx);
    if (ret != HERROR_OK) {
        printf("[HTTP_TEST] httpcli_get failed, err=%d\r\n", ret);
    } else {
        printf("[HTTP_TEST] HTTP GET success, received %u bytes:\r\n", http_body.recv_len);
        printf("==================== HTTP RESPONSE START ====================\r\n");
        printf("%s\r\n", http_body.p);
        printf("==================== HTTP RESPONSE END   ====================\r\n");
    }

    httpcli_close(ctx);

    if (http_body.p) {
        free(http_body.p);
    }
    free(ctx);
    printf("[HTTP_TEST] Task exit\r\n");

    os_time_dly(300);

    goto again;
}

/**
 * @brief 启动百度首页 HTTP GET 测试
 * @note  在 LTE DHCP 绑定成功后调用：
 *        http_get_baidu_test_start();
 */
void http_get_baidu_test_start(void)
{
    thread_fork("http_baidu", 3, 2048, 0, NULL, http_get_baidu_task, NULL);
}

#if IP_BRIDGE
/**
 * @brief 方案二：Bridge 双 AP 组网初始化
 *
 * 将 WiFi AP 和 UART AP 通过 LWIP Bridge 桥接为统一广播域。
 * Bridge 拥有统一 IP 192.168.0.1/24，WiFi/UART 作为纯 L2 端口（无 IP）。
 * LTE 网卡作为 WAN 口（napt=0），NAPT 标记在 Bridge 上（napt=1）。
 */
static void dual_ap_bridge_init(void)
{
    struct ip4_addr lan_ip, lan_mask, lan_gw;
    struct lan_setting *lan = net_get_lan_info(BRIDGE_NETIF);
    struct netif *br = net_get_netif_handle(BRIDGE_NETIF);

    IP4_ADDR(&lan_ip,
             lan->WIRELESS_IP_ADDR0, lan->WIRELESS_IP_ADDR1,
             lan->WIRELESS_IP_ADDR2, lan->WIRELESS_IP_ADDR3);
    IP4_ADDR(&lan_mask,
             lan->WIRELESS_NETMASK0, lan->WIRELESS_NETMASK1,
             lan->WIRELESS_NETMASK2, lan->WIRELESS_NETMASK3);
    IP4_ADDR(&lan_gw,
             lan->WIRELESS_GATEWAY0, lan->WIRELESS_GATEWAY1,
             lan->WIRELESS_GATEWAY2, lan->WIRELESS_GATEWAY3);

#if defined CONFIG_UART_NET_ENABLE && !defined UART_NET_STA_MODE
    // === Step 1: 初始化 UART 端口（纯 L2，无 IP）===
    uart_ethernetif_set_dev_name("uart0");
    uart_ethernetif_set_platform_data(&uart0_data);
    lwip_uart_set_mac_addr(uart_net_ap_mac_addr);
#endif

#if defined CONFIG_WIFI_ENABLE
    // === Step 2: 启动 WiFi 硬件（Bridge 模式下不在 wifi_demo_task 启动）===
    wifi_on();
#endif

    // === Step 3: 确保 lwip 初始化 ===
    Init_LwIP(BRIDGE_NETIF);
#if defined CONFIG_UART_NET_ENABLE && !defined UART_NET_STA_MODE
    Init_LwIP_BridgePort(UART_NETIF);
    lwip_netif_set_up(UART_NETIF);
#endif
#if defined CONFIG_WIFI_ENABLE
    Init_LwIP_BridgePort(WIFI_NETIF);
    lwip_netif_set_up(WIFI_NETIF);
#endif

    // === Step 4: 创建 Bridge netif ===
    bridgeif_initdata_t bridge_cfg = BRIDGEIF_INITDATA2(
                                         2,    // max_ports: WiFi + UART
                                         32,   // max_fdb_dynamic
                                         8,    // max_fdb_static
                                         0x02, 0x00, 0x00, 0x00, 0x00, 0x01  // Bridge MAC
                                     );

    netif_add(br, &lan_ip, &lan_mask, &lan_gw,
              &bridge_cfg, bridgeif_init, tcpip_input);
    netif_set_up(br);

    // === Step 5: 添加物理端口（WiFi 需等待 HSM 初始化完成）===
    struct netif *wifi = net_get_netif_handle(WIFI_NETIF);
    struct netif *uart = net_get_netif_handle(UART_NETIF);

    if (wifi) {
        // WIFI 端口缺少 NETIF_FLAG_ETHERNET（wireless_ethernetif_init 未设置），bridgeif_add_port 要求此标志
        wifi->flags |= NETIF_FLAG_ETHERNET;
        if (bridgeif_add_port(br, wifi) == ERR_OK) {
            printf("[Bridge] WiFi bridged as br0 (%s)\n", ip4addr_ntoa(&lan_ip));
        } else {
            printf("[Bridge] WiFi add port FAILED\n");
        }
    }
    if (uart) {
        // UART 端口缺少 NETIF_FLAG_ETHERNET（uart_ethernetif_init 未设置），bridgeif_add_port 要求此标志
        uart->flags |= NETIF_FLAG_ETHERNET;
        if (bridgeif_add_port(br, uart) == ERR_OK) {
            printf("[Bridge] UART bridged as br0 (%s)\n", ip4addr_ntoa(&lan_ip));
        } else {
            printf("[Bridge] UART add port FAILED\n");
        }
    }

    // === Step 6: NAPT — Bridge(napt=1) + LTE(napt=0) ====
    ip_napt_enable(lan_ip.addr, 1);

    // === Step 7: 启动 Bridge 上的 DHCP Server ===
    dhcps_init(BRIDGE_NETIF);
}
#endif /* Bridge mode */

#endif
