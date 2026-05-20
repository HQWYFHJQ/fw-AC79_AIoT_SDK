#include "system/includes.h"
#include "os/os_api.h"
#include "asm/hwi.h"
#include "wifi/wifi_connect.h"
#include "utils/syscfg_id.h"
#include "logic/boot.h"
#include "norflash.h"
#include "asm/crc16.h"


extern s32 spi0flash_read(u8 *buf, u32 addr, u16 len);
extern s32 spi0flash_write(u8 *buf, u32 addr, u32 len);
extern void spi0flash_eraser(FLASH_ERASER eraser, u32 address);
extern u32 get_flash_capability(void);

extern struct wifi_store_info wifi_reserved_info;

#define RESERVED_CONFIG_MAGIC   0x5753
#define FLASH_SECTOR_SIZE       4096

struct reserved_info_transfer {
    u32 reserved_addr;    // parm_priv[0..3]  预留区 flash 地址
    u32 reserved_len;     // parm_priv[4..7]  预留区长度
};

//预留区存储的WiFi/url配置
struct reserved_wifi_config {
    u16 magic;
    u16 crc;

    struct wifi_store_info wifi_info;
    // URL 信息
    char update_url[128];         //  URL
    char backup_url[128];         // 备用 URL

    u8 extension[48];            // 预留扩展
};

static struct reserved_wifi_config g_wifi_config;
static struct reserved_info_transfer reserved_addr_info = {0};

//不再通过boot->arch_info拿升级参数
static void reserved_addr_info_save(void)
{
    UPDATA_PARM *p;
    p = (u8 *)get_updata_ram_end_addr();

    struct reserved_info_transfer *info = (struct reserved_info_transfer *)p->parm_priv;
    reserved_addr_info.reserved_addr = info->reserved_addr;
    reserved_addr_info.reserved_len = info->reserved_len;

    printf("reserved_addr_info_save: addr=0x%x, len=0x%x\n",
           reserved_addr_info.reserved_addr, reserved_addr_info.reserved_len);
}

//RAM丢失reserved_addr时从flash搜索恢复
static int reserved_area_info_find(void)
{
    u32 app_code_addr = JL_SFC->BASE_ADR;
    u32 last_sector_addr = get_flash_capability();
    u32 reserved_addr = 0;
    u16 magic = 0;
    int find = 0;
    int search_cnt = 0;
    int max_search_cnt = 10010;

    //printf("reserved_area_info_find: last_sector_addr=0x%x, app_code_addr=0x%x\n",
    //      last_sector_addr, app_code_addr);

    if (reserved_addr_info.reserved_len > 0 && reserved_addr_info.reserved_addr > app_code_addr) {
        printf("reserved_addr_info is valid, addr=0x%x\n", reserved_addr_info.reserved_addr);
        return 1;
    }

    printf("reserved_addr_info is gone, try resume from flash end --\n");
    // 从 Flash 最后一个扇区向前遍历搜索
    reserved_addr = last_sector_addr - FLASH_SECTOR_SIZE;

    while (search_cnt < max_search_cnt && reserved_addr > app_code_addr) {
        //magic位于扇区首地址
        spi0flash_read((u8 *)&magic, reserved_addr, sizeof(magic));
        //printf("search sector 0x%x, magic=0x%x\n", reserved_addr, magic);

        if (magic == RESERVED_CONFIG_MAGIC) {
            //found
            printf("found reserved area backup at 0x%x\n", reserved_addr);

            //read
            spi0flash_read((u8 *)&g_wifi_config, reserved_addr, sizeof(g_wifi_config));

            //check magic again
            if (g_wifi_config.magic == RESERVED_CONFIG_MAGIC) {
                u16 crc = CRC16((u8 *)&g_wifi_config + 4, sizeof(g_wifi_config) - 4);
                if (g_wifi_config.crc == crc) {
                    printf("reserved config CRC OK\n");

                    //预留区地址放回RAM
                    reserved_addr_info.reserved_addr = reserved_addr;
                    reserved_addr_info.reserved_len = sizeof(g_wifi_config);

                    find = 1;
                    break;
                } else {

                    printf("reserved config crc err, continue search\n");
                }
            }

        } else {
            //printf("reserved config magic invalid: 0x%x, continue search\n", g_wifi_config.magic);
        }

        //next sector
        reserved_addr -= FLASH_SECTOR_SIZE;
        search_cnt++;

    }

    if (!find) {
        printf("reserved area not found after searching %d sectors\n", search_cnt);
    }

    return find;
}
// 从预留区加载 WiFi/URL 配置
void reserved_config_load(void)
{

    reserved_addr_info_save();

    // 尝试从 Flash 搜索恢复（如果 RAM 信息丢失）
    if (!reserved_area_info_find()) {
        printf("reserved_config_load: failed to find reserved area\n");
        return;
    }

    // 从Ram读配置
    spi0flash_read((u8 *)&g_wifi_config, reserved_addr_info.reserved_addr, sizeof(g_wifi_config));

    if (g_wifi_config.magic != RESERVED_CONFIG_MAGIC) {
        printf("config magic invalid: 0x%x\n", g_wifi_config.magic);
        return;
    }

    u32 crc = CRC16((u8 *)&g_wifi_config + 4, sizeof(g_wifi_config) - 4);
    if (g_wifi_config.crc != crc) {
        printf("config crc error\n");
        return;
    }

    memcpy(&wifi_reserved_info, &g_wifi_config.wifi_info, sizeof(struct wifi_store_info));

    printf("reserved config loaded:\n");
    printf("  mode=%d, ssid[0]=%s\n", wifi_reserved_info.mode, wifi_reserved_info.ssid[0]);
    printf("  update_url=%s\n", g_wifi_config.update_url);
}

// 获取 WiFi 配置
struct reserved_wifi_config *get_wifi_config(void)
{
    return &g_wifi_config;
}

char *reserved_http_get(void)
{
    if (g_wifi_config.update_url) {
        return g_wifi_config.update_url;
    }
    return NULL;
}

