#include "update.h"
#include "app_config.h"
#include "system/includes.h"
#include "fs/fs.h"
#include "asm/sfc_norflash_api.h"

#ifdef CONFIG_NET_UPGRADE_INFO_USE_RESERVED


static u32 reserved_flash_addr = 0;
static u32 reserved_flash_len = 0;

#define RESERVED_FILE_NAME    "mnt/sdfile/app/HttpUpdate"
// 获取预留区地址（HttpUpdate 文件区域）
void reserved_config_addr_get(u32 *addr, u32 *len)
{
    if (reserved_flash_addr != 0) {
        *addr = reserved_flash_addr;
        *len = reserved_flash_len;
        return;
    }

    FILE *fp = fopen(RESERVED_FILE_NAME, "r");
    if (fp == NULL) {
        printf("reserved file not found: %s\n", RESERVED_FILE_NAME);
        *addr = 0;
        *len = 0;
        return;
    }

    struct vfs_attr attr;
    fget_attrs(fp, &attr);

    // 转换为 flash 地址
    reserved_flash_addr = sdfile_cpu_addr2flash_addr(attr.sclust);
    reserved_flash_len = attr.fsize;

    *addr = reserved_flash_addr;
    *len = reserved_flash_len;

    fclose(fp);
    printf("reserved: addr=0x%x, len=0x%x\n", *addr, *len);
}

// 写配置到预留区
int reserved_wifi_config_write(struct reserved_wifi_config *config)
{
    u32 addr, len;
    reserved_config_addr_get(&addr, &len);

    if (addr == 0 || len < sizeof(struct reserved_wifi_config)) {
        printf("reserved write failed: addr=0x%x, len=0x%x, need=%d\n",
               addr, len, sizeof(struct reserved_wifi_config));
        return -1;
    }

    // 设置魔数和 CRC
    config->magic = RESERVED_CONFIG_MAGIC;
    config->crc = CRC16((u8 *)config + 4, sizeof(*config) - 4);

    // 擦除并写入
    norflash_ioctl(NULL, IOCTL_ERASE_SECTOR, addr);
    norflash_write(NULL, (u8 *)config, sizeof(*config), addr);

    printf("reserved config write success:\n");
    printf("  ssid[0]=%s, ssid[1]=%s\n", config->wifi_info.ssid[0], config->wifi_info.ssid[1]);
    printf("  update_url=%s\n", config->update_url);
    return 0;
}

// 从预留区读取 WiFi 配置
int reserved_wifi_config_read(struct reserved_wifi_config *config)
{
    u32 addr, len;
    reserved_config_addr_get(&addr, &len);

    if (addr == 0) {
        return -1;
    }

    norflash_read(NULL, (u8 *)config, sizeof(*config), addr);

    if (config->magic != RESERVED_CONFIG_MAGIC) {
        printf("reserved config magic invalid: 0x%x\n", config->magic);
        return -1;
    }

    u16 crc = CRC16((u8 *)config + 4, sizeof(*config) - 4);
    if (config->crc != crc) {
        printf("reserved config crc error: 0x%x != 0x%x\n", config->crc, crc);
        return -1;
    }

    printf("reserved config read success:\n");
    printf("  ssid[0]=%s, ssid[1]=%s\n", config->wifi_info.ssid[0], config->wifi_info.ssid[1]);
    printf("  update_url=%s\n", config->update_url);

    return 0;
}

#endif
