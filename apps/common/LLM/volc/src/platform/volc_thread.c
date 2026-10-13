#include "volc_thread.h"

#include <unistd.h>
#include "pthread.h"
#include "os/os_api.h"
#include "volc_errno.h"
#include "volc_memory.h"
#include "volc_time.h"
#include "volc_type.h"

volc_thread_local_t g_thread_local = NULL;

static void _volc_thread_local_delete_key(volc_thread_local_t local)
{
    (void)local;
}

#include "spinlock.h"

DEFINE_SPINLOCK(volc_lock);
static int volc_thread_pid[32];
static int main_thread_pid = 0;

int *get_volc_thread_pid(void)
{
    spin_lock(&volc_lock);
    int i = 0;
    for (i = 0; i < 32; i++) {
        if (volc_thread_pid[i] == 0) {
            volc_thread_pid[i]  = !0;
            break;
        }
    }
    if (i == 32) {
        spin_unlock(&volc_lock);
        return NULL;
    }
    spin_unlock(&volc_lock);
    return &volc_thread_pid[i];
}

volc_tid_t volc_thread_get_id(void)
{
    int my_pid = get_cur_thread_pid();

    spin_lock(&volc_lock);
    for (int i = 0; i < 16; i++) {
        if (volc_thread_pid[i] == my_pid) {
            spin_unlock(&volc_lock);
            return (volc_tid_t)&volc_thread_pid[i];
        }
    }
    spin_unlock(&volc_lock);

    main_thread_pid = my_pid;
    return (volc_tid_t)&main_thread_pid;
}

uint32_t volc_thread_set_name(const char *name)
{
    volc_debug("%s, %s\n", __FUNCTION__, name);
    (void)name;
    return VOLC_SUCCESS;
}

uint32_t volc_thread_create(volc_tid_t *thread, const volc_thread_param_t *param, void *(*start_routine)(void *), void *args)
{
    int ret = 0;
    int stack_size = 0;
    int priority = 0;

    if (NULL == thread || NULL == start_routine) {
        return VOLC_FAILED;
    }

    if (NULL != param) {
        priority = (param->priority == -1) ? 5 : param->priority;
        stack_size = param->stack_size;
    } else {
        stack_size = 8192;
        priority = 3;
    }

    if (stack_size < 32768) {
        stack_size = 32768;
    }

    int *handle = get_volc_thread_pid();
    if (handle == NULL) {
        return VOLC_FAILED;
    }

    ret = thread_fork(param->name ? param->name : "v_th", priority, stack_size / sizeof(int), 0, handle, (void (*)(void *))start_routine, args);
    *thread = handle;

    if (0 != ret) {
        *handle = 0;
        return VOLC_FAILED;
    }
    return VOLC_SUCCESS;
}

void volc_thread_exit(volc_tid_t thread)
{
    if (NULL == thread) {
        return;
    }

    int *pid_ptr = (int *)thread;
    if (pid_ptr == &main_thread_pid) {
        return;
    }

    int target_pid = *pid_ptr;
    if (target_pid == 0) {
        return;
    }

    if (target_pid == get_cur_thread_pid()) {
        *pid_ptr = 0;
        thread_kill(&target_pid, 0);
    } else {
        thread_kill(pid_ptr, KILL_WAIT);
        *pid_ptr = 0;
    }
}

uint32_t volc_thread_join(volc_tid_t thread, void *ret)
{
    (void)ret;
    if (NULL == thread) {
        return VOLC_FAILED;
    }

    int *pid_ptr = (int *)thread;
    if (pid_ptr == &main_thread_pid) {
        return VOLC_SUCCESS;
    }

    if (*pid_ptr != 0 && *pid_ptr != get_cur_thread_pid()) {
        thread_kill(pid_ptr, KILL_WAIT);
        *pid_ptr = 0;
    }
    return VOLC_SUCCESS;
}

void volc_thread_destroy(volc_tid_t thread)
{
    if (NULL == thread) {
        return;
    }
    int *pid_ptr = (int *)thread;
    if (pid_ptr != &main_thread_pid) {
        *pid_ptr = 0;
    }
}

uint32_t volc_thread_detach(volc_tid_t thread)
{
    (void)thread;
    return VOLC_SUCCESS;
}

void volc_thread_sleep(uint64_t time)
{
    uint64_t remaining_time = time / VOLC_HUNDREDS_OF_NANOS_IN_A_MICROSECOND;
    while (0 != remaining_time) {
        if (remaining_time <= VOLC_MAX_UINT32) {
            usleep(remaining_time);
            remaining_time = 0;
        } else {
            usleep(VOLC_MAX_UINT32);
            remaining_time -= (uint64_t)VOLC_MAX_UINT32;
        }
    }
}

void volc_thread_sleep_until(uint64_t time)
{
    uint64_t cur_time = volc_get_time_ms() * VOLC_HUNDREDS_OF_NANOS_IN_A_MILLISECOND;
    if (time > cur_time) {
        volc_thread_sleep(time - cur_time);
    }
}

volc_thread_local_t volc_thread_local_create(void)
{
    pthread_key_t *key = (pthread_key_t *)volc_calloc(1, sizeof(pthread_key_t));
    if (NULL == key) {
        return NULL;
    }
    pthread_key_create(key, _volc_thread_local_delete_key);
    return (volc_thread_local_t)key;
}

uint32_t volc_thread_local_destroy(volc_thread_local_t local)
{
    if (NULL == local) {
        return VOLC_FAILED;
    }
    pthread_key_delete(*(pthread_key_t *)local);
    volc_free(local);
    return VOLC_SUCCESS;
}

uint32_t volc_thread_local_set(volc_thread_local_t local, void *data)
{
    if (NULL == local || NULL == data) {
        return VOLC_FAILED;
    }
    pthread_setspecific(*(pthread_key_t *)local, data);
    return VOLC_SUCCESS;
}

uint32_t volc_thread_local_clean(volc_thread_local_t local)
{
    if (NULL == local) {
        return VOLC_FAILED;
    }
    pthread_setspecific(*(pthread_key_t *)local, NULL);
    return VOLC_SUCCESS;
}

void *volc_thread_local_get(volc_thread_local_t local)
{
    if (NULL == local) {
        return NULL;
    }
    return pthread_getspecific(*(pthread_key_t *)local);
}
