#include "schedy.h"

#include <assert.h>
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "coroed/api/task.h"
#include "coroed/core/relax.h"
#include "coroed/core/spinlock.h"
#include "kthread.h"
#include "uthread.h"

enum {
  /**
   * Максимальное количество файберов, которое может
   * одновременно выполняться на планировщике.
   */
  SCHED_THREADS_LIMIT = 512,

  /**
   * Количество рабочих потоков для исполнения файберов.
   */
  SCHED_WORKERS_COUNT = (size_t)(8),

  /**
   * Обеспечивает костыль для ретрая операций
   * `submit` и `acquire_next` при высокой
   * конкуренции на спинлоках файберов.
   */
  SCHED_NEXT_MAX_ATTEMPTS = (size_t)(16),
};

/**
 * Задача, выполняющаяся на планировщике.
 *
 * Hint: также тут можно хранить список задач, ожидающих
 *       завершения этой, чтобы уведомить их о данном
 *       событии.
 */
struct task {
  /**
   * Контекст исполнения.
   */
  struct uthread* thread;

  /**
   * Установлен при `UTHREAD_RUNNING`. Указывает на
   * рабочий поток, на котором исполняется задача.
   * Необходим для получения контекста локального
   * планировщика при операциях `yield`, `await`, `submit`.
   */
  struct worker* worker;

  enum {
    /** Готова к исполнению. */
    UTHREAD_RUNNABLE,

    /** Прямо сейчас выполняется. */
    UTHREAD_RUNNING,

    /** Завершена и скоро станет зомби. */
    UTHREAD_FINISHED,

    /** Отработала и может быть переиспользования. */
    UTHREAD_ZOMBIE,

    /** Блокирован и ждет завершения event */
    UTHREAD_BLOCKED,
  } state;  // Текущее состояние задачи

  /**
   * Защищает поля структуры от неупорядоченного доступа.
   */
  struct spinlock lock;
  struct {
    int is_created;
    unsigned long long state_update_time;
    unsigned long long running_time;
    unsigned long long runnable_time;
    unsigned long long blocked_time;
  } stats;
  
};

/**
 * Рабочий поток, исполняющий файберы.
 */
struct worker {
  /**
   * Идентификатор рабочего потока.
   */
  size_t index;

  /**
   * Поток операционной системы, на котором
   * исполняется рабочий.
   */
  struct kthread kthread;

  /**
   * Контекст планировщика. Нужно переключиться на него,
   * чтобы вернуться в планировщик.
   */
  struct uthread sched_thread;

  /**
   * В данный момент исполняемая на рабочем
   * задача. Может быть `NULL`.
   */
  struct task* running_task;

  struct {
    size_t steps;     // Сколько шагов было выполнено
    size_t finished;  // Сколько задач было завершено
  } statistics;       // Локальная статистика работяги
};

static struct {
  atomic_size_t tasks_finished;
  atomic_size_t tasks_runnable;
  atomic_size_t tasks_running;
  atomic_size_t tasks_blocked;
  atomic_uint_least64_t total_running_time;
  atomic_uint_least64_t total_runnable_time;
  atomic_uint_least64_t total_blocked_time;
} global_stats = {0};

static unsigned long long get_current_time() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (unsigned long long)ts.tv_sec * 1000000000ULL + (unsigned long long)ts.tv_nsec;
}

static void update_stats(struct task* task, int updated_state){
  unsigned long long cur_time = get_current_time();
  //printf("cur_time: %lu; last_time: %lu", cur_time, task->stats.state_update_time);
  unsigned long long dif_between_updates = cur_time - task->stats.state_update_time;
  switch (task->state)
  {
  case UTHREAD_RUNNABLE:
    task->stats.runnable_time+=dif_between_updates;
    atomic_fetch_add(&global_stats.total_runnable_time, dif_between_updates);
    atomic_fetch_sub(&global_stats.tasks_runnable, 1);
    break;
  case UTHREAD_RUNNING:
    task->stats.running_time+=dif_between_updates;
    atomic_fetch_add(&global_stats.total_running_time, dif_between_updates);
    atomic_fetch_sub(&global_stats.tasks_running, 1);
    break;
  case UTHREAD_BLOCKED:
    task->stats.blocked_time+=dif_between_updates;
    atomic_fetch_add(&global_stats.total_blocked_time, dif_between_updates);
    atomic_fetch_sub(&global_stats.tasks_blocked, 1);
    break;
  case UTHREAD_FINISHED:
      atomic_fetch_add(&global_stats.tasks_finished, 1);
      break;
  default:
    break;
  }
  switch (updated_state) {
    case UTHREAD_RUNNING:
      atomic_fetch_add(&global_stats.tasks_running, 1);
      break;
    case UTHREAD_RUNNABLE:
      atomic_fetch_add(&global_stats.tasks_runnable, 1);
      break;
    case UTHREAD_BLOCKED:
      atomic_fetch_add(&global_stats.tasks_blocked, 1);
      break;
    default:
      break;
  }
  task->stats.state_update_time = cur_time;
}
static struct spinlock tasks_lock;              // Защищает список задач
static size_t next_task_index = 0;              // Для планирования round-robin
static struct task tasks[SCHED_THREADS_LIMIT];  // Список всех задач

// Hint: для реализации более сложных схем управления, вам
//       вам могут понадобиться связаные списки (`core/list.h`).

static kthread_id_t kthread_ids[SCHED_WORKERS_COUNT];
static struct worker workers[SCHED_WORKERS_COUNT];

/**
 * Установить задачу в пустое состояние.
 */
void sched_task_init(struct task* task) {
  task->thread = NULL;
  task->worker = NULL;
  task->state = UTHREAD_ZOMBIE;
  task->stats.is_created = 1;
  task->stats.running_time = 0;
  task->stats.runnable_time = 0;
  task->stats.blocked_time = 0;
  task->stats.state_update_time = get_current_time();
  spinlock_init(&task->lock);
}

/**
 * Установить рабочего в пустое состояние.
 */
void sched_worker_init(struct worker* worker, size_t index) {
  worker->index = index;
  worker->sched_thread.context = NULL;
  worker->running_task = NULL;
  worker->statistics.steps = 0;
  worker->statistics.finished = 0;
}

void sched_init() {
  spinlock_init(&tasks_lock);
  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    sched_task_init(&tasks[i]);
  }
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    kthread_ids[i] = 0;
    sched_worker_init(&workers[i], i);
  }
}

/**
 * Находясь в контексте задачи `task`, переключиться
 * в контекст планировщика.
 */
void sched_switch_to_scheduler(struct task* task) {
  struct uthread* sched = &task->worker->sched_thread;
  task->worker = NULL;
  uthread_switch(task->thread, sched);
}

/**
 * Находясь в контексте планировщика, переключиться
 * в контекст задачи `task`. Выполнять ее до следующего
 * невынужденного возвращения в планировщик.
 */
void sched_switch_to(struct worker* worker, struct task* task) {
  assert(task->thread != &worker->sched_thread);

  update_stats(task, UTHREAD_RUNNING);
  task->state = UTHREAD_RUNNING;

  task->worker = worker;
  worker->running_task = task;

  struct uthread* sched = &worker->sched_thread;
  uthread_switch(sched, task->thread);
}

/**
 * Получить следующую задачу на исполнение в
 * соответствии с текущим алгоритмом планирования.
 *
 * Вызывающему передается владение задачей, а также
 * захваченный лок `task->lock`.
 */
struct task* sched_acquire_next();

/**
 * Вернуть задачу в очередь планирования.
 *
 * Очереди передается владение задачей,
 * а также она отпустит лок `task->lock`.
 */
void sched_release(struct task* task);

/**
 * Цикл планировщика. Выполняется, пока есть задачи.
 */
int sched_loop(void* argument) {
  struct worker* worker = argument;
  kthread_ids[worker->index] = kthread_id();

  for (;;) {
    struct task* task = sched_acquire_next();
    if (task == NULL) {
      break;
    }

    sched_switch_to(worker, task);

    worker->statistics.steps += 1;
    if (task->state == UTHREAD_FINISHED) {
      worker->statistics.finished += 1;
    }

    sched_release(task);

    // Hint: где-то здесь можно было бы опросить
    //       механизмы для неблокирующего ввода-вывода
    //       и перевести удовлетворенные BLOCKED
    //       потоки в RUNNABLE состояние.
  }

  return 0;
}

struct task* sched_acquire_next() {
  // На всякий случай пытаемся найти задачу несколько раз,
  // так как какие-то `task->lock` могли быть отпущены.

  for (size_t attempt = 0; attempt < SCHED_NEXT_MAX_ATTEMPTS; ++attempt) {
    spinlock_lock(&tasks_lock);  // Защитим `next_task_index`

    for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
      struct task* task = &tasks[next_task_index];
      if (!spinlock_try_lock(&task->lock)) {
        continue;
      }

      // Планирование round-robin
      next_task_index = (next_task_index + 1) % SCHED_THREADS_LIMIT;

      if (task->thread != NULL && (task->state == UTHREAD_RUNNABLE || task->state == UTHREAD_BLOCKED)) {
        spinlock_unlock(&tasks_lock);
        return task;
      }

      spinlock_unlock(&task->lock);
    }

    spinlock_unlock(&tasks_lock);
    SPINLOOP(2 * attempt);
  }

  return NULL;
}

void sched_release(struct task* task) {
  task->worker = NULL;
  if (task->state == UTHREAD_FINISHED) {
    // Отправляем задачу на кладбище, а могли бы
    // еще, например, разблокировать зависимые задачи.
        update_stats(task, UTHREAD_ZOMBIE);
    uthread_reset(task->thread);

    task->state = UTHREAD_ZOMBIE;

  } else if (task->state == UTHREAD_RUNNING) {
    update_stats(task, UTHREAD_RUNNABLE);
    task->state = UTHREAD_RUNNABLE;

  } else if (task->state == UTHREAD_BLOCKED){
        //update_stats(task, UTHREAD_BLOCKED);
  }
  else {
    assert(false && "Not implemented");
  }
  spinlock_unlock(&task->lock);
}

/**
 * Отметить задачу завершенной.
 */
void sched_finish(struct task* task) {
    update_stats(task, UTHREAD_FINISHED);
  task->state = UTHREAD_FINISHED;

}

void task_yield(struct task* caller) {
  //update_stats(task, task->state);
  sched_switch_to_scheduler(caller);
}
void task_block(struct task* caller) {
  update_stats(caller, UTHREAD_BLOCKED);
  caller->state = UTHREAD_BLOCKED;
  sched_switch_to_scheduler(caller);
}

void task_unblock(struct task* caller) {
  if (caller->state == UTHREAD_BLOCKED) {
    update_stats(caller, UTHREAD_RUNNABLE);
    caller->state = UTHREAD_RUNNABLE;
  }
}

void task_exit(struct task* caller) {
  sched_finish(caller);
  task_yield(caller);
}

task_t task_submit(struct task* caller, uthread_routine entry, void* argument) {
  (void)caller;  // Может быть полезно.
  task_t child = sched_submit(*entry, argument);
  return child;
}

task_t sched_try_submit(void (*entry)(), void* argument) {
  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    struct task* task = &tasks[i];

    if (!spinlock_try_lock(&task->lock)) {
      continue;
    }

    if (task->thread == NULL) {
      task->thread = uthread_allocate();
      assert(task->thread != NULL);
                update_stats(task, UTHREAD_ZOMBIE);
      task->state = UTHREAD_ZOMBIE;

    }

    const bool is_submitted = task->state == UTHREAD_ZOMBIE;

    if (task->state == UTHREAD_ZOMBIE) {
      uthread_reset(task->thread);
      uthread_set_entry(task->thread, entry);
      uthread_set_arg_0(task->thread, task);
      uthread_set_arg_1(task->thread, argument);

      task->stats.is_created = 1;
      task->stats.running_time = 0;
      task->stats.runnable_time = 0;
      task->stats.blocked_time = 0;
      task->stats.state_update_time = get_current_time();

      update_stats(task, UTHREAD_RUNNABLE);
            task->state = UTHREAD_RUNNABLE;
      //update_stats(task, UTHREAD_RUNNABLE);
    }

    spinlock_unlock(&task->lock);
    if (is_submitted) {
      return (task_t){.task = task};
    }
  }

  return (task_t){.task = NULL};
}

task_t sched_submit(void (*entry)(), void* argument) {
  for (size_t attempt = 0; attempt < SCHED_NEXT_MAX_ATTEMPTS; ++attempt) {
    task_t handle = sched_try_submit(entry, argument);
    if (handle.task != NULL) {
      return handle;
    }
    SPINLOOP(2 * attempt);
  }

  assert(false && "Can't create a task");
}

void sched_start() {
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    enum kthread_status status = kthread_create(&worker->kthread, sched_loop, worker);
    assert(status == KTHREAD_SUCCESS);
  }
}

void sched_wait() {
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    enum kthread_status status = kthread_join(&worker->kthread);
    assert(status == KTHREAD_SUCCESS);
  }
}

void sched_print_statistics() {
  printf("Statistics\n");

  size_t steps_count = 0;
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    steps_count += worker->statistics.steps;
  }

  printf("Global scheduler stats:\n");
  printf("  Total tasks executed:     %zu\n", atomic_load(&global_stats.tasks_finished));
  printf("  Total steps done:         %zu\n", steps_count);
  printf("  Tasks currently runnable: %zu\n", atomic_load(&global_stats.tasks_runnable));
  printf("  Tasks currently running:  %zu\n", atomic_load(&global_stats.tasks_running));
  printf("  Tasks currently blocked:  %zu\n", atomic_load(&global_stats.tasks_blocked));
  printf("\n");

  uint64_t total_running = atomic_load(&global_stats.total_running_time);
  uint64_t total_runnable = atomic_load(&global_stats.total_runnable_time);
  uint64_t total_blocked = atomic_load(&global_stats.total_blocked_time);

  printf("Total time across all tasks:\n");
  printf("  Total RUNNING time:  %.6f ms\n", total_running / 1000000.0);
  printf("  Total RUNNABLE time: %.6f ms\n", total_runnable / 1000000.0);
  printf("  Total BLOCKED time:  %.6f ms\n", total_blocked / 1000000.0);
  printf("\n");

  printf("Worker statistics:\n");
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    printf("  Worker %zu (thread ID: %zu)\n", i, kthread_ids[i]);
    printf("    Steps executed:      %zu\n", worker->statistics.steps);
    printf("    Tasks finished:      %zu\n", worker->statistics.finished);
  }
  printf("\n");

  printf("Task statistics:\n");
  size_t task_num = 0;
  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    struct task* task = &tasks[i];
    spinlock_lock(&task->lock);

    if (task->thread != NULL && task->stats.is_created == 1) {
      unsigned long long running_time = task->stats.running_time;
      unsigned long long runnable_time = task->stats.runnable_time;
      unsigned long long blocked_time = task->stats.blocked_time;
      unsigned long long total_time = running_time + runnable_time + blocked_time;

      printf("  Task %zu:\n", task_num++);
      printf("    RUNNING time:    %.6f ms (%.2f%%)\n",
             running_time / 1000000.0,
             total_time > 0 ? (running_time * 100.0 / total_time) : 0.0);
      printf("    RUNNABLE time:   %.6f ms (%.2f%%)\n",
             runnable_time / 1000000.0,
             total_time > 0 ? (runnable_time * 100.0 / total_time) : 0.0);
      printf("    BLOCKED time:    %.6f ms (%.2f%%)\n",
             blocked_time / 1000000.0,
             total_time > 0 ? (blocked_time * 100.0 / total_time) : 0.0);
      printf("    Total time:      %.6f ms\n", total_time / 1000000.0);
    }

    spinlock_unlock(&task->lock);
  }
}

void sched_destroy() {
  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    struct task* task = &tasks[i];
    spinlock_lock(&task->lock);
    if (task->thread != NULL) {
      uthread_free(task->thread);
    }
    spinlock_unlock(&task->lock);
  }
}
