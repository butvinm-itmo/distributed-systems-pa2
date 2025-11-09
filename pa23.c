
#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "banking.h"
#include "common.h"
#include "ipc.h"
#include "pa1.h"
#include "worker.h"

#define defer_return(r) \
    do {                \
        result = r;     \
        goto defer;     \
    } while (0)

typedef struct {
    Worker* worker;
    AllHistory history;
} BankClientWorker;

typedef struct {
    Worker* worker;
    BalanceState* balance;
    BalanceHistory history;
} BankAccountWorker;

int execute_bank_account_worker(BankAccountWorker s) {
    Worker* w = s.worker;
    Message msg;
    size_t started = 0;
    size_t done = 0;

    msg = (Message) { .s_header = { .s_magic = MESSAGE_MAGIC, .s_type = STARTED, .s_local_time = w->id } };
    sprintf(msg.s_payload, log_started_fmt, w->id, getpid(), getppid());
    msg.s_header.s_payload_len = strlen(msg.s_payload);
    if (send_multicast(w, &msg) != 0) {
        fprintf(w->events_log, "Process %1d failed to multicast STARTED message: %s\n", w->id, strerror(errno));
        fprintf(stderr, "Process %1d failed to multicast STARTED message: %s\n", w->id, strerror(errno));
        fflush(w->events_log);
        return 1;
    }
    fwrite(msg.s_payload, sizeof(char), msg.s_header.s_payload_len, w->events_log);
    fwrite(msg.s_payload, sizeof(char), msg.s_header.s_payload_len, stdout);
    fflush(w->events_log);

    while (started != w->nbr_count - 1) {
        if (receive_any(w, &msg) != 0) {
            fprintf(w->events_log, "Process %1d failed to receive message: %s\n", w->id, strerror(errno));
            fprintf(stderr, "Process %1d failed to receive message: %s\n", w->id, strerror(errno));
            fflush(w->events_log);
            return 1;
        }
        if (msg.s_header.s_type == STARTED) started++;
        if (msg.s_header.s_type == DONE) done++;
    }
    fprintf(w->events_log, log_received_all_started_fmt, w->id);
    fprintf(stdout, log_received_all_started_fmt, w->id);
    fflush(w->events_log);

    msg = (Message) { .s_header = { .s_magic = MESSAGE_MAGIC, .s_type = DONE, .s_local_time = w->id } };
    sprintf(msg.s_payload, log_done_fmt, w->id);
    msg.s_header.s_payload_len = strlen(msg.s_payload);
    if (send_multicast(w, &msg) != 0) {
        fprintf(w->events_log, "Process %1d failed to multicast DONE message: %s\n", w->id, strerror(errno));
        fprintf(stderr, "Process %1d failed to multicast DONE message: %s\n", w->id, strerror(errno));
        fflush(w->events_log);
        return 1;
    }
    fwrite(msg.s_payload, sizeof(char), msg.s_header.s_payload_len, w->events_log);
    fwrite(msg.s_payload, sizeof(char), msg.s_header.s_payload_len, stdout);
    fflush(w->events_log);

    while (done != w->nbr_count - 1) {
        if (receive_any(w, &msg) != 0) {
            fprintf(w->events_log, "Process %1d failed to receive message: %s\n", w->id, strerror(errno));
            fprintf(stderr, "Process %1d failed to receive message: %s\n", w->id, strerror(errno));
            fflush(w->events_log);
            return 1;
        }
        if (msg.s_header.s_type == DONE) done++;
    }
    fprintf(w->events_log, log_received_all_done_fmt, w->id);
    fprintf(stdout, log_received_all_done_fmt, w->id);
    fflush(w->events_log);

    return 0;
}

int execute_bank_client_worker(BankClientWorker s) {
    Worker* w = s.worker;
    Message msg;
    size_t started = 0;
    size_t done = 0;

    while (started != w->nbr_count) {
        if (receive_any(w, &msg) != 0) {
            fprintf(w->events_log, "Process %1d failed to receive message: %s\n", w->id, strerror(errno));
            fprintf(stderr, "Process %1d failed to receive message: %s\n", w->id, strerror(errno));
            fflush(w->events_log);
            return 1;
        }
        if (msg.s_header.s_type == STARTED) started++;
        if (msg.s_header.s_type == DONE) done++;
    }
    fprintf(w->events_log, log_received_all_started_fmt, w->id);
    fprintf(stdout, log_received_all_started_fmt, w->id);
    fflush(w->events_log);

    while (done != w->nbr_count) {
        if (receive_any(w, &msg) != 0) {
            fprintf(w->events_log, "Process %1d failed to receive message: %s\n", w->id, strerror(errno));
            fprintf(stderr, "Process %1d failed to receive message: %s\n", w->id, strerror(errno));
            fflush(w->events_log);
            return 1;
        }
        if (msg.s_header.s_type == DONE) done++;
    }
    fprintf(w->events_log, log_received_all_done_fmt, w->id);
    fprintf(stdout, log_received_all_done_fmt, w->id);
    fflush(w->events_log);

    return 0;
}

void transfer(void* parent_data, local_id src, local_id dst, balance_t amount) {
}

typedef struct {
    bool ok;
    int bank_account_workers_count; // number of bank account processes
    balance_t initial_balances[MAX_PROCESS_ID + 1]; // initial account balances
} CliArgs;

CliArgs arg_parse(int argc, char** argv) {
    CliArgs args = { .ok = false };

    if (argc < 3) {
        fprintf(stderr, "usage: %s -p X <B1..BX>\n", argv[0]);
        return args;
    }

    if (strcmp(argv[1], "-p") != 0) {
        fprintf(stderr, "usage: %s -p X <B1..BX>\n", argv[0]);
        return args;
    }

    args.bank_account_workers_count = atoi(argv[2]);
    if (args.bank_account_workers_count <= 0) {
        fprintf(stderr, "error: Number of processes must be a positive integer\n");
        return args;
    }

    if (argc != 3 + args.bank_account_workers_count) {
        fprintf(stderr, "error: Process and balances number mismatch\n");
        return args;
    }
    for (int i = PARENT_ID; i <= args.bank_account_workers_count; i++) {
        args.initial_balances[i] = atoi(argv[3 + i - 1]);
        if (args.initial_balances[i] <= 0) {
            fprintf(stderr, "error: Balance must be a positive integer\n");
            return args;
        }
    }

    args.ok = true;
    return args;
}

int main(int argc, char** argv) {
    int result = 0;
    Worker* workers = NULL;
    Worker* w;

    CliArgs args = arg_parse(argc, argv);
    if (!args.ok) return 1;

    FILE* pipes_log_fd = fopen(pipes_log, "a");
    if (pipes_log_fd == NULL) {
        fprintf(stderr, "Failed to open file %s: %s", pipes_log, strerror(errno));
        return 1;
    }

    FILE* events_log_fd = fopen(events_log, "a");
    if (events_log_fd == NULL) {
        fprintf(stderr, "Failed to open file %s: %s", events_log, strerror(errno));
        return 1;
    }

    workers = calloc(args.bank_account_workers_count + 1, sizeof(Worker));
    if (init_workers(workers, args.bank_account_workers_count, events_log_fd, pipes_log_fd) != 0) defer_return(1);
    fflush(pipes_log_fd); // flush to avoid writing the same buffer again from workers

    for (worker_id worker_id = PARENT_ID + 1; worker_id < args.bank_account_workers_count + 1; worker_id++) {
        int worker_pid = fork();
        switch (worker_pid) {
        case -1: {
            fprintf(stderr, "Failed to fork a new process for worker %d: %s\n", worker_id, strerror(errno));
            defer_return(2);
        } break;
        case 0: {
            w = &workers[worker_id];
            BankAccountWorker bank_account_worker = {
                .worker = w,
                .history = {
                    .s_id = worker_id,
                    .s_history_len = 0,
                    .s_history = { [0] = { .s_time = 0, .s_balance = args.initial_balances[worker_id], .s_balance_pending_in = 0 } } }
            };
            bank_account_worker.balance = &bank_account_worker.history.s_history[0];
            deinit_unused_channels(w, workers, pipes_log_fd);

            int status = execute_bank_account_worker(bank_account_worker);
            defer_return(status);
        } break;
        }
    }

    w = &workers[PARENT_ID];
    BankClientWorker bank_client_worker = { .worker = w };
    deinit_unused_channels(bank_client_worker.worker, workers, pipes_log_fd);

    int status = execute_bank_client_worker(bank_client_worker);
    while (wait(NULL) > 0);
    defer_return(status);

defer:
    if (workers != NULL) deinit_workers(w, workers, pipes_log_fd);
    fclose(pipes_log_fd);
    fclose(events_log_fd);
    return result;
}
