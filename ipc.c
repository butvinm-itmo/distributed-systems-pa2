#define _POSIX_C_SOURCE 199309L
#include "ipc.h"
#include "worker.h"
#include <assert.h>
#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

#define spin()                                                   \
    do {                                                         \
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 100000 }; \
        nanosleep(&ts, NULL);                                    \
    } while (0)

static int _write_all(int fd, const char* buf, size_t size);

static int _read_all(int fd, char* buf, size_t size);

int send(void* self, local_id dst, const Message* msg) {
    Worker* s = self;
    int res;
    assert((s->id != dst) && "Send to self");
    assert((msg->s_header.s_payload_len <= MAX_PAYLOAD_LEN) && "Message payload len is bigger than MAX_PAYLOAD_LEN");

    res = _write_all(s->chs[dst].write_fd, (const char*)msg, sizeof(msg->s_header) + msg->s_header.s_payload_len);
    if (res != 0) return res;

    return 0;
}

int send_multicast(void* self, const Message* msg) {
    Worker* s = self;
    for (worker_id nbr_id = 0; nbr_id < s->nbr_count + 1; nbr_id++) {
        if (nbr_id == s->id) continue;
        int result = send(self, nbr_id, msg);
        if (result != 0) return result;
    }
    return 0;
}

int receive(void* self, local_id from, Message* msg) {
    Worker* s = self;
    assert((s->id != from) && "Send to self");
    int res;

    res = _read_all(s->chs[from].read_fd, (char*)msg, sizeof(msg->s_header));
    if (res != 0) return res;
    assert((msg->s_header.s_magic == MESSAGE_MAGIC) && "Bad message magic");

    res = _read_all(s->chs[from].read_fd, (char*)msg + sizeof(msg->s_header), msg->s_header.s_payload_len);
    if (res != 0) return res;

    return 0;
}

int receive_any(void* self, Message* msg) {
    Worker* s = self;
    int res;

    while (1) {
        for (worker_id nbr_id = 0; nbr_id < s->nbr_count + 1; nbr_id++) {
            if (nbr_id == s->id) continue;

            ssize_t recv = read(s->chs[nbr_id].read_fd, (char*)msg, sizeof(msg->s_header));
            if (recv > 0) {
                res = _read_all(s->chs[nbr_id].read_fd, (char*)msg + recv, sizeof(msg->s_header) - recv);
                if (res != 0) return res;
                assert((msg->s_header.s_magic == MESSAGE_MAGIC) && "Bad message magic");

                res = _read_all(s->chs[nbr_id].read_fd, (char*)msg + sizeof(msg->s_header), msg->s_header.s_payload_len);
                if (res != 0) return res;

                return 0;
            } else if (recv < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    continue;
                } else if (errno == EINTR) {
                    nbr_id--;
                } else {
                    return -1;
                }
            }
        }
        spin();
    }
    return 0;
}

static int _read_all(int fd, char* buf, size_t size) {
    size_t recv_total = 0;
    while (recv_total < size) {
        ssize_t recv = read(fd, buf + recv_total, size - recv_total);
        if (recv > 0) {
            recv_total += recv;
        } else if (recv < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                spin();
                continue;
            } else if (errno == EINTR) {
                continue;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }
    return 0;
}

static int _write_all(int fd, const char* buf, size_t size) {
    size_t sent_total = 0;
    while (sent_total < size) {
        ssize_t sent = write(fd, buf + sent_total, size - sent_total);
        if (sent > 0) {
            sent_total += sent;
        } else if (sent < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                spin();
                continue;
            } else if (errno == EINTR) {
                continue;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }
    return 0;
}
