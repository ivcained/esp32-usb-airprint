// USB printer class host driver (single device, no hub support).
//
// Tasks:
//   usb_lib     - runs usb_host_lib_handle_events()
//   usb_client  - runs usb_host_client_handle_events(); opens/claims/tears
//                 down the printer and executes all transfer callbacks
// A print job runs in its own task and talks to the printer through
// usb_printer_write()/flush()/wait_response(). A scan uses the device's
// vendor interface through usb_scanner_*(); it holds the device like a job.
#include "local_config.h"
#include "usb_printer.h"

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

#ifdef PRINTER_RELAY
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>
#endif

#include "esp_check.h"
#include "esp_log.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "freertos/task.h"
#include "usb/usb_helpers.h"
#include "usb/usb_host.h"

static const char *TAG = "usb_printer";

#define OUT_XFER_SIZE 16384     // two of these: one filling while one is sent
#define OUT_XFER_COUNT 2
#define IN_XFER_SIZE 512        // multiple of the 64-byte full-speed MPS
#define CTRL_XFER_SIZE (sizeof(usb_setup_packet_t) + 1024)
#define RESP_BUF_SIZE 2048
#define MSG_BUF_SIZE 4096       // reassembly of one XML reply (status is ~1 KB)
#define IN_POLL_IDLE_MS 2000    // back-channel poll interval; every read returns
#define IN_POLL_JOB_MS 250      // a fresh ~1 KB status, so don't read back-to-back

#define SCAN_OUT_SIZE 4224      // largest scanner command: gamma table, 16 + 2056 bytes
#define SCAN_IN_SIZE USB_SCAN_MAX_READ

#define USB_CLASS_PRINTER 0x07
#define USB_CLASS_VENDOR 0xff
#define PRN_REQ_GET_DEVICE_ID 0
#define PRN_REQ_GET_PORT_STATUS 1
#define PRN_REQ_SOFT_RESET 2

typedef struct {
    usb_host_client_handle_t client;
    TaskHandle_t client_task;
    usb_printer_conn_cb_t conn_cb;
    SemaphoreHandle_t lock;             // protects resp buffer and info

    // device (owned by the client task)
    uint8_t pending_addr;
    volatile bool gone;
    volatile bool connected;
    usb_device_handle_t dev;
    uint8_t intf, alt, ep_out, ep_in;
    bool claimed;
    usb_printer_info_t info;

    // transfers
    usb_transfer_t *out[OUT_XFER_COUNT];
    usb_transfer_t *in;
    usb_transfer_t *ctrl;
    QueueHandle_t out_free;             // OUT transfers not in flight
    usb_transfer_t *fill;               // OUT transfer being filled by the job
    atomic_int out_inflight;            // OUT transfers submitted, not completed
    volatile bool in_inflight;
    bool in_stopped;                    // IN endpoint failed, stop polling
    TickType_t in_last;                 // when the last IN read completed
    volatile bool ctrl_done;
    SemaphoreHandle_t ctrl_sem;
    SemaphoreHandle_t ctrl_mutex;

    // scanner interface
    uint8_t scan_intf, scan_ep_out, scan_ep_in;
    uint16_t scan_mps;                  // bulk IN max packet size: reads are multiples of it
    bool scan_claimed;
    volatile bool scan_active;
    bool scan_stuck;                    // a transfer never came back: no more scanning
    usb_transfer_t *scan_out, *scan_in;
    SemaphoreHandle_t scan_out_sem, scan_in_sem;

    // job state
    volatile bool job_active;           // a print job or a scan holds the device
    volatile bool cancel;
    volatile usb_transfer_status_t out_status;  // first failed OUT status in the job

    char resp[RESP_BUF_SIZE];           // non-status replies, one line each
    size_t resp_len;
    char msg[MSG_BUF_SIZE];             // XML reply being reassembled (client task)
    size_t msg_len;
    char last_status[320];              // last logged status summary
    usb_printer_status_t status;        // parsed GetStatusResponse (under lock)
} printer_t;

static printer_t s_p;

// ---------------------------------------------------------------------------
// transfer callbacks (run in the client task)

static void out_cb(usb_transfer_t *xfer)
{
    if (xfer->status != USB_TRANSFER_STATUS_COMPLETED && s_p.out_status == USB_TRANSFER_STATUS_COMPLETED) {
        s_p.out_status = xfer->status;
        ESP_LOGW(TAG, "bulk OUT failed, status %d", xfer->status);
    }
    atomic_fetch_sub(&s_p.out_inflight, 1);
    xQueueSend(s_p.out_free, &xfer, 0);
}

static void in_submit(void)
{
    if (s_p.gone || !s_p.ep_in || s_p.in_inflight || s_p.in_stopped)
        return;
    s_p.in->num_bytes = IN_XFER_SIZE;
    s_p.in_inflight = true;
    if (usb_host_transfer_submit(s_p.in) != ESP_OK)
        s_p.in_inflight = false;
}

// copies the text of <tag>...</tag> (first occurrence) into out
static void xml_value(const char *msg, const char *tag, char *out, size_t n)
{
    out[0] = 0;
    const char *p = strstr(msg, tag);
    if (!p)
        return;
    p += strlen(tag);
    size_t i = 0;
    while (p[i] && p[i] != '<' && p[i] != ']' && i < n - 1) {
        out[i] = p[i];
        i++;
    }
    out[i] = 0;
}

static void resp_append(const char *line)
{
    size_t n = strlen(line);
    xSemaphoreTake(s_p.lock, portMAX_DELAY);
    if (s_p.resp_len + n + 1 >= RESP_BUF_SIZE)     // full: drop the history
        s_p.resp_len = 0;
    memcpy(s_p.resp + s_p.resp_len, line, n);
    s_p.resp_len += n;
    s_p.resp[s_p.resp_len++] = '\n';
    s_p.resp[s_p.resp_len] = 0;
    xSemaphoreGive(s_p.lock);
}

// One complete IVEC reply. Status replies are only logged when they change;
// everything else (StartJobResponse, ...) is logged and kept for
// usb_printer_wait_response().
static void handle_reply(const char *msg)
{
    char op[48], resp[24], detail[64];
    xml_value(msg, "<ivec:operation>", op, sizeof(op));
    xml_value(msg, "<ivec:response>", resp, sizeof(resp));
    xml_value(msg, "<ivec:response_detail>", detail, sizeof(detail));
    if (strcmp(op, "GetStatusResponse") == 0) {
        char status[24], sdetail[48], data[240], summary[sizeof(s_p.last_status)];
        xml_value(msg, "<ivec:status>", status, sizeof(status));
        xml_value(msg, "<ivec:status_detail>", sdetail, sizeof(sdetail));
        xml_value(msg, "<![CDATA[", data, sizeof(data));
        snprintf(summary, sizeof(summary), "%s (%s) %s", status, sdetail[0] ? sdetail : "-", data);
        xSemaphoreTake(s_p.lock, portMAX_DELAY);
        s_p.status.valid = true;
        snprintf(s_p.status.state, sizeof(s_p.status.state), "%s", status);
        snprintf(s_p.status.detail, sizeof(s_p.status.detail), "%s", sdetail);
        snprintf(s_p.status.data, sizeof(s_p.status.data), "%s", data);
        xSemaphoreGive(s_p.lock);
        if (strcmp(summary, s_p.last_status) != 0) {
            strcpy(s_p.last_status, summary);
            ESP_LOGI(TAG, "printer status: %s", summary);
        }
        return;
    }
    char line[160];
    snprintf(line, sizeof(line), "%s %s %s", op[0] ? op : "?", resp, detail);
    ESP_LOGI(TAG, "printer reply: %s", line);
    resp_append(line);
}

static void in_cb(usb_transfer_t *xfer)
{
    s_p.in_inflight = false;
    s_p.in_last = xTaskGetTickCount();
    if (xfer->status == USB_TRANSFER_STATUS_COMPLETED && xfer->actual_num_bytes > 0) {
        size_t n = xfer->actual_num_bytes;
        if (s_p.msg_len + n >= MSG_BUF_SIZE) {
            ESP_LOGW(TAG, "back-channel message too long, dropped");
            s_p.msg_len = 0;
        }
        memcpy(s_p.msg + s_p.msg_len, xfer->data_buffer, n);
        s_p.msg_len += n;
        s_p.msg[s_p.msg_len] = 0;
        char *end;
        while ((end = strstr(s_p.msg, "</cmd>")) != NULL) {
            end += strlen("</cmd>");
            char saved = *end;
            *end = 0;
            handle_reply(s_p.msg);
            *end = saved;
            size_t used = end - s_p.msg;
            memmove(s_p.msg, end, s_p.msg_len - used + 1);
            s_p.msg_len -= used;
        }
        // skip whitespace between replies
        while (s_p.msg_len && (s_p.msg[0] == '\r' || s_p.msg[0] == '\n' || s_p.msg[0] == ' ')) {
            memmove(s_p.msg, s_p.msg + 1, s_p.msg_len--);
        }
        if (s_p.msg_len)
            in_submit();        // rest of a reply is still pending
    } else if (xfer->status != USB_TRANSFER_STATUS_COMPLETED && xfer->status != USB_TRANSFER_STATUS_CANCELED &&
               xfer->status != USB_TRANSFER_STATUS_NO_DEVICE) {
        ESP_LOGW(TAG, "bulk IN failed (status %d), back channel disabled", xfer->status);
        s_p.in_stopped = true;
    }
}

static void ctrl_cb(usb_transfer_t *xfer)
{
    s_p.ctrl_done = true;
    xSemaphoreGive(s_p.ctrl_sem);
}

// Synchronous control transfer. Callable from any task: from the client task
// it pumps client events itself, since that is where the callback runs.
static esp_err_t ctrl_request(uint8_t bmRequestType, uint8_t bRequest, uint16_t wValue, uint16_t wIndex,
                              uint16_t wLength, uint8_t *data, size_t *actual)
{
    if (!s_p.dev || s_p.gone)
        return ESP_ERR_INVALID_STATE;
    if (wLength > CTRL_XFER_SIZE - sizeof(usb_setup_packet_t))
        return ESP_ERR_INVALID_SIZE;
    xSemaphoreTake(s_p.ctrl_mutex, portMAX_DELAY);
    usb_setup_packet_t *setup = (usb_setup_packet_t *)s_p.ctrl->data_buffer;
    setup->bmRequestType = bmRequestType;
    setup->bRequest = bRequest;
    setup->wValue = wValue;
    setup->wIndex = wIndex;
    setup->wLength = wLength;
    if (!(bmRequestType & USB_BM_REQUEST_TYPE_DIR_IN) && wLength && data)
        memcpy(s_p.ctrl->data_buffer + sizeof(usb_setup_packet_t), data, wLength);
    s_p.ctrl->num_bytes = sizeof(usb_setup_packet_t) + wLength;
    s_p.ctrl->device_handle = s_p.dev;
    s_p.ctrl->bEndpointAddress = 0;
    s_p.ctrl_done = false;
    xSemaphoreTake(s_p.ctrl_sem, 0);

    esp_err_t err = usb_host_transfer_submit_control(s_p.client, s_p.ctrl);
    if (err == ESP_OK) {
        bool in_client = xTaskGetCurrentTaskHandle() == s_p.client_task;
        TickType_t start = xTaskGetTickCount();
        while (!s_p.ctrl_done && xTaskGetTickCount() - start < pdMS_TO_TICKS(2000)) {
            if (in_client)
                usb_host_client_handle_events(s_p.client, pdMS_TO_TICKS(10));
            else
                xSemaphoreTake(s_p.ctrl_sem, pdMS_TO_TICKS(100));
        }
        if (!s_p.ctrl_done)
            err = ESP_ERR_TIMEOUT;
        else if (s_p.ctrl->status != USB_TRANSFER_STATUS_COMPLETED)
            err = ESP_FAIL;
    }
    if (err == ESP_OK && (bmRequestType & USB_BM_REQUEST_TYPE_DIR_IN) && data) {
        size_t n = s_p.ctrl->actual_num_bytes - sizeof(usb_setup_packet_t);
        if (n > wLength)
            n = wLength;
        memcpy(data, s_p.ctrl->data_buffer + sizeof(usb_setup_packet_t), n);
        if (actual)
            *actual = n;
    }
    xSemaphoreGive(s_p.ctrl_mutex);
    return err;
}

// ---------------------------------------------------------------------------
// attach / detach (client task)

static void str_desc_ascii(const usb_str_desc_t *d, char *out, size_t out_len)
{
    size_t j = 0;
    if (d && d->bLength >= 2) {
        for (size_t i = 0; i < (size_t)(d->bLength - 2) / 2 && j < out_len - 1; i++) {
            uint16_t ch = d->wData[i];
            out[j++] = (ch >= 0x20 && ch < 0x7f) ? (char)ch : '?';
        }
    }
    out[j] = 0;
}

static void free_scan_transfers(void)
{
    if (s_p.scan_out)
        usb_host_transfer_free(s_p.scan_out);
    if (s_p.scan_in)
        usb_host_transfer_free(s_p.scan_in);
    s_p.scan_out = s_p.scan_in = NULL;
}

static void release_device(void)
{
    if (s_p.claimed)
        usb_host_interface_release(s_p.client, s_p.dev, s_p.intf);
    s_p.claimed = false;
    if (!s_p.scan_stuck)
        free_scan_transfers();
    if (s_p.scan_claimed)
        usb_host_interface_release(s_p.client, s_p.dev, s_p.scan_intf);
    s_p.scan_claimed = false;
    s_p.scan_ep_in = s_p.scan_ep_out = 0;
    for (int i = 0; i < OUT_XFER_COUNT; i++) {
        if (s_p.out[i])
            usb_host_transfer_free(s_p.out[i]);
        s_p.out[i] = NULL;
    }
    if (s_p.in)
        usb_host_transfer_free(s_p.in);
    if (s_p.ctrl)
        usb_host_transfer_free(s_p.ctrl);
    s_p.in = s_p.ctrl = NULL;
    xQueueReset(s_p.out_free);
    if (s_p.dev)
        usb_host_device_close(s_p.client, s_p.dev);
    s_p.dev = NULL;
    s_p.ep_in = s_p.ep_out = 0;
}

// Ranks printer-class protocols: 2 = bidirectional (preferred), 1 =
// unidirectional, 3 = IEEE 1284.4 (packetized, we only use it as a plain
// OUT pipe).
static int proto_rank(int proto)
{
    return proto == 2 ? 3 : proto == 1 ? 2 : proto == 3 ? 1 : 0;
}

// Debug override: take the interface with this number whatever its class
// says. Canon printers keep the BJL command channel on their vendor
// interface, so a printer-class pipe can accept bytes and never answer.
static bool find_forced_interface(const usb_config_desc_t *cfg, int want)
{
    int offset = 0;
    bool in_if = false, found = false;
    uint8_t cur_out = 0, cur_in = 0;
    uint16_t mps = 0;
    const usb_standard_desc_t *d = (const usb_standard_desc_t *)cfg;
    for (;;) {
        d = usb_parse_next_descriptor(d, cfg->wTotalLength, &offset);
        if (!d || d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_INTERFACE) {
            if (in_if && cur_out) {
                s_p.intf = want;
                s_p.alt = 0;
                s_p.ep_out = cur_out;
                s_p.ep_in = cur_in;
                return true;
            }
            if (!d)
                break;
            const usb_intf_desc_t *intf = (const usb_intf_desc_t *)d;
            in_if = intf->bInterfaceNumber == want && intf->bAlternateSetting == 0;
            cur_out = cur_in = 0;
            mps = 0;
        } else if (in_if && d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_ENDPOINT) {
            const usb_ep_desc_t *ep = (const usb_ep_desc_t *)d;
            if ((ep->bmAttributes & USB_BM_ATTRIBUTES_XFERTYPE_MASK) == USB_BM_ATTRIBUTES_XFER_BULK) {
                if (ep->bEndpointAddress & USB_B_ENDPOINT_ADDRESS_EP_DIR_MASK) {
                    cur_in = ep->bEndpointAddress;
                    mps = ep->wMaxPacketSize & 0x7ff;
                } else {
                    cur_out = ep->bEndpointAddress;
                }
            }
        }
    }
    (void)mps;
    return found;
}

// Finds the printer-class interface, preferring the bidirectional protocol.
static bool find_printer_interface(const usb_config_desc_t *cfg)
{
    int offset = 0, best_rank = -1;
    uint8_t cur_intf = 0, cur_alt = 0, cur_out = 0, cur_in = 0;
    bool in_printer = false;
    const usb_standard_desc_t *d = (const usb_standard_desc_t *)cfg;

    // commits the interface collected so far if it beats the current best
    #define COMMIT()                                                                        \
        do {                                                                                \
            if (in_printer && cur_out && proto_rank(cur_proto) > best_rank) {               \
                best_rank = proto_rank(cur_proto);                                          \
                s_p.intf = cur_intf;                                                        \
                s_p.alt = cur_alt;                                                          \
                s_p.ep_out = cur_out;                                                       \
                s_p.ep_in = cur_proto == 2 ? cur_in : 0;                                    \
            }                                                                               \
        } while (0)

    int cur_proto = 0;
    while ((d = usb_parse_next_descriptor(d, cfg->wTotalLength, &offset)) != NULL) {
        if (d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_INTERFACE) {
            COMMIT();
            const usb_intf_desc_t *intf = (const usb_intf_desc_t *)d;
            in_printer = intf->bInterfaceClass == USB_CLASS_PRINTER && intf->bInterfaceSubClass == 1;
            cur_intf = intf->bInterfaceNumber;
            cur_alt = intf->bAlternateSetting;
            cur_proto = intf->bInterfaceProtocol;
            cur_out = cur_in = 0;
        } else if (in_printer && d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_ENDPOINT) {
            const usb_ep_desc_t *ep = (const usb_ep_desc_t *)d;
            if ((ep->bmAttributes & USB_BM_ATTRIBUTES_XFERTYPE_MASK) == USB_BM_ATTRIBUTES_XFER_BULK) {
                if (ep->bEndpointAddress & USB_B_ENDPOINT_ADDRESS_EP_DIR_MASK)
                    cur_in = ep->bEndpointAddress;
                else
                    cur_out = ep->bEndpointAddress;
            }
        }
    }
    COMMIT();
    #undef COMMIT
    return best_rank >= 0;
}

// Logs every interface and endpoint (useful when a new device shows up).
static void log_interfaces(const usb_config_desc_t *cfg)
{
    int offset = 0;
    const usb_standard_desc_t *d = (const usb_standard_desc_t *)cfg;
    while ((d = usb_parse_next_descriptor(d, cfg->wTotalLength, &offset)) != NULL) {
        if (d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_INTERFACE) {
            const usb_intf_desc_t *i = (const usb_intf_desc_t *)d;
            ESP_LOGI(TAG, "  interface %d alt %d: class %02x/%02x/%02x", i->bInterfaceNumber, i->bAlternateSetting,
                     i->bInterfaceClass, i->bInterfaceSubClass, i->bInterfaceProtocol);
        } else if (d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_ENDPOINT) {
            const usb_ep_desc_t *ep = (const usb_ep_desc_t *)d;
            static const char *const types[] = {"control", "isoc", "bulk", "interrupt"};
            ESP_LOGI(TAG, "    endpoint 0x%02x %s, max packet %d", ep->bEndpointAddress,
                     types[ep->bmAttributes & USB_BM_ATTRIBUTES_XFERTYPE_MASK], ep->wMaxPacketSize & 0x7ff);
        }
    }
}

// The scanner function: a vendor-specific interface with a bulk IN and a
// bulk OUT endpoint (SANE's pixma backend uses interface 0 of the MG2500).
static bool find_scanner_interface(const usb_config_desc_t *cfg)
{
    int offset = 0;
    const usb_standard_desc_t *d = (const usb_standard_desc_t *)cfg;
    bool cand = false, found = false;
    uint8_t intf = 0, in = 0, out = 0;
    uint16_t mps = 0;
    for (;;) {
        d = usb_parse_next_descriptor(d, cfg->wTotalLength, &offset);
        if (!d || d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_INTERFACE) {
            if (cand && in && out && !found) {
                found = true;
                s_p.scan_intf = intf;
                s_p.scan_ep_in = in;
                s_p.scan_ep_out = out;
                s_p.scan_mps = mps;
            }
            if (!d)
                break;
            const usb_intf_desc_t *i = (const usb_intf_desc_t *)d;
            cand = i->bInterfaceClass == USB_CLASS_VENDOR && i->bAlternateSetting == 0 &&
                   i->bInterfaceNumber != s_p.intf;
            intf = i->bInterfaceNumber;
            in = out = 0;
        } else if (cand && d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_ENDPOINT) {
            const usb_ep_desc_t *ep = (const usb_ep_desc_t *)d;
            if ((ep->bmAttributes & USB_BM_ATTRIBUTES_XFERTYPE_MASK) != USB_BM_ATTRIBUTES_XFER_BULK)
                continue;
            if (ep->bEndpointAddress & USB_B_ENDPOINT_ADDRESS_EP_DIR_MASK) {
                in = ep->bEndpointAddress;
                mps = ep->wMaxPacketSize & 0x7ff;
            } else {
                out = ep->bEndpointAddress;
            }
        }
    }
    return found && s_p.scan_mps > 0;
}

// ---------------------------------------------------------------------------
// DEBUG: does this printer answer plain HTTP on its IPP-USB interface?
// An IPP-USB device (class 7/1/4) speaks HTTP over bulk endpoints, which is
// how macOS prints without any Canon driver. A 200 on GET / proves the relay
// route is open.

#ifdef PRINTER_IPPUSB_PROBE

static void abort_endpoint(uint8_t ep);

static SemaphoreHandle_t probe_sem;

static void probe_cb(usb_transfer_t *xfer)
{
    if (probe_sem)
        xSemaphoreGive(probe_sem);
}

// The probe runs inside the USB client task, so nothing else drains the
// client's event queue while we wait: pump it here or the callback never runs.
static bool probe_wait(uint32_t ms)
{
    TickType_t start = xTaskGetTickCount();
    while (xTaskGetTickCount() - start < pdMS_TO_TICKS(ms)) {
        if (xSemaphoreTake(probe_sem, pdMS_TO_TICKS(10)) == pdTRUE)
            return true;
        usb_host_client_handle_events(s_p.client, pdMS_TO_TICKS(10));
    }
    return false;
}

static void probe_ippusb(const usb_config_desc_t *cfg, int want)
{
    // find alt 1 of the IPP-USB interface and its bulk endpoints
    int offset = 0;
    bool in_if = false, have = false;
    uint8_t ep_out = 0, ep_in = 0;
    const usb_standard_desc_t *d = (const usb_standard_desc_t *)cfg;
    for (;;) {
        d = usb_parse_next_descriptor(d, cfg->wTotalLength, &offset);
        if (!d || d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_INTERFACE) {
            if (in_if && ep_out && ep_in) {
                have = true;
                break;
            }
            if (!d)
                break;
            const usb_intf_desc_t *i = (const usb_intf_desc_t *)d;
            in_if = i->bInterfaceNumber == want && i->bAlternateSetting == 1;
            ep_out = ep_in = 0;
        } else if (in_if && d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_ENDPOINT) {
            const usb_ep_desc_t *ep = (const usb_ep_desc_t *)d;
            if ((ep->bmAttributes & USB_BM_ATTRIBUTES_XFERTYPE_MASK) == USB_BM_ATTRIBUTES_XFER_BULK) {
                if (ep->bEndpointAddress & USB_B_ENDPOINT_ADDRESS_EP_DIR_MASK)
                    ep_in = ep->bEndpointAddress;
                else
                    ep_out = ep->bEndpointAddress;
            }
        }
    }
    if (!have) {
        ESP_LOGE(TAG, "probe: interface %d alt 1 with bulk endpoints not found", want);
        return;
    }
    ESP_LOGW(TAG, "probe: interface %d alt 1, OUT 0x%02x, IN 0x%02x", want, ep_out, ep_in);

    esp_err_t err = usb_host_interface_claim(s_p.client, s_p.dev, want, 1);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "probe: claim alt 1 failed: %s", esp_err_to_name(err));
        return;
    }
    ESP_LOGW(TAG, "probe: claim alt 1 ok");
    // ESP-IDF's interface_claim only parses descriptors: it never sends
    // SET_INTERFACE, so the device would stay on alt 0 and NAK every byte.
    uint8_t req = USB_BM_REQUEST_TYPE_DIR_OUT | USB_BM_REQUEST_TYPE_TYPE_STANDARD |
                  USB_BM_REQUEST_TYPE_RECIP_INTERFACE;
    esp_err_t se = ctrl_request(req, USB_B_REQUEST_SET_INTERFACE, 1, want, 0, NULL, NULL);
    if (se != ESP_OK) {
        ESP_LOGE(TAG, "probe: SET_INTERFACE alt 1 failed: %s", esp_err_to_name(se));
        return;
    }
    ESP_LOGW(TAG, "probe: alt 1 is live on the device");

    // printer-class SOFT_RESET (as CUPS/ipp-usb do for stuck devices) and a
    // data-toggle clear on both pipes before the first byte.
    uint8_t creq = USB_BM_REQUEST_TYPE_DIR_OUT | USB_BM_REQUEST_TYPE_TYPE_CLASS |
                   USB_BM_REQUEST_TYPE_RECIP_INTERFACE;
    esp_err_t sr = ctrl_request(creq, 0x01, 0, want, 0, NULL, NULL);
    ESP_LOGW(TAG, "probe: SOFT_RESET -> %s", esp_err_to_name(sr));
    usb_host_endpoint_clear(s_p.dev, ep_out);
    usb_host_endpoint_clear(s_p.dev, ep_in);

    probe_sem = xSemaphoreCreateBinary();
    usb_transfer_t *tx = NULL, *rx = NULL;
    if (!probe_sem || usb_host_transfer_alloc(1024, 0, &tx) != ESP_OK ||
        usb_host_transfer_alloc(1024, 0, &rx) != ESP_OK) {
        ESP_LOGE(TAG, "probe: no transfers");
        goto out;
    }
    tx->device_handle = rx->device_handle = s_p.dev;
    tx->bEndpointAddress = ep_out;
    rx->bEndpointAddress = ep_in;
    tx->callback = rx->callback = probe_cb;

    static const char head[] = "POST /ipp/print HTTP/1.1\r\nHost: localhost\r\n"
                               "Content-Type: application/ipp\r\nContent-Length: 9\r\n\r\n";
    // IPP 2.0 Get-Printer-Attributes, request id 1, end-of-attributes
    static const uint8_t ipp[] = {0x02, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x01, 0x03};
    size_t hl = sizeof(head) - 1;
    memcpy(tx->data_buffer, head, hl);
    memcpy(tx->data_buffer + hl, ipp, sizeof(ipp));
    tx->num_bytes = (int)(hl + sizeof(ipp));
    if (usb_host_transfer_submit(tx) != ESP_OK) {
        ESP_LOGE(TAG, "probe: GET failed to submit");
        goto out;
    }
    if (!probe_wait(8000)) {
        ESP_LOGE(TAG, "probe: POST not completed in 8 s (status %d, %d bytes so far)", tx->status,
                 tx->actual_num_bytes);
        goto out;
    }
    ESP_LOGW(TAG, "probe: GET completed, status %d, %d bytes", tx->status, tx->actual_num_bytes);

    rx->num_bytes = 512;
    if (usb_host_transfer_submit(rx) != ESP_OK) {
        ESP_LOGE(TAG, "probe: read failed to submit");
        goto out;
    }
    static char acc[2048];
    int total = 0;
    bool got_ipp_hdr = false;
    for (int r = 0; r < 10; r++) {
        if (r > 0) {
            rx->num_bytes = 512;
            if (usb_host_transfer_submit(rx) != ESP_OK)
                break;
        }
        if (!probe_wait(2000))
            break;
        int n = rx->actual_num_bytes;
        if (n <= 0)
            break;
        if (n > (int)sizeof(acc) - 1 - total)
            n = (int)sizeof(acc) - 1 - total;
        memcpy(acc + total, rx->data_buffer, n);
        total += n;
        acc[total] = 0;
        if (strstr(acc, "application/ipp"))
            got_ipp_hdr = true;
        if (total >= (int)sizeof(acc) - 1)
            break;
    }
    ESP_LOGW(TAG, "probe: reply %d bytes, application/ipp in headers: %d", total, got_ipp_hdr);
    if (total > 0) {
        char line[130];
        int k = total < 120 ? total : 120;
        for (int i = 0; i < k; i++) {
            char c = acc[i];
            line[i] = (c >= 32 && c < 127) ? c : '.';
        }
        line[k] = 0;
        ESP_LOGW(TAG, "probe: HTTP says \"%s\"", line);
        char *body = strstr(acc, "\r\n\r\n");
        if (body) {
            // chunked: "<size>\r\n<payload>"
            char *data = strstr(body + 4, "\r\n");
            if (data && total - (int)(data + 2 - acc) >= 8) {
                const uint8_t *b = (const uint8_t *)(data + 2);
                ESP_LOGW(TAG, "probe: IPP body %02x %02x (needs 02 00)...", b[0], b[1]);
                const char *fmt = strstr(acc, "document-format-supported");
                if (fmt)
                    ESP_LOGW(TAG, "probe: printer lists document-format-supported");
                const char *urf = strstr(acc, "image/urf");
                const char *pwg = strstr(acc, "image/pwg-raster");
                ESP_LOGW(TAG, "probe: urf advertised %d, pwg advertised %d", urf != NULL, pwg != NULL);
            }
        }
    }
out:
    // The probe is one-shot and runs before anything else uses this device:
    // leave the transfers and the claim alone rather than dequeuing them,
    // which asserts in the host driver.
    ESP_LOGW(TAG, "probe: done");
}

#endif  // PRINTER_IPPUSB_PROBE

// ---------------------------------------------------------------------------
// IPP-USB relay: a raw TCP bridge on port RELAY_PORT that carries an IPP
// client's HTTP straight into the printer's IPP-USB portals and back.
//
// Why this exists: a Canon G2020 answers Canon's BJL print service with
// NotSupportService, and its only working print path is IPP over USB (class
// 07/01/04). Relaying lets the client's own driver and raster do the work, so
// no printer language has to be implemented here.

#ifdef PRINTER_RELAY

#define RELAY_SLOTS 1
#define RELAY_OUT_SIZE 4096
#define RELAY_IN_SIZE 2048
#define RELAY_HEAD_MAX 98304          /* whole document request, so the portal gets it in one burst */
#define RELAY_PREFIX_MAX 1024

typedef struct {
    bool used;
    uint8_t intf, ep_out, ep_in;
    usb_transfer_t *out, *in;
    QueueHandle_t out_free;     // OUT transfers not in flight
    SemaphoreHandle_t in_sem;
    volatile bool dead;
} relay_slot_t;

static relay_slot_t s_relay[RELAY_SLOTS];
static SemaphoreHandle_t s_relay_guard;
static SemaphoreHandle_t s_slot_free;

static void relay_out_cb(usb_transfer_t *x)
{
    relay_slot_t *sl = x->context;
    ESP_LOGI(TAG, "relay: out done %d %d", x->num_bytes, x->status);
    if (x->status != USB_TRANSFER_STATUS_COMPLETED)
        sl->dead = true;
    xQueueSend(sl->out_free, &x, 0);
}

static void relay_in_cb(usb_transfer_t *x)
{
    xSemaphoreGive((SemaphoreHandle_t)x->context);
}

// Claim every IPP-USB portal, activate alt 1 and arm one IN transfer each.
// The portals stay claimed for the life of the device.
static void relay_setup(const usb_config_desc_t *cfg)
{
    int offset = 0, n = 0;
    const usb_standard_desc_t *d = (const usb_standard_desc_t *)cfg;
    uint8_t intf = 0, alt = 0, proto = 0, cls = 0, sub = 0, ep_out = 0, ep_in = 0;
    bool in_it = false;

    for (;;) {
        d = usb_parse_next_descriptor(d, cfg->wTotalLength, &offset);
        if (!d || d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_INTERFACE) {
            if (in_it && ep_out && ep_in && n < RELAY_SLOTS) {
                relay_slot_t *sl = &s_relay[n];
                esp_err_t err = usb_host_interface_claim(s_p.client, s_p.dev, intf, 1);
                if (err == ESP_OK) {
                    uint8_t req = USB_BM_REQUEST_TYPE_DIR_OUT | USB_BM_REQUEST_TYPE_TYPE_STANDARD |
                                  USB_BM_REQUEST_TYPE_RECIP_INTERFACE;
                    if (ctrl_request(req, USB_B_REQUEST_SET_INTERFACE, 1, intf, 0, NULL, NULL) != ESP_OK) {
                        ESP_LOGW(TAG, "relay: SET_INTERFACE %d failed", intf);
                    }
                    // Without SOFT_RESET and a data-toggle clear the portal NAKs
                    // every byte of the first request (same as the probe path).
                    uint8_t creq = USB_BM_REQUEST_TYPE_DIR_OUT | USB_BM_REQUEST_TYPE_TYPE_CLASS |
                                   USB_BM_REQUEST_TYPE_RECIP_INTERFACE;
                    esp_err_t sr = ctrl_request(creq, 0x01, 0, intf, 0, NULL, NULL);
                    ESP_LOGW(TAG, "relay: SOFT_RESET %d -> %s", intf, esp_err_to_name(sr));
                    usb_host_endpoint_clear(s_p.dev, ep_out);
                    usb_host_endpoint_clear(s_p.dev, ep_in);
                    sl->intf = intf;
                    sl->ep_out = ep_out;
                    sl->ep_in = ep_in;
                    sl->out_free = xQueueCreate(1, sizeof(usb_transfer_t *));
                    sl->in_sem = xSemaphoreCreateBinary();
                    if (usb_host_transfer_alloc(RELAY_OUT_SIZE, 0, &sl->out) == ESP_OK &&
                        usb_host_transfer_alloc(RELAY_IN_SIZE, 0, &sl->in) == ESP_OK) {
                        sl->dead = false;
                        sl->out->device_handle = sl->in->device_handle = s_p.dev;
                        sl->out->bEndpointAddress = ep_out;
                        sl->in->bEndpointAddress = ep_in;
                        sl->out->context = sl;
                        sl->in->context = sl->in_sem;
                        sl->out->callback = relay_out_cb;
                        sl->in->callback = relay_in_cb;
                        xQueueSend(sl->out_free, &sl->out, 0);
                        sl->in->num_bytes = RELAY_IN_SIZE;
                        usb_host_transfer_submit(sl->in);
                        ESP_LOGI(TAG, "relay: portal %d ready (OUT 0x%02x IN 0x%02x)", intf, ep_out, ep_in);
                        n++;
                    } else {
                        ESP_LOGE(TAG, "relay: no transfers for portal %d", intf);
                    }
                } else {
                    ESP_LOGW(TAG, "relay: claim %d failed: %s", intf, esp_err_to_name(err));
                }
            }
            if (!d)
                break;
            const usb_intf_desc_t *i = (const usb_intf_desc_t *)d;
            intf = i->bInterfaceNumber;
            alt = i->bAlternateSetting;
            cls = i->bInterfaceClass;
            sub = i->bInterfaceSubClass;
            proto = i->bInterfaceProtocol;
            ep_out = ep_in = 0;
            in_it = cls == USB_CLASS_PRINTER && sub == 1 && proto == 4 && alt == 1;
        } else if (in_it && d->bDescriptorType == USB_B_DESCRIPTOR_TYPE_ENDPOINT) {
            const usb_ep_desc_t *ep = (const usb_ep_desc_t *)d;
            if ((ep->bmAttributes & USB_BM_ATTRIBUTES_XFERTYPE_MASK) == USB_BM_ATTRIBUTES_XFER_BULK) {
                if (ep->bEndpointAddress & USB_B_ENDPOINT_ADDRESS_EP_DIR_MASK)
                    ep_in = ep->bEndpointAddress;
                else
                    ep_out = ep->bEndpointAddress;
            }
        }
    }
    if (n == 0)
        ESP_LOGE(TAG, "relay: no IPP-USB portal found");
}

typedef struct {
    int fd;
    relay_slot_t *slot;
} relay_conn_t;

// Index just past the request head, or -1 if it has not arrived yet.
static int find_head_end(const char *head, int len, int from)
{
    for (int i = from < 3 ? 0 : from - 3; i + 4 <= len; i++)
        if (head[i] == '\r' && head[i + 1] == '\n' && head[i + 2] == '\r' && head[i + 3] == '\n')
            return i + 4;
    return -1;
}

static bool head_is_chunked(const char *head, size_t head_len)
{
    for (size_t i = 0; i + 18 < head_len; i++)
        if (strncasecmp(head + i, "transfer-encoding:", 18) == 0)
            return strcasestr(head + i, "chunked") != NULL;
    return false;
}

// Content-Length of a request head, or -1 when the request has no body.
static long head_content_length(const char *head, size_t head_len)
{
    for (size_t i = 0; i + 15 < head_len; i++)
        if (strncasecmp(head + i, "content-length:", 15) == 0)
            return strtol(head + i + 15, NULL, 10);
    return -1;
}

// Keep Content-Length honest after the body was resized.
static void adjust_length(char *head, size_t *head_len, int *buf_len, int delta)
{
    char *p = head, *limit = head + *head_len;
    while (p + 15 < limit) {
        if (strncasecmp(p, "content-length:", 15) == 0) {
            char *d = p + 15;
            while (*d == ' ')
                d++;
            char *after = d;
            long n = strtol(d, &after, 10);
            char num[12];
            int len = 0;
            long want = n + delta;
            do {
                num[len++] = (char)('0' + want % 10);
                want /= 10;
            } while (want > 0 && len < 11);
            for (int a = 0, b2 = len - 1; a < b2; a++, b2--) {
                char t = num[a];
                num[a] = num[b2];
                num[b2] = t;
            }
            memmove(d + len, after, *buf_len - (size_t)(after - head));
            memcpy(d, num, (size_t)len);
            int shift = len - (int)(after - d);
            *head_len += (size_t)shift;
            *buf_len += shift;
            return;
        }
        p++;
    }
}

// The portal also insists the IPP printer-uri names "localhost", so rewrite it
// in the request body. Returns the (possibly changed) body length.
static int rewrite_uri(uint8_t *b, int len)
{
    static const char local[] = "localhost";
    if (len < 8 || (b[0] != 2 && b[0] != 1))
        return len;
    int i = 8;
    while (i + 3 <= len) {
        uint8_t tag = b[i++];
        if (tag == 0x03)
            break;
        if (tag < 0x10)
            continue;
        int nl = (b[i] << 8) | b[i + 1];
        i += 2;
        if (i + nl > len)
            break;
        char *name = (char *)b + i;
        i += nl;
        int vl = (b[i] << 8) | b[i + 1];
        i += 2;
        if (i + vl > len)
            break;
        if ((nl == 11 && strncmp(name, "printer-uri", 11) == 0) ||
            (nl == 7 && strncmp(name, "job-uri", 7) == 0)) {
            uint8_t *v = b + i;
            int sp = -1, slash = -1;
            for (int k = 0; k + 3 <= vl; k++)
                if (v[k] == ':' && v[k + 1] == '/' && v[k + 2] == '/') {
                    sp = k + 3;
                    break;
                }
            if (sp > 0)
                for (int k = sp; k < vl; k++)
                    if (v[k] == '/') {
                        slash = k;
                        break;
                    }
            if (slash < 0)
                goto skip_uri;
            int alen = slash - sp, lv = (int)sizeof(local) - 1;
            memmove(v + sp + lv, v + slash, (size_t)(vl - slash));
            memcpy(v + sp, local, (size_t)lv);
            int nvl = vl - alen + lv;
            /* The portal publishes its job URI as ipp://localhost/jobs with the
             * number in a separate job-id attribute, so a "<job-uri>/<n>" that
             * CUPS builds from them answers client-error-not-found. Drop it. */
            if (nl == 7) {
                int cut = nvl;
                while (cut > 0 && v[cut - 1] >= '0' && v[cut - 1] <= '9')
                    cut--;
                if (cut > 0 && cut < nvl && v[cut - 1] == '/') {
                    int drop = nvl - (cut - 1);
                    nvl -= drop;
                    len -= drop;
                    ESP_LOGI(TAG, "relay: trim job-uri, drop %d", drop);
                }
            }
            b[i - 2] = (uint8_t)(nvl >> 8);
            b[i - 1] = (uint8_t)(nvl & 0xff);
            len += nvl - vl;
            i += nvl;
            continue;
        }
    skip_uri:;
        i += vl;
    }
    return len;
}

// Hand `len` bytes to the printer, waiting for a free OUT transfer.
static int send_out(QueueHandle_t out_free, const uint8_t *data, int len)
{
    bool first = true;
    while (len > 0) {
        int n0 = len > RELAY_OUT_SIZE ? RELAY_OUT_SIZE : len;   // one transfer per OUT buffer
        usb_transfer_t *x = NULL;
        if (xQueueReceive(out_free, &x, pdMS_TO_TICKS(50000)) != pdTRUE) {
            ESP_LOGW(TAG, "relay: printer not taking data after 50 s");
            return -1;
        }
        memcpy(x->data_buffer, data, (size_t)n0);
        x->num_bytes = n0;
        int r = usb_host_transfer_submit(x) == ESP_OK ? 0 : -1;
        {
            char seg[640];
            int w = 0;
            for (int q = 0; q < n0 && w < 600; q++)
                w += snprintf(seg + w, sizeof seg - (size_t)w, "%02x", data[q]);
            ESP_LOGI(TAG, "relay: out %d rc %d %s", n0, r, seg);
        }
        if (r != 0)
            return r;
        data += n0;
        len -= n0;
        first = false;
    }
    (void)first;
    return 0;
}

// The IPP-USB portal only accepts "Host: localhost"; rewrite whatever the
// client sent, or add the header if it is missing. Everything after the head
// (a body prefix, if the client pipelined it) moves with the head.
static void rewrite_host(char *head, size_t *head_len, int *buf_len, size_t cap)
{
    static const char local[] = "Host: localhost";
    char *p = head;
    size_t n = *head_len;
    while ((size_t)(p - head) < n) {
        char *eol = NULL;
        for (size_t q = (size_t)(p - head); q + 1 < n; q++)
            if (head[q] == '\r' && head[q + 1] == '\n') {
                eol = head + q;
                break;
            }
        if (!eol)
            return;
        if (eol - p > 5 && strncasecmp(p, "host:", 5) == 0) {
            size_t old_len = (size_t)(eol - p), new_len = sizeof(local) - 1;
            if (new_len > old_len && n + (new_len - old_len) >= cap)
                return;
            memmove(p + new_len, eol, (size_t)(*buf_len - (int)(eol - head)));
            memcpy(p, local, new_len);
            int shift = (int)new_len - (int)old_len;
            *head_len += (size_t)shift;
            *buf_len += shift;
            return;
        }
        p = eol + 2;
    }
}

// Rewrites shrink the body, so the length is set once from the real value
// rather than patched field by field (a rewrite can insert or drop bytes).
static int set_length(char *head, size_t head_len, int *buf_len, size_t cap, long value)
{
    for (char *p = head; p + 15 < head + head_len; p++) {
        if (strncasecmp(p, "content-length:", 15) != 0)
            continue;
        char *d = p + 15, *after = d;
        strtol(d, &after, 10);
        /* keep the space that separates field and value, so replacing the
         * number never eats a byte of the terminator */
        char num[20];
        int nl = snprintf(num, sizeof num, " %ld", value);
        int shift = nl - (int)(after - d);
        if (shift > 0 && (size_t)*buf_len + (size_t)shift > cap)
            return 0;
        memmove(after + shift, after, (size_t)(*buf_len - (int)(after - head)));
        memcpy(d + shift, num, (size_t)nl);
        *buf_len += shift;
        return shift;
    }
    return 0;
}

// Adds the keep-alive hint the printer needs before it holds the connection
// open for the next request.
static void set_connection(char *head, size_t head_len, int *buf_len, size_t cap)
{
    static const char field[] = "Connection: keep-alive\r\n";
    if (head_len + 2 > sizeof(field) - 1) {     // last CRLF is already there
        size_t cut = head_len - 2;
        if ((size_t)*buf_len + sizeof(field) - 1 > cap)
            return;
        memmove(head + cut + sizeof(field) - 1, head + cut, (size_t)(*buf_len - (int)cut));
        memcpy(head + cut, field, sizeof(field) - 1);
        *buf_len += sizeof(field) - 1;
    }
}

// Drops the headers CUPS adds for a proxy hop and the printer rejects
// (it answers HTTP/1.1 505 otherwise), and turns the request line into HTTP/1.1.
static void strip_proxy_headers(char *head, size_t *head_len, int *buf_len)
{
    static const char *drop[] = {"date:", "user-agent:", "keep-alive:", "connection:", "pragma:", NULL};
    char *start = memchr(head, '\n', *head_len);
    if (!start)
        return;
    start++;
    char *p = start;
    while (p < head + *head_len) {
        char *eol = memmem(p, (size_t)(head + *head_len - p), "\r\n", 2);
        size_t line = eol ? (size_t)(eol - p) : (size_t)(head + *head_len - p);
        if (line == 0)                     /* the blank line ends the head */
            return;
        bool kill = false;
        for (int i = 0; !kill && drop[i]; i++)
            if (line > strlen(drop[i]) && strncasecmp(p, drop[i], strlen(drop[i])) == 0)
                kill = true;
        size_t gone = eol ? line + 2 : line;
        if (kill) {
            memmove(p, p + gone, (size_t)(head + *buf_len - (p + gone)));
            *buf_len -= (int)gone;
            *head_len -= gone;
            continue;
        }
        if (!eol)
            return;
        p = eol + 2;
    }
}

// request framing states for the IPP relay
enum { F_REQ, F_FLUSH, F_FULL };
enum { CS_SIZE, CS_DATA, CS_SKIP, CS_END, CS_TRAIL };

// Walk HTTP chunked framing. Returns the bytes that belong to the body and sets
// *done once the terminal chunk has been passed.
static int chunk_walk(const uint8_t *b, int len, int *cs, int *left, int *skip, bool *last,
                      char *sz, int *szn, bool *done)
{
    int i = 0;
    while (i < len) {
        uint8_t ch = b[i];
        if (*cs == CS_DATA) {
            int avail = len - i;
            int use = (int)*left < avail ? (int)*left : avail;
            i += use;
            *left -= use;
            if (*left == 0) {
                *skip = 2;                      // CRLF after the chunk
                *last = false;
                *cs = CS_SKIP;
            }
        } else if (*cs == CS_SKIP) {
            int avail = len - i;
            int use = *skip < avail ? *skip : avail;
            i += use;
            *skip -= use;
            if (*skip == 0)
                *cs = *last ? CS_END : CS_SIZE;
        } else if (*cs == CS_SIZE) {
            i++;
            if (ch == '\n') {
                sz[*szn] = 0;
                *left = strtol(sz, NULL, 16);
                *szn = 0;
                if (*left > 0) {
                    *cs = CS_DATA;
                } else {
                    *skip = 2;                  // CRLF closing the body
                    *last = true;
                    *cs = CS_SKIP;
                }
            } else if (ch != '\r' && *szn < 15) {
                sz[(*szn)++] = (char)ch;
            }
        } else {
            *done = true;
            break;
        }
    }
    return i;
}

// Drop an Expect: 100-continue line. The relay answers the "100 Continue"
// itself, because the head is held back until the body prefix can be rewritten.
static void strip_expect(char *head, size_t *head_len, int *buf_len)
{
    size_t n = *head_len;
    for (size_t i = 0; i + 7 < n; i++)
        if (strncasecmp(head + i, "expect:", 7) == 0) {
            size_t j = i;
            while (j + 1 < n && !(head[j] == '\r' && head[j + 1] == '\n'))
                j++;
            size_t len = j + 2 <= n ? j + 2 - i : n - i;
            memmove(head + i, head + i + len, (size_t)(*buf_len - (int)(i + len)));
            *head_len -= len;
            *buf_len -= (int)len;
            return;
        }
}

enum { S_HEAD, S_WAIT, S_BODY };

// One connection: copy both ways until either side ends. Requests are buffered
// just far enough that Host, printer-uri and job-uri can be rewritten for the
// portal before anything reaches the printer.
// CUPS creates jobs with Create-Job + Send-Document, which needs the job to
// stay on one portal. Printing the whole body in a single Print-Job request is
// simpler, so hide those two operations from the client by renaming them to
// Validate-Job (same width, so the response stays byte-identical in size).
// Status polls are answered here, not by the printer: it allows one job-store
// client per portal, so a poll queued behind the job stalls CUPS. While a job
// is open, polls are sent to the printer as soon as the head is known and the
// answer is held back for a rewrite; class/format come from the host header so
// CUPS' request sequence matches what it sends to a real printer.
static bool s_ka, in_job, s_job_open;
static bool s_portal_up;                        /* the portal stays dialled */
static bool s_printing;                         /* a document body is on the wire */
static bool job;
static uint8_t p_search[32768];
static int p_len;
static uint8_t s_poll[900];                     /* our own answer */
static int s_poll_len;
static int s_job_len;

// Locate the body of a buffered answer. Returns the body offset, and sets
// *blen, once the answer is complete; -1 while it is still arriving.
static int reply_body(const uint8_t *b, int n, int *blen)
{
    static const uint8_t sep[] = {0x0d, 0x0a, 0x0d, 0x0a};
    const uint8_t *h = memmem(b, (size_t)n, sep, sizeof sep);
    if (!h)
        return -1;
    int hdr = (int)(h - b) + 4;
    bool chunked = memmem(b, (size_t)hdr, "chunked", 7) != NULL;
    if (!chunked) {
        const uint8_t *c = memmem(b, (size_t)hdr, "Content-Length:", 15);
        if (!c)
            return -1;
        int cl = atoi((const char *)c + 15);
        if (n - hdr < cl)
            return -1;
        *blen = cl;
        return hdr;
    }
    int i = hdr, total = 0, start = -1;
    while (i < n) {
        while (i < n && (b[i] == '\r' || b[i] == '\n'))   /* stray empty lines */
            i++;
        if (i >= n)
            return -1;
        const uint8_t *e = memmem(b + i, (size_t)(n - i), "\r\n", 2);
        if (!e)
            return -1;
        char *endp = NULL;
        long sz = strtol((const char *)b + i, &endp, 16);
        i = (int)(e - b) + 2;
        if (sz == 0 || endp == (char *)b + i - 2) {
            if (total > 0) {                    /* terminal chunk */
                *blen = total;
                return start;
            }
            continue;                           /* empty line, keep reading */
        }
        if (n - i < sz + 2)
            return -1;
        if (start < 0)
            start = i;
        total += sz;
        i += sz + 2;
    }
    return -1;
}

static void send_reply(int fd, int off, int blen)
{
    uint8_t *body = p_search + off;
    static uint8_t out[34000];
    int n = 0;

    /* the printer's own answer decides the state we report */
    s_job_open = memmem(body, (size_t)blen, "idle", 4) == NULL;
    n = snprintf((char *)out, 120, "HTTP/1.1 200 OK\r\nDate: x\r\n"
                                   "Content-Type: application/ipp\r\nConnection: close\r\n"
                                   "Content-Length: %-6d\r\n\r\n", blen);
    memcpy(out + n, body, (size_t)blen);
    n += blen;

    uint8_t *q = memmem(out, (size_t)n, "printer-state", 13);
    if (q && q + 15 < out + n) {
        uint8_t *v = q + 13;
        int len = (v[0] << 8) | v[1];
        const char *w = s_job_open ? "processing" : "idle";
        int keep = (int)strlen(w);
        if (len >= keep) {
            memcpy((char *)v + 2, w, (size_t)keep);
            if (keep < len) {
                memmove(v + 2 + keep, v + 2 + len, (size_t)(out + n - (v + 2 + len)));
                v[0] = 0; v[1] = (uint8_t)keep;
                n -= len - keep;
            }
        }
    }
    q = memmem(out, (size_t)n, "printer-state-message", 21);
    if (q && q + 23 < out + n) {
        uint8_t *v = q + 21;
        int len = (v[0] << 8) | v[1];
        const char *w = s_job_open ? "Printing." : "idle.";
        int keep = (int)strlen(w);
        if (len >= keep) {
            memcpy((char *)v + 2, w, (size_t)keep);
            if (keep < len) {
                memmove(v + 2 + keep, v + 2 + len, (size_t)(out + n - (v + 2 + len)));
                v[0] = 0; v[1] = (uint8_t)keep;
                n -= len - keep;
            }
        }
    }
    q = memmem(out, (size_t)n, "printer-is-accepting-jobs", 25);
    if (q && q + 27 < out + n && q[27] == 'f')
        q[27] = 't';
    uint8_t *f = (uint8_t *)memmem(out, 120, "Content-Length: ", 16);
    if (f) {                                    /* fixed width, no NUL written */
        char num[8];
        int nl = snprintf(num, sizeof num, "%-6d", n);
        memcpy(f + 16, num, (size_t)nl);
    }
    ESP_LOGI(TAG, "relay: reply %d", n);
    send(fd, out, (size_t)n, 0);
}

static void build_poll(void)
{
    static const char *st[] = {"idle", "processing"};
    static const char *msg[] = {"idle.", "Printing."};

    for (int v = 0; v < 2; v++) {
        uint8_t *r = s_poll + (v ? 460 : 0);
        int n = snprintf((char *)r, 460,
                         "HTTP/1.1 200 OK\r\nDate: x\r\nContent-Type: application/ipp\r\n"
                         "Connection: close\r\nContent-Length:        \r\n\r\n");
        uint8_t *b = r + n;
        int m = 0;
        b[m++] = 2; b[m++] = 0; b[m++] = 0; b[m++] = 0;
        b[m++] = 0; b[m++] = 0; b[m++] = 0; b[m++] = 1; b[m++] = 1;
        b[m++] = 0x47; b[m++] = 0; b[m++] = 0x12;
        memcpy(b + m, "attributes-charset", 18); m += 18;
        b[m++] = 0; b[m++] = 5; memcpy(b + m, "utf-8", 5); m += 5;
        b[m++] = 0x48; b[m++] = 0; b[m++] = 0x1b;
        memcpy(b + m, "attributes-natural-language", 27); m += 27;
        b[m++] = 0; b[m++] = 5; memcpy(b + m, "en-us", 5); m += 5;
        b[m++] = 0x04; b[m++] = 0; b[m++] = 0x10;
        memcpy(b + m, "queued-job-count", 16); m += 16;
        b[m++] = 0; b[m++] = 4; b[m++] = 0; b[m++] = 0; b[m++] = 0; b[m++] = 0;
        b[m++] = 0x21; b[m++] = 0; b[m++] = 0x0e;
        memcpy(b + m, "copies-default", 14); m += 14;
        b[m++] = 0; b[m++] = 4; b[m++] = 0; b[m++] = 0; b[m++] = 0; b[m++] = 1;
        b[m++] = 0x04; b[m++] = 0; b[m++] = 0x0d;
        memcpy(b + m, "printer-state", 13); m += 13;
        b[m++] = 0; b[m++] = (uint8_t)strlen(st[v]);
        memcpy(b + m, st[v], strlen(st[v])); m += (int)strlen(st[v]);
        b[m++] = 0x23; b[m++] = 0; b[m++] = 0x14;
        memcpy(b + m, "printer-state-message", 21); m += 21;
        b[m++] = 0; b[m++] = (uint8_t)strlen(msg[v]);
        memcpy(b + m, msg[v], strlen(msg[v])); m += (int)strlen(msg[v]);
        b[m++] = 0x23; b[m++] = 0; b[m++] = 0x18;
        memcpy(b + m, "printer-is-accepting-jobs", 25); m += 25;
        b[m++] = 0; b[m++] = 4; memcpy(b + m, "true", 4); m += 4;
        b[m++] = 0x23; b[m++] = 0; b[m++] = 0x0f;
        memcpy(b + m, "document-format-default", 23); m += 23;
        b[m++] = 0; b[m++] = 10; memcpy(b + m, "image/urf", 9); m += 9;
        b[m++] = 3;
        int total = n + m;
        if (s_poll_len > 0) {                   /* pad both variants equally */
            int pad = (s_poll_len > 460 ? s_poll_len - 460 : 0) - total;
            if (pad > 0) {
                memset(b + m, ' ', (size_t)pad);
                m += pad;
                total += pad;
            }
        }
        char num[8];
        int nl = snprintf(num, sizeof num, "%-6d", total);
        uint8_t *f = (uint8_t *)memmem(r, (size_t)n, "Content-Length: ", 16);
        if (f)
            memcpy(f + 16, num, (size_t)nl);
        if (v)
            s_poll_len = total > 460 ? total - 460 : total;
        else
            s_job_len = total;
    }
    ESP_LOGI(TAG, "relay: poll built %d/%d", s_job_len, s_poll_len);
}

static bool head_is_status_poll(const uint8_t *h, size_t hl, size_t avail)
{
    /* The operation-id sits right behind the head terminator: look 8 bytes
     * past it, into the body prefix, for 02 00 00 0b (Get-Printer-Attributes
     * with version 2.0). */
    static const uint8_t sig[] = {0x0d, 0x0a, 0x02, 0x00, 0x00, 0x0b};
    size_t lim = hl + 8 < avail ? hl + 8 : avail;
    if (lim < sizeof sig)
        return false;
    return memmem(h, lim, sig, sizeof sig) != NULL;
}

static void parse_context(uint8_t *head)
{
    char *ua = strcasestr((char *)head, "\r\nUser-Agent:");
    char *op = (char *)head + 8;

    s_ka = false;
    {
        char *q = strcasestr((char *)head, "connection:");
        if (q && strncasecmp(q, "connection: keep-alive", 22) == 0)
            s_ka = true;
    }
    (void)op;
    job = ua != NULL;                       /* class 1: a job request */
    if (ua && !s_poll_len)
        build_poll();
}

static size_t end;
static long g_cl, g_orig, g_spill;
static int g_chunk_left, g_skip_left, g_cs, g_szn;
static bool g_pfin, g_last_chunk;

// The portal answers Get-Printer-Attributes with barely anything (no
// printer-state, no capabilities), which CUPS tolerates and iOS does not: an
// iPhone drops a printer from the list when the attributes are thin. The
// attributes below are static for this device, so they are answered here and
// the printer is not asked at all.
enum { IPP_OP_ATTRS = 0x01, IPP_JOB_ATTRS = 0x02, IPP_END = 0x03, IPP_PRINTER_ATTRS = 0x04 };

// Group tags are written once, when the group changes: ip_a/ip_i take the
// attribute *value* tag (0x47 charset, 0x44 keyword, ...), not the group.
static uint8_t g_cur;
static void ip_grp(uint8_t *o, int *p, uint8_t g)
{
    if (g_cur != g) { o[(*p)++] = g; g_cur = g; }
}

static void ip_a(uint8_t *o, int *p, uint8_t tag, const char *name, const char *val, int vl)
{
    int nl = (int)strlen(name);
    o[(*p)++] = tag;
    o[(*p)++] = (uint8_t)(nl >> 8); o[(*p)++] = (uint8_t)nl;
    memcpy(o + *p, name, (size_t)nl); *p += nl;
    o[(*p)++] = (uint8_t)(vl >> 8); o[(*p)++] = (uint8_t)vl;
    if (vl) { memcpy(o + *p, val, (size_t)vl); *p += vl; }
}

// collections (IPP 0x34 beg / 0x4A memberAttrName / 0x37 end): the Apple PPD
// generator reads media sizes from these and fails without them.
static void ip_cbeg(uint8_t *o, int *p, uint8_t g, const char *name)
{
    int l = (int)strlen(name);
    ip_grp(o, p, g);
    o[(*p)++] = 0x34;
    o[(*p)++] = 0; o[(*p)++] = (uint8_t)l;
    memcpy(o + *p, name, (size_t)l); *p += l;
    o[(*p)++] = 0; o[(*p)++] = 0;                   /* empty collection value */
}
static void ip_cmem(uint8_t *o, int *p, const char *n)      /* memberAttrName */
{
    int l = (int)strlen(n);
    o[(*p)++] = 0x4A; o[(*p)++] = 0; o[(*p)++] = 0;         /* empty name */
    o[(*p)++] = 0; o[(*p)++] = (uint8_t)l;                   /* value = member name */
    memcpy(o + *p, n, (size_t)l); *p += l;
}
static void ip_crng(uint8_t *o, int *p, int lo, int hi)     /* rangeOfInteger member */
{
    o[(*p)++] = 0x36; o[(*p)++] = 0; o[(*p)++] = 0;
    o[(*p)++] = 0; o[(*p)++] = 8;
    o[(*p)++] = (uint8_t)(lo >> 24); o[(*p)++] = (uint8_t)(lo >> 16);
    o[(*p)++] = (uint8_t)(lo >> 8);  o[(*p)++] = (uint8_t)lo;
    o[(*p)++] = (uint8_t)(hi >> 24); o[(*p)++] = (uint8_t)(hi >> 16);
    o[(*p)++] = (uint8_t)(hi >> 8);  o[(*p)++] = (uint8_t)hi;
}
static void ip_ckey(uint8_t *o, int *p, const char *v)      /* keyword member */
{
    int l = (int)strlen(v);
    o[(*p)++] = 0x44; o[(*p)++] = 0; o[(*p)++] = 0;
    o[(*p)++] = 0; o[(*p)++] = (uint8_t)l;
    memcpy(o + *p, v, (size_t)l); *p += l;
}
/* a 1setOf is repeated values, not one comma-joined string */
static void ip_list(uint8_t *o, int *p, uint8_t tag, const char *name, const char *csv)
{
    const char *q = csv;
    int first = 1;
    while (*q) {
        const char *e = strchr(q, ',');
        int l = e ? (int)(e - q) : (int)strlen(q);
        ip_a(o, p, tag, first ? name : "", q, l);   /* set: name only on 1st */
        first = 0;
        if (!e)
            break;
        q = e + 1;
    }
}

static void ip_cend(uint8_t *o, int *p)
{
    o[(*p)++] = 0x37; o[(*p)++] = 0; o[(*p)++] = 0; o[(*p)++] = 0; o[(*p)++] = 0;
}

// resolution = 2 x int32 + 1 byte units (3 = dpi)
static void ip_res(uint8_t *o, int *p, uint8_t g, const char *name, int dpi)
{
    ip_grp(o, p, g);
    char b[9] = {(char)(dpi >> 24), (char)(dpi >> 16), (char)(dpi >> 8), (char)dpi,
                 (char)(dpi >> 24), (char)(dpi >> 16), (char)(dpi >> 8), (char)dpi, 3};
    ip_a(o, p, 0x32, name, b, 9);
}

static void ip_i(uint8_t *o, int *p, uint8_t tag, const char *name, int v)
{
    char b[4] = {(char)(v >> 24), (char)(v >> 16), (char)(v >> 8), (char)v};
    ip_a(o, p, tag, name, b, 4);
}

static void ip_b(uint8_t *o, int *p, uint8_t tag, const char *name, bool v)
{
    char c = v ? 1 : 0;
    ip_a(o, p, tag, name, &c, 1);
}

// Build a complete Get-Printer-Attributes answer. Returns the body length.
static int air_attrs(uint32_t rid, uint8_t *o, int cap)
{
    (void)cap;
    g_cur = 0;
    o[0] = 1; o[1] = 1;                                  // IPP/1.1
    o[2] = 0; o[3] = 0;                                  // successful-ok
    o[4] = (uint8_t)(rid >> 24); o[5] = (uint8_t)(rid >> 16);
    o[6] = (uint8_t)(rid >> 8);  o[7] = (uint8_t)rid;
    int p = 8;

    ip_grp(o, &p, IPP_OP_ATTRS);
    ip_a(o, &p, 0x47, "attributes-charset", "utf-8", 5);
    ip_a(o, &p, 0x48, "attributes-natural-language", "en-us", 5);

    ip_grp(o, &p, IPP_PRINTER_ATTRS);
    /* Real, resolvable host: CUPS builds the PPD from this answer and fetches
     * the icons from whatever host is named here. */
    static char base[96], uri[128];
    snprintf(base, sizeof base, "http://%s.local", HOSTNAME);
    snprintf(uri, sizeof uri, "ipp://%s.local/ipp/print", HOSTNAME);
    ip_a(o, &p, 0x45, "printer-uri-supported", uri, (int)strlen(uri));
    ip_a(o, &p, 0x44, "uri-security-supported", "none", 4);
    ip_a(o, &p, 0x44, "uri-authentication-supported", "none", 4);
    ip_a(o, &p, 0x42, "printer-name", "Canon G2020", 11);
    ip_a(o, &p, 0x41, "printer-info", "Canon G2020", 11);
    ip_a(o, &p, 0x41, "printer-make-and-model", "Canon PIXMA G2020", 17);
    ip_a(o, &p, 0x41, "printer-location", "LAN", 3);
    ip_a(o, &p, 0x45, "printer-uuid", "urn:uuid:00000000-0000-1000-8000-04a918bd0000", 45);
    /* iOS picks a driver from printer-device-id and the PWG raster block; with
     * only document-format + urf-supported it lists the printer and then
     * refuses to submit a job. */
    static const char devid[] =
        "MFG:Canon;CMD:IVEC,URF;MDL:G2020 series;CLS:PRINTER;DES:Canon G2020 series;"
        "VER:1.040;URF:V1.4,CP1,PQ3-5,RS600,SRGB24,W8,OB9,OFU0,IS1;";
    ip_a(o, &p, 0x41, "printer-device-id", devid, (int)sizeof(devid) - 1);
    ip_a(o, &p, 0x41, "printer-firmware-name", "G2020 series", 12);
    ip_a(o, &p, 0x41, "printer-firmware-string-version", "1.040", 5);
    ip_a(o, &p, 0x30, "printer-alert", "code=other", 10);
    ip_a(o, &p, 0x41, "printer-alert-description", "Non-alert", 9);
    ip_b(o, &p, 0x22, "color-supported", true);
    ip_i(o, &p, 0x23, "printer-state", 3);
    ip_a(o, &p, 0x44, "printer-state-reasons", "none", 4);
    ip_b(o, &p, 0x22, "printer-is-accepting-jobs", true);
    /* CUPS flags a conformance failure and stops the queue when the printer
     * group carries no job-state. */
    ip_i(o, &p, 0x23, "job-state", s_printing ? 5 : 9);
    ip_a(o, &p, 0x44, "job-state-reasons", "none", 4);

    static const int ops[] = {2, 4, 5, 6, 8, 9, 10, 11};
    for (size_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++)
        ip_i(o, &p, 0x23, "operations-supported", ops[i]);

    ip_a(o, &p, 0x47, "charset-configured", "utf-8", 5);
    ip_a(o, &p, 0x47, "charset-supported", "utf-8", 5);
    ip_a(o, &p, 0x48, "natural-language-configured", "en-us", 5);
    ip_a(o, &p, 0x48, "generated-natural-language-supported", "en-us", 5);

    static const char pdl[] = "image/urf,image/pwg-raster,application/pdf";
    ip_a(o, &p, 0x49, "document-format-default", "image/urf", 9);
    ip_list(o, &p, 0x49, "document-format-supported", "image/urf,image/pwg-raster,application/pdf");
    ip_list(o, &p, 0x44, "urf-supported", "V1.4,CP1,W8,RS300,SRGB24");

    ip_a(o, &p, 0x44, "media-default", "iso_a4_210x297mm", 16);
    ip_a(o, &p, 0x44, "media-ready", "iso_a4_210x297mm", 16);
    static const char med[] = "iso_a4_210x297mm,na_letter_8.5x11in";
    ip_list(o, &p, 0x44, "media-supported", "iso_a4_210x297mm,na_letter_8.5x11in");
    ip_a(o, &p, 0x44, "media-type-supported", "stationery", 10);
    ip_a(o, &p, 0x44, "media-source-supported", "auto", 4);
    {   /* media sizes, as collections */
        static const struct { const char *name; int w, h; } sz[] = {
            {"iso_a4_210x297mm", 21000, 29700},
            {"na_letter_8.5x11in", 21590, 27940},
        };
        for (size_t i = 0; i < sizeof(sz) / sizeof(sz[0]); i++) {
            /* media-size-supported: elements are a collection with x/y only */
            {
                ip_cbeg(o, &p, IPP_PRINTER_ATTRS, i ? "" : "media-size-supported");
                ip_cmem(o, &p, "x-dimension");
                ip_crng(o, &p, sz[i].w, sz[i].w);
                ip_cmem(o, &p, "y-dimension");
                ip_crng(o, &p, sz[i].h, sz[i].h);
                ip_cend(o, &p);
            }
        }
        /* media-col-database entries */
        for (size_t i = 0; i < sizeof(sz) / sizeof(sz[0]); i++) {
            ip_cbeg(o, &p, IPP_PRINTER_ATTRS, i ? "" : "media-col-database");
            ip_cmem(o, &p, "media-size");               /* nested collection member */
            ip_cbeg(o, &p, IPP_PRINTER_ATTRS, "");
            ip_cmem(o, &p, "x-dimension");
            ip_crng(o, &p, sz[i].w, sz[i].w);
            ip_cmem(o, &p, "y-dimension");
            ip_crng(o, &p, sz[i].h, sz[i].h);
            ip_cend(o, &p);
            ip_cmem(o, &p, "media-type");
            ip_ckey(o, &p, "stationery");
            ip_cmem(o, &p, "media-source");
            ip_ckey(o, &p, "auto");
            ip_cend(o, &p);
        }
    }

    ip_res(o, &p, IPP_PRINTER_ATTRS, "printer-resolution-default", 300);
    ip_res(o, &p, IPP_PRINTER_ATTRS, "printer-resolution-supported", 300);
    ip_res(o, &p, IPP_PRINTER_ATTRS, "pwg-raster-document-resolution-supported", 300);
    ip_a(o, &p, 0x44, "pwg-raster-document-sheet-back", "normal", 6);
    ip_b(o, &p, 0x22, "pwg-raster-document-optimize", false);
    ip_list(o, &p, 0x44, "pwg-raster-document-type-supported", "srgb_8,sgry_8");
    ip_i(o, &p, 0x23, "print-quality-default", 4);
    ip_i(o, &p, 0x23, "print-quality-supported", 4);
    ip_a(o, &p, 0x44, "print-color-mode-default", "color", 5);
    ip_a(o, &p, 0x44, "print-color-mode-supported", "color", 5);
    ip_i(o, &p, 0x21, "copies-default", 1);
    ip_i(o, &p, 0x21, "copies-supported", 99);
    ip_a(o, &p, 0x44, "sides-default", "one-sided", 9);
    ip_a(o, &p, 0x44, "sides-supported", "one-sided", 9);
    ip_a(o, &p, 0x44, "job-sheets-default", "none", 4);
    ip_a(o, &p, 0x44, "job-sheets-supported", "none", 4);
    ip_a(o, &p, 0x44, "pdl-override-supported", "attempted", 9);
    ip_b(o, &p, 0x22, "multiple-document-jobs-supported", false);
    ip_a(o, &p, 0x44, "ipp-versions-supported", "1.1", 3);
    ip_a(o, &p, 0x44, "compression-supported", "none", 4);
    {   /* the web UI is on :8080, and only there */
        static char more[128];
        snprintf(more, sizeof more, "http://%s.local:8080/", HOSTNAME);
        ip_a(o, &p, 0x45, "printer-more-info", more, (int)strlen(more));
    }

    o[p++] = IPP_END;
    return p;
}

// Answer a Get-Printer-Attributes locally.
static int air_reply(int fd, uint8_t *body)
{
    uint32_t rid = ((uint32_t)body[4] << 24) | ((uint32_t)body[5] << 16) |
                   ((uint32_t)body[6] << 8) | (uint32_t)body[7];
    static uint8_t ans[4096];
    int n = air_attrs(rid, ans, (int)sizeof ans);
    char head[160];
    int hn = snprintf(head, sizeof head,
                      "HTTP/1.1 200 OK\r\nContent-Type: application/ipp\r\n"
                      "Content-Length: %d\r\nConnection: keep-alive\r\n\r\n", n);
    if (send(fd, head, (size_t)hn, 0) < 0)
        return -1;
    return send(fd, ans, (size_t)n, 0) < 0 ? -1 : 0;
}

// The printer never answers Validate-Job (it goes out and the answer never
// comes), which is what makes an iPhone say "printer is no longer available"
// right after "Printing". Job state is answered here for the same reason.
static int air_quick(int fd, uint8_t *body, bool with_job, int state)
{
    uint32_t rid = ((uint32_t)body[4] << 24) | ((uint32_t)body[5] << 16) |
                   ((uint32_t)body[6] << 8) | (uint32_t)body[7];
    uint8_t ans[512];
    int p = 0;
    g_cur = 0;
    ans[p++] = 1; ans[p++] = 1; ans[p++] = 0; ans[p++] = 0;
    ans[p++] = (uint8_t)(rid >> 24); ans[p++] = (uint8_t)(rid >> 16);
    ans[p++] = (uint8_t)(rid >> 8);  ans[p++] = (uint8_t)rid;
    ip_grp(ans, &p, IPP_OP_ATTRS);
    ip_a(ans, &p, 0x47, "attributes-charset", "utf-8", 5);
    ip_a(ans, &p, 0x48, "attributes-natural-language", "en-us", 5);
    if (with_job) {
        ip_grp(ans, &p, IPP_JOB_ATTRS);
        ip_i(ans, &p, 0x23, "job-state", state);            // 5 processing, 9 done
        ip_a(ans, &p, 0x44, "job-state-reasons", "none", 4);
    }
    ans[p++] = IPP_END;
    char head[160];
    int hn = snprintf(head, sizeof head,
                      "HTTP/1.1 200 OK\r\nContent-Type: application/ipp\r\n"
                      "Content-Length: %d\r\nConnection: keep-alive\r\n\r\n", p);
    if (send(fd, head, (size_t)hn, 0) < 0)
        return -1;
    return send(fd, ans, (size_t)p, 0) < 0 ? -1 : 0;
}

static void relay_reopen(void);
static void patch_ops(uint8_t *p, int n)
{
    for (int i = 0; i + 22 <= n; i++) {
        if (memcmp(p + i, "operations-supported", 20) != 0)
            continue;
        int len = (p[i + 20] << 8) | p[i + 21];
        int o = i + 22;
        int hits = 0;
        for (; len >= 4 && o + 4 <= n; len -= 4, o += 4)
            if (p[o] == 0 && p[o + 1] == 0 && p[o + 2] == 0 && (p[o + 3] == 5 || p[o + 3] == 6)) {
                p[o + 3] = 4;
                hits++;
            }
        if (hits)
            ESP_LOGI(TAG, "relay: ops rewritten %d", hits);
        break;
    }
}


// The portal answers 400 to a chunked request. Dechunk a fully buffered body
// in place and swap Transfer-Encoding for a Content-Length of the same line
// length (padded), so the request goes out the way the printer expects.
static int dechunk(char *buf, size_t hl, int *buf_len, size_t cap)
{
    char *te = NULL;
    for (size_t i = 0; i + 18 < hl; i++)
        if (strncasecmp(buf + i, "transfer-encoding:", 18) == 0) {
            te = buf + i;
            break;
        }
    if (!te)
        return -1;
    char *te_end = memmem(te, hl - (size_t)(te - buf), "\r\n", 2);
    if (!te_end)
        return -1;
    size_t te_len = (size_t)(te_end + 2 - te);      /* incl. CRLF */
    /* only touch the buffer when the whole body is here: a half-done dechunk
     * would leave the request unusable */
    if (!memmem(buf + hl, (size_t)(*buf_len - (int)hl), "\r\n0\r\n", 5))
        return -1;

    size_t src = hl, dst = hl;
    while ((int)src < *buf_len) {
        char *endp = NULL;
        long sz = strtol(buf + src, &endp, 16);
        if (endp == buf + src)
            return -1;
        char *e = memmem(endp, (size_t)(*buf_len - (int)(endp - buf)), "\r\n", 2);
        if (!e)
            return -1;
        src = (size_t)(e - buf) + 2;
        if (sz == 0)
            break;
        if ((int)src + (int)sz > *buf_len)
            return -1;
        memmove(buf + dst, buf + src, (size_t)sz);
        dst += (size_t)sz;
        src += (size_t)sz;
        if (src + 2 <= (size_t)*buf_len)
            src += 2;                               /* the chunk's CRLF */
    }
    int body = (int)(dst - hl);
    char hdr[40];
    int hn = snprintf(hdr, sizeof hdr, "Content-Length: %d", body);
    while ((size_t)hn < te_len - 2)
        hdr[hn++] = ' ';
    hdr[hn++] = '\r';
    hdr[hn++] = '\n';
    if ((size_t)hn > te_len) {
        if ((size_t)*buf_len + (hn - te_len) > cap)
            return -1;
        memmove(te + hn, te + te_len, (size_t)(*buf_len - (int)(te_len + (te - buf))));
    }
    memcpy(te, hdr, (size_t)hn);
    *buf_len += (int)(hn - te_len);
    return body;
}

static int frame_req(uint8_t *rx, size_t cap, int *hlen, int *frag, int *off_p, int n, int *sent, int fd,
                     QueueHandle_t out_free, int *ops_left, int *keepalive, bool *hole, bool *hold,
                     bool state_job)
{
    long cl, orig, spill = g_spill;
    size_t hl_head = end;
    int chunk_left, skip_left, cs, szn2;
    bool pfin, last_chunk;
    char szbuf[16];

    cl = g_cl;
    orig = g_orig;
    chunk_left = g_chunk_left;
    skip_left = g_skip_left;
    cs = g_cs;
    pfin = g_pfin;
    last_chunk = g_last_chunk;
    szn2 = g_szn;
    int off_i = *off_p;
    bool fail = false;

    *keepalive = 0;                             /* reset per call, set on a close */

    if (*frag == F_REQ) {
        cl = 0;
        int fresh = n;                          /* fresh bytes sit at rx + off_i */
        if ((size_t)fresh > cap - (size_t)*hlen)
            return -1;                          // never overrun the buffer
        if (*hole && *hlen > 0) {               // resume a head left mid-read
            /* The reorder buffer still owns the head tail: put it in front
             * before the fresh bytes are copied in behind it. */
            if (hl_head + (size_t)spill > cap)
                return -1;
            memcpy(rx + hl_head, rx + hl_head + spill, (size_t)spill);
            ESP_LOGI(TAG, "relay: resume head at %u", (unsigned)hl_head);
            *hole = false;
            memmove(rx + *hlen, rx + off_i, (size_t)fresh);
            *hlen += fresh;
            off_i = n;
            *sent = n;
        } else if (*hlen == 0) {                // nothing buffered yet
            /* The printer runs a fresh chunked stream on every connection, so
             * after the first request the next head is glued behind the
             * terminal CRLF. Skip exactly those two bytes, no more: the tail of
             * an unread chunk may itself end in CRLF. */
            if (fresh >= 2 && rx[off_i] == '\r' && rx[off_i + 1] == '\n') {
                off_i += 2;
                fresh -= 2;
            }
            if (fresh <= 0)
                goto out;
            if ((size_t)fresh > cap)
                return -1;
            memmove(rx, rx + off_i, (size_t)fresh);
            *hlen = fresh;
            off_i = n;
            *sent = n;
        } else {                                // append behind a parsed head
            memmove(rx + *hlen, rx + off_i, (size_t)fresh);
            *hlen += fresh;
            off_i = n;
            *sent = n;
        }
        int found = find_head_end((const char *)rx, *hlen, 0);
        if (found < 0)
            goto out;
        size_t hl = (size_t)found;
        /* The Content-Length field shifts when Host is rewritten, so the
         * remaining body length is known only here. Clamp to what is buffered
         * so a wrong guess can never desynchronise the stream. */
        long len = head_is_chunked((const char *)rx, hl) ? -1 : head_content_length((const char *)rx, hl);
        if (len < 0) {                          /* chunked: try to dechunk it */
            long d2 = dechunk((char *)rx, hl, hlen, cap);
            if (d2 >= 0) {
                len = d2;                       /* head length is unchanged */
                ESP_LOGI(TAG, "relay: dechunked %ld", d2);
            } else if (*hlen < (int)cap - 64) {
                goto out;                       /* wait until it can be dechunked */
            }
        }
        {
            char *eol = memchr(rx, '\r', hl);
            char *ver = eol ? memmem(rx, (size_t)(eol - (char *)rx), " HTTP/1.", 8) : NULL;
            if (ver && ver + 8 <= eol)
                ver[7] = (ver[7] >= '0' && ver[7] <= '9') ? '1' : ver[7];
        }
        parse_context(rx);
        if (strncasecmp((char *)rx, "POST", 4) == 0)
            strip_proxy_headers((char *)rx, &hl, hlen);
        rewrite_host((char *)rx, &hl, hlen, cap);
        end = hl;
        bool expect = false;
        for (size_t i = 0; i + 7 < end; i++)
            if (strncasecmp((const char *)rx + i, "expect:", 7) == 0)
                expect = true;
        if (expect) {
            strip_expect((char *)rx, &hl, hlen);
            end = hl;
            /* Only interim-answer when the body is still missing. If it already
             * arrived with the head, a late "100 Continue" lands after the real
             * answer and CUPS chokes on it. */
            if (len < 0 || (int)(*hlen - (int)end) < (int)len) {
                static const char cont[] = "HTTP/1.1 100 Continue\r\n\r\n";
                send(fd, cont, sizeof(cont) - 1, 0);
            }
        }
        {
            char seg[101];
            for (size_t q = 0; q < end; q += 100) {
                size_t n2 = end - q > 100 ? 100 : end - q;
                memcpy(seg, rx + q, n2); seg[n2] = 0;
                ESP_LOGI(TAG, "relay: > %s", seg);
            }
            if (*hlen > (int)end + 4)
                ESP_LOGI(TAG, "relay: op %02x%02x", rx[end + 2], rx[end + 3]);
            if (end < 900)
                ESP_LOG_BUFFER_HEX_LEVEL(TAG, rx, end, ESP_LOG_INFO);
        }
        g_orig = len;
        /* A status poll is a short request; a job is kilobytes. Classify by
         * the body length, which is known here, instead of hunting the
         * operation id inside a body that has not arrived yet. A poll is
         * forwarded untouched and only its answer is rewritten. */
        /* An IPP operation id sits right behind the version in the body: a
         * Get-Printer-Attributes is answered here, never forwarded. */
        {
            uint8_t *b = rx + end;
            int have_b = *hlen - (int)end;
            /* only once the whole body is here: the rest of it would
             * otherwise be parsed as a new request */
            if (len >= 0 && have_b >= (int)len && len >= 8 && b[0] > 0 && b[0] < 3 && b[2] == 0) {
                int op = b[3];
                bool bypass = memmem(rx, (size_t)end, "-Raw:", 6) != NULL;
                if (!bypass && (op == 9 || op == 11 || op == 4 || op == 8)) {
                    ESP_LOGI(TAG, "relay: op %d answered locally", op);
                    *keepalive = 1;
                    *frag = F_REQ;
                    p_len = 0;
                    *sent = n;
                    {   /* keep anything pipelined behind the body */
                        int rest = *hlen - (int)end - (int)len;
                        if (rest > 0)
                            memmove(rx, rx + end + len, (size_t)rest);
                        *hlen = rest > 0 ? rest : 0;
                    }
                    int r = (op == 9 || op == 11) ? air_reply(fd, b) : air_quick(fd, b, op == 8, s_printing ? 5 : 9);
                    if (r != 0)
                        ESP_LOGW(TAG, "relay: local answer failed");
                    goto out;
                }
            }
        }
        if (s_printing && cl == 0) {
            s_printing = false;
            ESP_LOGI(TAG, "relay: document finished");
        }
        if (len >= 0 && len <= 1200 && s_poll_len) {
            ESP_LOGI(TAG, "relay: poll (body %ld)", len);
            cl = len;
            *hold = true;
            *keepalive = 1;
            p_len = 0;
        } else {
            cl = len;
            if (len > 1200)
                s_job_open = true;
        }
        (void)state_job;
        if (s_ka) {
            if (!memmem(rx, (size_t)end, "Connection:", 11))
                set_connection((char *)rx, (size_t)end, hlen, cap);
            *keepalive = 1;
        }
        *frag = F_FLUSH;
    }

    if (*frag == F_FLUSH) {
        *frag = F_FULL;
        if (cl > 1200 || cl < 0)
            s_printing = true;
        size_t bstart = end;
        int have = *hlen - (int)bstart;
        bool chunked = cl < 0;
        int body = have;
        /* The portal stalls on a head sent without any body byte behind it, so
         * wait for the first body bytes before the head goes out. */
        if (!chunked && cl > 0 && have == 0) {
            *frag = F_REQ;
            goto out;
        }
        /* The portal answers 400 when a control request arrives in pieces, so
         * a small body is fully gathered first; documents keep streaming. */
        ESP_LOGI(TAG, "relay: gather h%d c%ld b%u cap%u ch%d", have, cl, (unsigned)bstart, (unsigned)cap, chunked);
        if ((!chunked && cl > have && bstart + (size_t)cl <= (size_t)cap &&
             (int)bstart + have < 2000) ||
            /* a chunked body has no length to wait for, but the portal reads
             * the request head a lot more reliably when a body prefix of at
             * least one OUT packet is already behind it */
            (chunked && have < 1200 && (int)bstart + have < (int)cap - 64)) {
            ESP_LOGI(TAG, "relay: gathering");
            *frag = F_REQ;
            goto out;
        }
        /* Two requests glued back to back: the next head is already buffered
         * behind this body. Trim the body to the value in the header (adjusted
         * for the rewrites above) and keep the rest for the next request. */
        if (!chunked && have > (int)cl && cl >= 0) {
            have = (int)cl;
            *hlen = (int)bstart + have;
        }
        bool ops = have > 8 && rx[bstart] == 2 && rx[bstart + 1] == 0 && rx[bstart + 2] == 0;
        if (body > 0) {
            int nl = rewrite_uri(rx + bstart, have);
            // patch_ops may extend the list, so it runs before the length is set
            if (ops)
                patch_ops(rx + bstart, nl);
            if (chunked) {
                cl = -1;
                body = nl;
            } else {
                /* The length must describe the body that actually goes out.
                 * It is clamped here, so a short read can never make the
                 * stream outrun the header. */
                int pending = nl - have;            // grown by patch_ops
                long want = g_orig + pending;
                /* send only the bytes that are really buffered; the rest
                 * follows through the socket instead of reading past them */
                body = have;
                if (body > want)
                    body = (int)want;
                cl = want - body;
                int sh = set_length((char *)rx, bstart, hlen, cap, want);
                end = (size_t)((int)end + sh);      // the head moved with it
                bstart = (size_t)((int)bstart + sh);
            }
        }
        if (cl < 0) {
            cs = CS_SIZE;
            szn2 = 0;
            skip_left = 0;
            pfin = false;
            if (body > 0)
                body = chunk_walk(rx + bstart, body, &cs, &chunk_left, &skip_left, &last_chunk, szbuf, &szn2,
                                  &pfin);
        }
        ESP_LOGI(TAG, "relay: flush body %d cl %ld pfx %u", body, cl, (unsigned)end);
        /* The portal only starts parsing a request when the head arrives
         * together with body bytes: a head sent alone makes it treat the body
         * as a runaway request line (414). Merge them when it fits. */
        /* The portal stalls the OUT endpoint on a head-only packet, so the head
         * always leaves together with whatever body prefix is buffered. */
        bool ok;
        if (body > 0 && end + (size_t)body <= cap) {
            ok = send_out(out_free, rx, (int)(end + (size_t)body)) == 0;
            *sent = (int)(end + (size_t)body);
            body = 0;
        } else {
            ok = send_out(out_free, rx, (int)end) == 0 &&
                 (body <= 0 || send_out(out_free, rx + bstart, body) == 0);
            *sent = (int)bstart + body;
        }
        if (ok) {
            *off_p = n;
        } else {
            fail = true;
            *off_p = 0;
        }
        if (cl == 0 || pfin) {                  // complete request
            *frag = F_REQ;
            *sent = *off_p;
            pfin = false;
            spill = *hlen - (int)end;
            cl = 0;
            memset(rx, 0, end);
            memmove(rx, rx + end, (size_t)spill);   // the rest is the next head
            *hlen = spill > 0 ? spill : 0;
            end = 0;
        }
        if (fail)
            return -1;
        goto tail;
    }

    // F_FULL: stream the remainder of this request straight through. off_i is
    // the buffer offset the fresh bytes were read to, not a count.
    int body = n;
    if (body > 0) {
        if (cl > 0) {
            if (body > cl) body = (int)cl;
            cl -= body;
        } else if (cl < 0) {
            body = chunk_walk(rx + off_i, body, &cs, &chunk_left, &skip_left, &last_chunk, szbuf, &szn2, &pfin);
        }
        if (body > 0 && send_out(out_free, rx + off_i, body) != 0)
            fail = true;
        /* Compact behind the send so the read window cannot shrink to
         * nothing as the document streams. */
        if (n > body)
            memmove(rx, rx + off_i + body, (size_t)(n - body));
        *hlen = n - body;
        *sent = n;
        *off_p = 0;
    }
    if (fail)
        return -1;
tail:
    if (cl == 0 || pfin) {
        *frag = F_REQ;
        *hlen = 0;
        pfin = false;
        spill = cl = 0;
        if (*off_p < n) {                       // leftovers start the next request
            memcpy(rx, rx + *off_p, (size_t)(n - *off_p));
            *hlen = n - *off_p;
        }
    }
    if ((size_t)end + (size_t)*hlen > cap)
        *hole = true;

out:
    g_spill = spill;
    g_orig = orig;
    g_cl = cl;
    g_chunk_left = chunk_left;
    g_skip_left = skip_left;
    g_cs = cs;
    g_pfin = pfin;
    g_last_chunk = last_chunk;
    g_szn = szn2;
    return *frag;
}

static void relay_conn_task(void *arg)
{
    relay_conn_t *c = arg;
    relay_slot_t *sl = c->slot;
    int fd = c->fd;
    usb_transfer_t *in_x = sl->in;
    QueueHandle_t out_free = sl->out_free;
    SemaphoreHandle_t in_sem = sl->in_sem;
    uint8_t *buf = malloc(RELAY_OUT_SIZE);
    uint8_t *rx = malloc(RELAY_IN_SIZE);
    char *head = malloc(RELAY_HEAD_MAX);
    int hlen = 0;
    int frag = F_REQ;
    int off = 0, sent = 0, keepalive = 0;
    bool hole = false, held = false, ops_patch = false;
    bool job = true;
    if (!buf || !rx || !head)
        goto done;

    /* The portal keeps one job store, and the job id from Create-Job has to
     * still exist when Send-Document arrives. Re-dialling per connection
     * therefore loses the job, so the portal is opened once and only rebuilt
     * after an error or /wake. */
    /* A stalled portal stays dead for the next client, so a connection that
     * ended badly rebuilds it; a clean one leaves it dialled. */


    // Clear both pipes and reset the printer class before the first byte: the
    // portal NAKs a request that follows a connection whose transfers were
    // left in flight, so a fresh connection starts from a known state.
    {
        /* Once per device only: a SOFT_RESET wipes the portal's job store,
         * and a Send-Document arriving on a fresh connection needs it. */
        static bool once;
        if (!once) {
            once = true;
            uint8_t creq = USB_BM_REQUEST_TYPE_DIR_OUT | USB_BM_REQUEST_TYPE_TYPE_CLASS |
                           USB_BM_REQUEST_TYPE_RECIP_INTERFACE;
            esp_err_t sr = ctrl_request(creq, 0x01, 0, sl->intf, 0, NULL, NULL);
            usb_host_endpoint_clear(s_p.dev, sl->ep_out);
            ESP_LOGI(TAG, "relay: first reset %s", esp_err_to_name(sr));
        }
    }
    // CUPS splits a lookup over two requests on the same connection, so the
    // operations list has to survive into the next request's body prefix
    int ops_left = 0;
    int silent = 0;
    while (!sl->dead) {
        // printer -> client
        if (xSemaphoreTake(in_sem, 0) == pdTRUE) {
            int n = in_x->actual_num_bytes;
            if (in_x->status != USB_TRANSFER_STATUS_COMPLETED) {
                ESP_LOGW(TAG, "relay: IN failed (%d)", in_x->status);
                break;
            }
            if (n > 12 && memcmp(in_x->data_buffer, "HTTP/1.", 7) == 0)
                ESP_LOGI(TAG, "relay: << %.*s", n > 48 ? 48 : n, in_x->data_buffer);
            {
                // report the IPP status wherever the body starts in this segment
                const uint8_t *p = in_x->data_buffer;
                for (int k = 0; k + 3 < n; k++)
                    if (p[k] == 2 && p[k + 1] == 0 && p[k + 2] == 0 && (k == 0 || p[k - 1] == '\n')) {
                        ESP_LOGI(TAG, "relay: ipp status %02x%02x", p[k + 2], p[k + 3]);
                        break;
                    }
            }
            if (ops_left > 0)
                ;
            if (n > 0)
                silent = 0;
            if (n > 0 && held)
                ESP_LOGI(TAG, "relay: poll answer %d", n);
            if (n > 0 && held) {
                /* Hold only until the printer's head is complete, then hand the
                 * answer over verbatim: its own chunked framing stays valid, so
                 * nothing has to be re-framed or re-measured. */
                int room = (int)sizeof p_search - p_len;
                int take = n < room ? n : room;
                memcpy(p_search + p_len, in_x->data_buffer, (size_t)take);
                p_len += take;
                if (memmem(p_search, (size_t)p_len, "\r\n\r\n", 4) || p_len >= (int)sizeof p_search) {
                    patch_ops(p_search, p_len);  /* the head and body arrive together */
                    send(fd, p_search, (size_t)p_len, 0);
                    held = false;
                    ops_patch = true;           /* the attribute list follows */
                }
            } else if (n > 0) {
                /* CUPS only ever uses Print-Job when the printer does not
                 * advertise Create-Job/Send-Document, and those two do not
                 * survive across connections. The list arrives in the poll
                 * answer, so it is rewritten in the first body segment. */
                if (ops_patch)
                    patch_ops(in_x->data_buffer, n);
                if (send(fd, in_x->data_buffer, n, 0) < 0)
                    break;
            }
            in_x->num_bytes = RELAY_IN_SIZE;
            if (usb_host_transfer_submit(in_x) != ESP_OK)
                break;
        }
        // client -> printer: read into the space left after the unread bytes
        int rpos = (int)end + hlen;
        int room_rx = (int)RELAY_HEAD_MAX - rpos;
        if (room_rx <= 0) {
            ESP_LOGW(TAG, "relay: request head too long");
            break;
        }
        if (room_rx > RELAY_IN_SIZE)
            room_rx = RELAY_IN_SIZE;
        fd_set rd;
        FD_ZERO(&rd);
        FD_SET(fd, &rd);
        int wait_us = silent > 1000000 ? 100000 : 20000;
        struct timeval tv = {.tv_sec = 0, .tv_usec = wait_us};
        int sel = select(fd + 1, &rd, NULL, NULL, &tv);
        if (sel == 0) {
            silent += wait_us;
            if (sent > 0 && keepalive && frag == F_REQ) {   /* tells us to keep it open */
                keepalive = 0;
                continue;
            }
            if (silent >= 15000000 && frag == F_REQ && hlen == 0) {
                ESP_LOGI(TAG, "relay: idle 15 s, closing");
                break;
            }
            continue;
        }
        if (sel < 0)
            break;
        int n = recv(fd, head + rpos, (size_t)room_rx, 0);
        if (n <= 0)
            break;
        sent = 0;
        off = rpos;                             /* where the fresh bytes sit */
        if (frame_req((uint8_t *)head, RELAY_HEAD_MAX, &hlen, &frag, &off, n, &sent, fd, out_free, &ops_left, &keepalive, &hole, &held, job) < 0)
            break;
        if (frag == F_REQ && hlen == 0)
            end = 0;
        if (sent < n && off < n) {              // unread bytes shuffle down
            memmove(head + end, head + off, (size_t)(n - off));
            hlen = (int)(n - off);
        } else if (sent < n) {
            hlen = (int)(n - sent);
        }
        off = 0;
        if (frag == F_REQ && hlen > 0)          // leftover starts a new head
            ;
    }
done:
    free(buf);
    free(rx);
    free(head);
    close(fd);
    free(c);
    sl->used = false;
    if (sl->dead)
        relay_reopen();
    xSemaphoreGive(s_slot_free);
    vTaskDelete(NULL);
}

static void relay_accept_task(void *arg)
{
    int port = (int)(intptr_t)arg;
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    if (ls < 0) {
        ESP_LOGE(TAG, "relay: no socket");
        vTaskDelete(NULL);
        return;
    }
    int one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(port), .sin_addr.s_addr = htonl(INADDR_ANY)};
    if (bind(ls, (struct sockaddr *)&a, sizeof(a)) != 0 || listen(ls, 2) != 0) {
        ESP_LOGE(TAG, "relay: bind/listen %d failed", port);
        close(ls);
        vTaskDelete(NULL);
        return;
    }
    ESP_LOGI(TAG, "relay: listening on %d", port);
    for (;;) {
        struct sockaddr_in peer;
        socklen_t plen = sizeof(peer);
        int fd = accept(ls, (struct sockaddr *)&peer, &plen);
        if (fd < 0)
            continue;
        // the printer keeps one job store per portal, so every connection
        // shares a single portal; a second connection waits its turn
        if (xSemaphoreTake(s_slot_free, pdMS_TO_TICKS(30000)) != pdTRUE) {
            ESP_LOGW(TAG, "relay: portal busy, dropping connection");
            close(fd);
            continue;
        }
        relay_slot_t *slot = NULL;
        xSemaphoreTake(s_relay_guard, portMAX_DELAY);
        for (int i = 0; i < RELAY_SLOTS; i++)
            if (s_relay[i].out && !s_relay[i].used) {
                s_relay[i].used = true;
                slot = &s_relay[i];
                break;
            }
        xSemaphoreGive(s_relay_guard);
        if (!slot) {
            ESP_LOGW(TAG, "relay: no free portal");
            close(fd);
            xSemaphoreGive(s_slot_free);
            continue;
        }
        relay_conn_t *c = malloc(sizeof(*c));
        if (!c) {
            close(fd);
            slot->used = false;
            xSemaphoreGive(s_slot_free);
            continue;
        }
        c->fd = fd;
        c->slot = slot;
        ESP_LOGI(TAG, "relay: connection from %s", inet_ntoa(peer.sin_addr));
        if (xTaskCreate(relay_conn_task, "relay_conn", 4096, c, 4, NULL) != pdPASS) {
            close(fd);
            free(c);
            slot->used = false;
            xSemaphoreGive(s_slot_free);
        }
    }
}

// The IPP-USB portal has one job store, so a connection must outlive the
// request that started a job. Rather than park the portal on every close,
// reopen it after each connection; a reopen is cheaper than stranding the job.
static void relay_reopen(void)
{
    if (!s_relay_guard)
        return;
    xSemaphoreTake(s_relay_guard, portMAX_DELAY);
    for (int i = 0; i < RELAY_SLOTS; i++) {
        relay_slot_t *sl = &s_relay[i];
        if (sl->dead || !sl->intf)
            continue;
        if (sl->out)
            usb_host_transfer_free(sl->out);
        if (sl->in)
            usb_host_transfer_free(sl->in);
        sl->out = sl->in = NULL;
        sl->dead = true;
        if (usb_host_interface_release(s_p.client, s_p.dev, sl->intf) == ESP_OK) {
            if (usb_host_interface_claim(s_p.client, s_p.dev, sl->intf, 1) == ESP_OK) {
                uint8_t req = USB_BM_REQUEST_TYPE_DIR_OUT | USB_BM_REQUEST_TYPE_TYPE_STANDARD |
                              USB_BM_REQUEST_TYPE_RECIP_INTERFACE;
                ctrl_request(req, USB_B_REQUEST_SET_INTERFACE, 1, sl->intf, 0, NULL, NULL);
                uint8_t creq2 = USB_BM_REQUEST_TYPE_DIR_OUT | USB_BM_REQUEST_TYPE_TYPE_CLASS |
                                USB_BM_REQUEST_TYPE_RECIP_INTERFACE;
                ctrl_request(creq2, 0x01, 0, sl->intf, 0, NULL, NULL);
                if (usb_host_transfer_alloc(RELAY_OUT_SIZE, 0, &sl->out) == ESP_OK &&
                    usb_host_transfer_alloc(RELAY_IN_SIZE, 0, &sl->in) == ESP_OK) {
                    sl->out->device_handle = sl->in->device_handle = s_p.dev;
                    sl->out->bEndpointAddress = sl->ep_out;
                    sl->in->bEndpointAddress = sl->ep_in;
                    sl->out->context = sl;
                    sl->in->context = sl->in_sem;
                    sl->out->callback = relay_out_cb;
                    sl->in->callback = relay_in_cb;
                    sl->dead = false;
                    usb_host_transfer_submit(sl->in);
                    ESP_LOGI(TAG, "relay: portal %d reopened", sl->intf);
                    break;
                }
            }
            ESP_LOGW(TAG, "relay: reopen %d failed", sl->intf);
        }
    }
    xSemaphoreGive(s_relay_guard);
}

// Unplug/replug: parks every portal until relay_setup rebuilds it. Old
// transfers and handles are deliberately leaked rather than freed, so a late
// callback can never touch freed memory.
void usb_printer_relay_reset(void)
{
    if (!s_relay_guard)
        return;
    xSemaphoreTake(s_relay_guard, portMAX_DELAY);
    for (int i = 0; i < RELAY_SLOTS; i++) {
        s_relay[i].used = false;
        s_relay[i].dead = true;
        s_relay[i].out = NULL;
        s_relay[i].in = NULL;
    }
    xSemaphoreGive(s_relay_guard);
}

esp_err_t usb_printer_relay_start(uint16_t port)
{
    s_relay_guard = xSemaphoreCreateMutex();
    s_slot_free = xSemaphoreCreateCounting(RELAY_SLOTS, RELAY_SLOTS);
    if (!s_relay_guard || !s_slot_free)
        return ESP_ERR_NO_MEM;
    return xTaskCreate(relay_accept_task, "relay_accept", 3072, (void *)(intptr_t)port, 4, NULL) == pdPASS
               ? ESP_OK
               : ESP_ERR_NO_MEM;
}

#endif  // PRINTER_RELAY

static void open_device(uint8_t addr)
{
    if (s_p.dev) {
        ESP_LOGW(TAG, "ignoring device %d: a printer is already attached", addr);
        return;
    }
    ESP_LOGI(TAG, "new USB device at address %d", addr);
    if (usb_host_device_open(s_p.client, addr, &s_p.dev) != ESP_OK) {
        s_p.dev = NULL;
        return;
    }
    const usb_device_desc_t *dd;
    const usb_config_desc_t *cfg;
    usb_device_info_t di;
    usb_host_get_device_descriptor(s_p.dev, &dd);
    usb_host_get_active_config_descriptor(s_p.dev, &cfg);
    usb_host_device_info(s_p.dev, &di);

    usb_printer_info_t info = {.vid = dd->idVendor, .pid = dd->idProduct};
    str_desc_ascii(di.str_desc_manufacturer, info.manufacturer, sizeof(info.manufacturer));
    str_desc_ascii(di.str_desc_product, info.product, sizeof(info.product));
    str_desc_ascii(di.str_desc_serial_num, info.serial, sizeof(info.serial));
    ESP_LOGI(TAG, "%04x:%04x \"%s\" \"%s\" serial \"%s\"", info.vid, info.pid, info.manufacturer, info.product,
             info.serial);

    bool have_intf = false;
#ifdef PRINTER_FORCE_INTF
    have_intf = find_forced_interface(cfg, PRINTER_FORCE_INTF);
    if (have_intf)
        ESP_LOGW(TAG, "DEBUG override: interface %d, bulk OUT 0x%02x, bulk IN 0x%02x", s_p.intf, s_p.ep_out,
                 s_p.ep_in);
    else
        ESP_LOGW(TAG, "DEBUG override: interface %d not found, falling back", PRINTER_FORCE_INTF);
#endif
    if (!have_intf && !find_printer_interface(cfg)) {
        ESP_LOGW(TAG, "not a printer (no printer-class interface with a bulk OUT endpoint), ignoring");
        usb_host_device_close(s_p.client, s_p.dev);
        s_p.dev = NULL;
        return;
    }
    info.bidirectional = s_p.ep_in != 0;
    log_interfaces(cfg);
    ESP_LOGI(TAG, "printer interface %d alt %d, bulk OUT 0x%02x, bulk IN 0x%02x", s_p.intf, s_p.alt, s_p.ep_out,
             s_p.ep_in);

    esp_err_t err = usb_host_transfer_alloc(CTRL_XFER_SIZE, 0, &s_p.ctrl);
    if (err == ESP_OK)
        s_p.ctrl->callback = ctrl_cb;   // must exist before any control transfer
    for (int i = 0; i < OUT_XFER_COUNT && err == ESP_OK; i++)
        err = usb_host_transfer_alloc(OUT_XFER_SIZE, 0, &s_p.out[i]);
    if (err == ESP_OK)
        err = usb_host_transfer_alloc(IN_XFER_SIZE, 0, &s_p.in);
    if (err == ESP_OK)
        err = usb_host_interface_claim(s_p.client, s_p.dev, s_p.intf, s_p.alt);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "setup failed: %s", esp_err_to_name(err));
        release_device();
        return;
    }
    s_p.claimed = true;
    s_p.scan_stuck = false;
#if defined(PRINTER_RELAY)
    // IPP-USB portals stay claimed for the relay; the scan interface is skipped
    // because claiming it would clear the alt setting.
    relay_setup(cfg);
#elif defined(PRINTER_IPPUSB_PROBE)
    // IPP-USB portal: claim it and prove HTTP works. The scan interface is
    // skipped while probing, because claiming it would clear the alt setting.
    probe_ippusb(cfg, PRINTER_IPPUSB_PROBE);
    probe_ippusb(cfg, PRINTER_IPPUSB_PROBE + 1);
#else
    if (find_scanner_interface(cfg)) {
        err = usb_host_interface_claim(s_p.client, s_p.dev, s_p.scan_intf, 0);
        s_p.scan_claimed = err == ESP_OK;
        if (s_p.scan_claimed)
            ESP_LOGI(TAG, "scanner interface %d, bulk OUT 0x%02x, bulk IN 0x%02x (max packet %d)", s_p.scan_intf,
                     s_p.scan_ep_out, s_p.scan_ep_in, s_p.scan_mps);
        else
            ESP_LOGW(TAG, "cannot claim scanner interface %d: %s", s_p.scan_intf, esp_err_to_name(err));
    } else {
        ESP_LOGI(TAG, "no scanner interface");
    }
#endif
    s_p.ctrl->callback = ctrl_cb;
    for (int i = 0; i < OUT_XFER_COUNT; i++) {
        s_p.out[i]->device_handle = s_p.dev;
        s_p.out[i]->bEndpointAddress = s_p.ep_out;
        s_p.out[i]->callback = out_cb;
        xQueueSend(s_p.out_free, &s_p.out[i], 0);
    }
    s_p.in->device_handle = s_p.dev;
    s_p.in->bEndpointAddress = s_p.ep_in;
    s_p.in->callback = in_cb;

    if (s_p.alt != 0) {
        uint8_t req = USB_BM_REQUEST_TYPE_DIR_OUT | USB_BM_REQUEST_TYPE_TYPE_STANDARD |
                      USB_BM_REQUEST_TYPE_RECIP_INTERFACE;
        if (ctrl_request(req, USB_B_REQUEST_SET_INTERFACE, s_p.alt, s_p.intf, 0, NULL, NULL) != ESP_OK)
            ESP_LOGW(TAG, "SET_INTERFACE alt %d failed", s_p.alt);
    }

    // IEEE 1284 Device ID: 2-byte big-endian length (including itself) + text
    static uint8_t idbuf[1024];
    size_t got = 0;
    uint8_t req = USB_BM_REQUEST_TYPE_DIR_IN | USB_BM_REQUEST_TYPE_TYPE_CLASS | USB_BM_REQUEST_TYPE_RECIP_INTERFACE;
    if (ctrl_request(req, PRN_REQ_GET_DEVICE_ID, cfg->bConfigurationValue - 1, (s_p.intf << 8) | s_p.alt,
                     sizeof(idbuf) - 1, idbuf, &got) == ESP_OK && got > 2) {
        size_t len = ((size_t)idbuf[0] << 8 | idbuf[1]);
        if (len < 2 || len > got)
            len = got;
        len -= 2;
        if (len >= sizeof(info.device_id))
            len = sizeof(info.device_id) - 1;
        memcpy(info.device_id, idbuf + 2, len);
        info.device_id[len] = 0;
        ESP_LOGI(TAG, "IEEE 1284 ID: %s", info.device_id);
    } else {
        ESP_LOGW(TAG, "GET_DEVICE_ID failed");
    }
    uint8_t st;
    if (usb_printer_port_status(&st) == ESP_OK)
        ESP_LOGI(TAG, "port status 0x%02x (%s%s%s)", st, (st & 0x20) ? "paper empty " : "",
                 (st & 0x10) ? "selected " : "", (st & 0x08) ? "no error" : "ERROR");

    if (info.vid != CANON_VID)
        ESP_LOGW(TAG, "not a Canon printer: Canon raster jobs will print garbage on it");

    xSemaphoreTake(s_p.lock, portMAX_DELAY);
    s_p.info = info;
    s_p.resp_len = 0;
    s_p.resp[0] = 0;
    xSemaphoreGive(s_p.lock);
    s_p.msg_len = 0;
    s_p.last_status[0] = 0;
    memset(&s_p.status, 0, sizeof(s_p.status));
    s_p.in_stopped = false;
    s_p.connected = true;
    in_submit();
    if (s_p.conn_cb)
        s_p.conn_cb(true, &info);
}

static void abort_endpoint(uint8_t ep)
{
    if (!ep || !s_p.dev)
        return;
    usb_host_endpoint_halt(s_p.dev, ep);
    usb_host_endpoint_flush(s_p.dev, ep);
    usb_host_endpoint_clear(s_p.dev, ep);
}

static void client_event_cb(const usb_host_client_event_msg_t *msg, void *arg)
{
    if (msg->event == USB_HOST_CLIENT_EVENT_NEW_DEV)
        s_p.pending_addr = msg->new_dev.address;
    else if (msg->event == USB_HOST_CLIENT_EVENT_DEV_GONE && msg->dev_gone.dev_hdl == s_p.dev)
        s_p.gone = true;
}

static void client_task(void *arg)
{
    bool teardown = false;
    while (1) {
        usb_host_client_handle_events(s_p.client, pdMS_TO_TICKS(50));
        if (s_p.pending_addr && !teardown) {   // a re-plug waits for the old teardown
            uint8_t addr = s_p.pending_addr;
            s_p.pending_addr = 0;
            open_device(addr);
        }
        if (s_p.connected && !s_p.gone && !s_p.in_inflight && !s_p.scan_active &&
            xTaskGetTickCount() - s_p.in_last >= pdMS_TO_TICKS(s_p.job_active ? IN_POLL_JOB_MS : IN_POLL_IDLE_MS))
            in_submit();
        if (s_p.gone && !teardown) {
            ESP_LOGW(TAG, "printer disconnected");
#ifdef PRINTER_RELAY
            usb_printer_relay_reset();
#endif
            teardown = true;
            s_p.connected = false;
            s_p.cancel = true;
            if (s_p.conn_cb)
                s_p.conn_cb(false, NULL);
        }
        // free everything once the job and all transfers are finished
        if (teardown && !s_p.job_active) {
            if (atomic_load(&s_p.out_inflight) > 0)
                abort_endpoint(s_p.ep_out);
            if (s_p.in_inflight)
                abort_endpoint(s_p.ep_in);
            if (atomic_load(&s_p.out_inflight) == 0 && !s_p.in_inflight) {
                release_device();
                s_p.gone = false;
                s_p.cancel = false;
                teardown = false;
                ESP_LOGI(TAG, "printer resources released");
            }
        }
    }
}

static void lib_task(void *arg)
{
    while (1) {
        uint32_t flags;
        usb_host_lib_handle_events(portMAX_DELAY, &flags);
        if (flags & USB_HOST_LIB_EVENT_FLAGS_NO_CLIENTS)
            usb_host_device_free_all();
    }
}

// ---------------------------------------------------------------------------
// public API

esp_err_t usb_printer_start(usb_printer_conn_cb_t cb)
{
    s_p.conn_cb = cb;
    s_p.lock = xSemaphoreCreateMutex();
    s_p.ctrl_mutex = xSemaphoreCreateMutex();
    s_p.ctrl_sem = xSemaphoreCreateBinary();
    s_p.out_free = xQueueCreate(OUT_XFER_COUNT, sizeof(usb_transfer_t *));
    s_p.scan_out_sem = xSemaphoreCreateBinary();
    s_p.scan_in_sem = xSemaphoreCreateBinary();
    if (!s_p.lock || !s_p.ctrl_mutex || !s_p.ctrl_sem || !s_p.out_free || !s_p.scan_out_sem || !s_p.scan_in_sem)
        return ESP_ERR_NO_MEM;

    const usb_host_config_t host_cfg = {.skip_phy_setup = false, .intr_flags = ESP_INTR_FLAG_LEVEL1};
    ESP_RETURN_ON_ERROR(usb_host_install(&host_cfg), TAG, "usb_host_install");
    if (xTaskCreate(lib_task, "usb_lib", 4096, NULL, 6, NULL) != pdPASS)
        return ESP_ERR_NO_MEM;

    const usb_host_client_config_t client_cfg = {
        .is_synchronous = false,
        .max_num_event_msg = 5,
        .async = {.client_event_callback = client_event_cb, .callback_arg = NULL},
    };
    ESP_RETURN_ON_ERROR(usb_host_client_register(&client_cfg, &s_p.client), TAG, "client register");
    if (xTaskCreate(client_task, "usb_client", 6144, NULL, 5, &s_p.client_task) != pdPASS)
        return ESP_ERR_NO_MEM;
    ESP_LOGI(TAG, "USB host started, waiting for a printer");
    return ESP_OK;
}

bool usb_printer_connected(void)
{
    return s_p.connected && !s_p.gone;
}

bool usb_printer_job_active(void)
{
    return s_p.job_active;
}

bool usb_printer_get_status(usb_printer_status_t *out)
{
    if (!usb_printer_connected())
        return false;
    xSemaphoreTake(s_p.lock, portMAX_DELAY);
    *out = s_p.status;
    xSemaphoreGive(s_p.lock);
    return out->valid;
}

bool usb_printer_get_info(usb_printer_info_t *out)
{
    if (!usb_printer_connected())
        return false;
    xSemaphoreTake(s_p.lock, portMAX_DELAY);
    *out = s_p.info;
    xSemaphoreGive(s_p.lock);
    return true;
}

// Claims the device for a print job or a scan.
static bool claim_device(void)
{
    xSemaphoreTake(s_p.lock, portMAX_DELAY);
    bool ok = !s_p.job_active;
    s_p.job_active = true;
    xSemaphoreGive(s_p.lock);
    return ok;
}

esp_err_t usb_printer_job_begin(void)
{
    if (!usb_printer_connected())
        return ESP_ERR_INVALID_STATE;
    if (!claim_device())
        return ESP_ERR_INVALID_STATE;
    // transfers cancelled by a previous job may still be returning
    for (int i = 0; i < 100 && uxQueueMessagesWaiting(s_p.out_free) < OUT_XFER_COUNT; i++)
        vTaskDelay(pdMS_TO_TICKS(10));
    if (uxQueueMessagesWaiting(s_p.out_free) < OUT_XFER_COUNT) {
        s_p.job_active = false;
        return ESP_ERR_INVALID_STATE;
    }
    s_p.cancel = false;
    s_p.out_status = USB_TRANSFER_STATUS_COMPLETED;
    s_p.fill = NULL;
    xSemaphoreTake(s_p.lock, portMAX_DELAY);
    s_p.resp_len = 0;
    s_p.resp[0] = 0;
    xSemaphoreGive(s_p.lock);
    s_p.job_active = true;
    return ESP_OK;
}

static esp_err_t job_check(void)
{
    if (s_p.gone || !s_p.connected)
        return ESP_ERR_INVALID_STATE;
    if (s_p.cancel)
        return ESP_ERR_INVALID_STATE;
    if (s_p.out_status != USB_TRANSFER_STATUS_COMPLETED)
        return ESP_FAIL;
    return ESP_OK;
}

static void log_stall(TickType_t since)
{
    uint32_t secs = (xTaskGetTickCount() - since) / configTICK_RATE_HZ;
    uint8_t st = 0;
    if (usb_printer_port_status(&st) == ESP_OK)
        ESP_LOGW(TAG, "printer not accepting data for %lus, port status 0x%02x%s", (unsigned long)secs, st,
                 (st & 0x20) ? " (PAPER EMPTY?)" : "");
    else
        ESP_LOGW(TAG, "printer not accepting data for %lus", (unsigned long)secs);
}

// Waits for a free OUT transfer. The printer NAKs while its buffer is full,
// which can last as long as it takes to print (or until paper is loaded).
static esp_err_t get_fill_buffer(void)
{
    TickType_t since = xTaskGetTickCount(), last_log = since;
    while (!s_p.fill) {
        ESP_RETURN_ON_ERROR(job_check(), TAG, "job aborted");
        if (xQueueReceive(s_p.out_free, &s_p.fill, pdMS_TO_TICKS(200)) == pdTRUE) {
            s_p.fill->num_bytes = 0;
            break;
        }
        if (xTaskGetTickCount() - last_log > pdMS_TO_TICKS(15000)) {
            last_log = xTaskGetTickCount();
            log_stall(since);
        }
    }
    return ESP_OK;
}

static esp_err_t submit_fill(void)
{
    usb_transfer_t *x = s_p.fill;
    s_p.fill = NULL;
    if (x->num_bytes == 0) {
        xQueueSend(s_p.out_free, &x, 0);
        return ESP_OK;
    }
    atomic_fetch_add(&s_p.out_inflight, 1);
    esp_err_t err = usb_host_transfer_submit(x);
    if (err != ESP_OK) {
        atomic_fetch_sub(&s_p.out_inflight, 1);
        xQueueSend(s_p.out_free, &x, 0);
    }
    return err;
}

esp_err_t usb_printer_write(const uint8_t *data, size_t len)
{
    while (len > 0) {
        ESP_RETURN_ON_ERROR(get_fill_buffer(), TAG, "no buffer");
        size_t room = OUT_XFER_SIZE - s_p.fill->num_bytes;
        size_t n = len < room ? len : room;
        memcpy(s_p.fill->data_buffer + s_p.fill->num_bytes, data, n);
        s_p.fill->num_bytes += n;
        data += n;
        len -= n;
        if (s_p.fill->num_bytes == OUT_XFER_SIZE)
            ESP_RETURN_ON_ERROR(submit_fill(), TAG, "submit");
    }
    return ESP_OK;
}

esp_err_t usb_printer_flush(void)
{
    if (s_p.fill)
        ESP_RETURN_ON_ERROR(submit_fill(), TAG, "submit");
    TickType_t since = xTaskGetTickCount(), last_log = since;
    while (uxQueueMessagesWaiting(s_p.out_free) < OUT_XFER_COUNT) {
        ESP_RETURN_ON_ERROR(job_check(), TAG, "job aborted");
        vTaskDelay(pdMS_TO_TICKS(10));
        if (xTaskGetTickCount() - last_log > pdMS_TO_TICKS(15000)) {
            last_log = xTaskGetTickCount();
            log_stall(since);
        }
    }
    return job_check();
}

esp_err_t usb_printer_wait_response(const char *needle, uint32_t timeout_ms)
{
    if (!s_p.ep_in)
        return ESP_ERR_NOT_SUPPORTED;   // unidirectional printer: nothing to wait for
    TickType_t start = xTaskGetTickCount();
    do {
        xSemaphoreTake(s_p.lock, portMAX_DELAY);
        bool found = strstr(s_p.resp, needle) != NULL;
        xSemaphoreGive(s_p.lock);
        if (found)
            return ESP_OK;
        ESP_RETURN_ON_ERROR(job_check(), TAG, "job aborted");
        vTaskDelay(pdMS_TO_TICKS(50));
    } while (xTaskGetTickCount() - start < pdMS_TO_TICKS(timeout_ms));
    return ESP_ERR_TIMEOUT;
}

void usb_printer_job_end(void)
{
    // the job's last bytes (end of page, end of job) are still in the
    // transfer being filled unless the driver synced at the end: send them.
    // Not after a cancel or a failed transfer (job_check stops the flush).
    if (s_p.fill && s_p.fill->num_bytes && job_check() == ESP_OK && usb_printer_flush() != ESP_OK)
        ESP_LOGW(TAG, "the end of the job did not reach the printer");
    if (s_p.fill) {
        usb_transfer_t *x = s_p.fill;
        s_p.fill = NULL;
        xQueueSend(s_p.out_free, &x, 0);
    }
    // transfers still in flight after a cancel are reaped by abort_endpoint
    if (s_p.cancel && atomic_load(&s_p.out_inflight) > 0 && !s_p.gone)
        abort_endpoint(s_p.ep_out);
    s_p.job_active = false;
}

void usb_printer_cancel(void)
{
    if (!s_p.job_active)
        return;
    ESP_LOGW(TAG, "cancelling job");
    s_p.cancel = true;
}

esp_err_t usb_printer_soft_reset(void)
{
    uint8_t req = USB_BM_REQUEST_TYPE_DIR_OUT | USB_BM_REQUEST_TYPE_TYPE_CLASS | USB_BM_REQUEST_TYPE_RECIP_INTERFACE;
    return ctrl_request(req, PRN_REQ_SOFT_RESET, 0, s_p.intf, 0, NULL, NULL);
}

esp_err_t usb_printer_port_status(uint8_t *status)
{
    uint8_t req = USB_BM_REQUEST_TYPE_DIR_IN | USB_BM_REQUEST_TYPE_TYPE_CLASS | USB_BM_REQUEST_TYPE_RECIP_INTERFACE;
    size_t got = 0;
    esp_err_t err = ctrl_request(req, PRN_REQ_GET_PORT_STATUS, 0, s_p.intf, 1, status, &got);
    return (err == ESP_OK && got != 1) ? ESP_FAIL : err;
}

// ---------------------------------------------------------------------------
// scanner

static void scan_cb(usb_transfer_t *xfer)
{
    xSemaphoreGive((SemaphoreHandle_t)xfer->context);
}

bool usb_scanner_present(void)
{
    return usb_printer_connected() && s_p.scan_claimed && !s_p.scan_stuck;
}

bool usb_scanner_active(void)
{
    return s_p.scan_active;
}

esp_err_t usb_scanner_begin(void)
{
    if (!usb_scanner_present())
        return ESP_ERR_NOT_FOUND;
    if (!claim_device())
        return ESP_ERR_INVALID_STATE;
    esp_err_t err = usb_host_transfer_alloc(SCAN_OUT_SIZE, 0, &s_p.scan_out);
    if (err == ESP_OK)
        err = usb_host_transfer_alloc(SCAN_IN_SIZE, 0, &s_p.scan_in);
    if (err != ESP_OK) {
        free_scan_transfers();
        s_p.job_active = false;
        return err;
    }
    s_p.scan_out->device_handle = s_p.scan_in->device_handle = s_p.dev;
    s_p.scan_out->bEndpointAddress = s_p.scan_ep_out;
    s_p.scan_in->bEndpointAddress = s_p.scan_ep_in;
    s_p.scan_out->callback = s_p.scan_in->callback = scan_cb;
    s_p.scan_out->context = s_p.scan_out_sem;
    s_p.scan_in->context = s_p.scan_in_sem;
    s_p.scan_active = true;
    return ESP_OK;
}

// Runs one transfer; bytes transferred or USB_SCAN_TIMEOUT / USB_SCAN_ERROR.
static int scan_xfer(usb_transfer_t *x, size_t num_bytes, uint32_t timeout_ms)
{
    if (!s_p.scan_active || s_p.gone || s_p.scan_stuck)
        return USB_SCAN_ERROR;
    SemaphoreHandle_t sem = x->context;
    xSemaphoreTake(sem, 0);
    x->num_bytes = (int)num_bytes;
    if (usb_host_transfer_submit(x) != ESP_OK)
        return USB_SCAN_ERROR;
    bool timed_out = xSemaphoreTake(sem, pdMS_TO_TICKS(timeout_ms)) != pdTRUE;
    if (timed_out) {
        // stop it: the callback still comes, with a cancelled status
        abort_endpoint(x->bEndpointAddress);
        if (xSemaphoreTake(sem, pdMS_TO_TICKS(1000)) != pdTRUE) {
            ESP_LOGE(TAG, "scanner transfer on 0x%02x did not come back", x->bEndpointAddress);
            s_p.scan_stuck = true;
            return USB_SCAN_ERROR;
        }
    }
    if (x->status == USB_TRANSFER_STATUS_COMPLETED)
        return x->actual_num_bytes;     // also when it completed just as it timed out
    if (timed_out)
        return USB_SCAN_TIMEOUT;
    ESP_LOGW(TAG, "scanner transfer on 0x%02x failed, status %d", x->bEndpointAddress, x->status);
    return USB_SCAN_ERROR;
}

int usb_scanner_write(const uint8_t *data, size_t len, uint32_t timeout_ms)
{
    if (len > SCAN_OUT_SIZE || !s_p.scan_out)
        return USB_SCAN_ERROR;
    memcpy(s_p.scan_out->data_buffer, data, len);
    return scan_xfer(s_p.scan_out, len, timeout_ms);
}

int usb_scanner_read(uint8_t *buf, size_t len, uint32_t timeout_ms)
{
    if (!s_p.scan_in)
        return USB_SCAN_ERROR;
    if (len > SCAN_IN_SIZE)
        len = SCAN_IN_SIZE;
    // IN transfers must be a multiple of the max packet size
    size_t n = (len + s_p.scan_mps - 1) / s_p.scan_mps * s_p.scan_mps;
    if (n > SCAN_IN_SIZE)
        n = SCAN_IN_SIZE / s_p.scan_mps * s_p.scan_mps;
    int r = scan_xfer(s_p.scan_in, n, timeout_ms);
    if (r > (int)len) {
        ESP_LOGW(TAG, "scanner sent %d bytes, %u expected: rest dropped", r, (unsigned)len);
        r = (int)len;
    }
    if (r > 0)
        memcpy(buf, s_p.scan_in->data_buffer, (size_t)r);
    return r;
}

void usb_scanner_end(void)
{
    if (!s_p.scan_active)
        return;
    s_p.scan_active = false;
    if (!s_p.scan_stuck)
        free_scan_transfers();
    s_p.in_last = xTaskGetTickCount();     // resume the printer's status polling later
    s_p.job_active = false;
}
