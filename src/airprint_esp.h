// AirPrint on the ESP32: connects the portable server (components/airprint)
// to the USB printer and announces it with mDNS / DNS-SD.
#pragma once

#include <stdbool.h>

#include "esp_err.h"

// on_job_state(true/false) when an AirPrint job starts/stops (LED).
esp_err_t airprint_esp_start(const char *hostname, void (*on_job_state)(bool printing));
// print_job_driver() changed (a printer of another model was plugged in):
// IPP, eSCL and mDNS now name that model. Nothing before airprint_esp_start.
void airprint_esp_driver_changed(void);
// Relay mode: advertise _ipp._tcp on `port` for an IPP-USB passthrough, without
// the driver path. Call once after the relay is listening.
esp_err_t airprint_relay_announce(const char *hostname, uint16_t port);
