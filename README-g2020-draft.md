# AirPrint relay for USB printers the stock firmware can't drive

A fork of [ngxson/esp32-usb-to-airprint](https://github.com/ngxson/esp32-usb-to-airprint)
that adds an **IPP-over-USB relay**. Instead of rendering a printer's own
language, the board advertises AirPrint and forwards each HTTP/IPP request into
the printer's own IPP-USB portal, so a printer that the stock firmware cannot
drive still prints from a phone or a Mac with no computer in the path.

Built and measured on a **Canon PIXMA G2020** (`04a9:18bd`). That model is the
reason the fork exists: the stock firmware's Canon (BJL) driver detects it,
accepts a job and records `completed`, while nothing comes out of the printer.
The G2020 refuses the BJL print service (`StartJobResponse NG NotSupportService`)
and its real channel is IPP over USB.

## Status

| Path | State |
|---|---|
| Mac, queue on port 631 via the advertised name | **verified** — jobs complete, paper moves |
| A single chunked `Print-Job` (68 KB real Apple Raster), the exact request an iPhone sends | **verified** — relayed, paper moves |
| iPhone | **in progress** — lists the printer, discovery and Validate-Job reach the relay, but the sheet drops the printer before the job stage |
| Android | **in progress** — lists the printer, then the sheet sits on "Preparing preview" |

The remaining work is in the `Get-Printer-Attributes` body, not the transport.
`ipptool` still reports one structural defect on the reply, so the next commit is
a byte-level fix to the answer, not another driver port.

## What it does

- `MODE=debug` gives an HTTP server on **8080** (`/info`, `/log`, `/coredump`,
  `POST /reboot`) so the board can be followed and updated over WiFi with no
  serial console.
- A relay on **631** that advertises `_ipp._tcp` (with the `_universal` and
  `_print` subtypes iOS browses) and forwards each request into the portal.
- `Get-Printer-Attributes` (`0x0009` **and** `0x000b`), `Validate-Job` and
  `Get-Job-Attributes` are answered by the board itself. The portal's answer is
  too thin for iOS and it never answers `Validate-Job` at all.
- Whole-body buffering with chunked framing resolved into `Content-Length`, and a
  USB emit that loops in 4096-byte slices, so a 68 KB phone job no longer
  overruns the transfer buffer and reboots the board.

## Hardware

See the upstream hardware notes. In short: an ESP32-S3 with the native USB-OTG
port, a passive USB-C OTG splitter for power, and **no hub** between the board and
the printer. Flashed to 8 MB with the partition table in `partitions.csv`.

## Build

```bash
cp .env.example .env      # WIFI_SSID, WIFI_PASSWORD, PRINTER_NAME, HOSTNAME
pio run -t upload
```

`.env` is git-ignored; `src/local_config.h` is generated from it at build time,
so rename the printer or host there and rebuild rather than editing the header.

## Verify without a phone

```bash
ipptool -tv ipp://<host>.local:631/ipp/print test/gpa.test   # capability answer
lpadmin -p test -v ipp://<host>.local:631/ipp/print -m everywhere -E
lp -d test /usr/share/cups/data/testprint
```

`lpadmin -m everywhere` runs CUPS' own driverless PPD builder over the answer and
is the cheapest gate: when it succeeds, a driverless client will accept the
printer.

## Follow the printer's real log

`GET http://<host>:8080/log` shows the requests, the portal state and the USB
transfers. `GET /info` shows the portal interface and free memory. Judge a job by
paper, never by a status field: `completed` means the bytes left the board.

## Notes worth carrying over to other printers

- Apple Raster is listed by iOS as `urf-supported`, so a printer that only does
  PWG raster usually still accepts `image/urf` through CUPS' `cgpdftoraster`.
- Do not put a USB hub in the path: ESP-IDF's host stack has limited hub support
  and the printer fails to enumerate.
- `Content-Length` must be recomputed for every rewritten head, and
  `Host: localhost` is mandatory for the portal.
- An attribute answer that parses but repeats a set's name on every value is
  rejected by CUPS as duplicate attributes; a set names only its first value.

## License

The upstream project's license and copyrights are kept (MIT, see `LICENSE`).
`src/usb_printer.c` carries the G2020 relay work.
