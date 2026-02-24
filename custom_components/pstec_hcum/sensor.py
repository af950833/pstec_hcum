
import asyncio
import logging
import datetime
import json
import os
import calendar
import struct
from typing import Optional, List, Dict, Any

from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.event import async_track_time_interval, async_track_time_change

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

_LOGGER = logging.getLogger(__name__)


def _crc16_modbus(data: bytes) -> int:
    """Modbus RTU CRC16 (poly 0xA001, init 0xFFFF)."""
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF


def _sync_read_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _sync_write_json_atomic(path: str, data, *, indent: int = 4):
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


class JSONFileEventHandler(FileSystemEventHandler):
    def __init__(self, usage_sensors, hass):
        super().__init__()
        self._usage_sensors = usage_sensors
        self.hass = hass

    def _handle(self, path: str):
        if not path.endswith(".json"):
            return

        async def _refresh():
            for sensor in self._usage_sensors:
                await self.hass.async_add_executor_job(sensor.update_from_file)
                await sensor.async_update()

        # watchdog callbacks run in a separate thread; schedule safely on HA loop
        self.hass.loop.call_soon_threadsafe(lambda: self.hass.async_create_task(_refresh()))

    def on_modified(self, event):
        self._handle(event.src_path)

    def on_created(self, event):
        self._handle(event.src_path)

    def on_moved(self, event):
        self._handle(getattr(event, "dest_path", event.src_path))


class PstecHcumHub:
    """HCUM Modbus RTU frame over TCP bridge (raw RTU bytes in TCP stream)."""

    def __init__(self, hass, entry):
        self.hass = hass
        self._entry = entry
        self._name = entry.data["name"].lower().replace(" ", "_")
        self._host = entry.data["host"]
        self._port = int(entry.data.get("port", 8899))
        self._timeout = 3.0  # hard-coded
        self._retries = 1  # hard-coded
        self._wait_ms = 1000  # hard-coded

        self._buffer = bytearray()
        self._sensors: List[HcumSensor] = []

        self._update_lock = asyncio.Lock()
        self._conn_lock = asyncio.Lock()
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

        _LOGGER.debug("pstec_hcum loaded: host=%s port=%s", self._host, self._port)

    def get_sensors(self):
        sensor_types = [
            ("rec_dev_record", "kWh", "total_increasing", "energy"),
            ("act_dev_record", "kWh", "total_increasing", "energy"),
            ("act_dev_power", "W", "measurement", "power"),
            ("dev_voltage", "V", "measurement", "voltage"),
            ("dev_current", "A", "measurement", "current"),
            ("dev_factor", "PF", "measurement", "power_factor"),
        ]
        self._sensors = [HcumSensor(self._name, *s) for s in sensor_types]
        return self._sensors

    def _build_request(self) -> bytes:
        # 01 03 00 00 00 14 + CRC(lo,hi)
        frame = bytearray([1, 3, 0, 0, 0, 20])
        crc = _crc16_modbus(frame)
        frame += crc.to_bytes(2, "little")
        return bytes(frame)

    async def _ensure_connected(self):
        async with self._conn_lock:
            if self._writer is not None and not self._writer.is_closing():
                return
            await self._close_connection()

            _LOGGER.debug("HCUM connecting to %s:%s", self._host, self._port)
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port),
                timeout=self._timeout,
            )
            self._buffer = bytearray()

    async def _close_connection(self):
        if self._writer is not None:
            try:
                self._writer.close()
                await asyncio.wait_for(self._writer.wait_closed(), timeout=2.0)
            except Exception:
                pass
        self._reader = None
        self._writer = None

    async def _read_frame(self) -> bytes:
        """Assemble one Modbus RTU response frame (01 03 28 ... CRC)."""
        assert self._reader is not None
        deadline = asyncio.get_running_loop().time() + self._timeout
        frame_len = 3 + 40 + 2  # 45

        while asyncio.get_running_loop().time() < deadline:
            # parse buffer first
            if len(self._buffer) >= frame_len:
                max_i = len(self._buffer) - frame_len
                for i in range(max_i + 1):
                    if self._buffer[i] != 0x01 or self._buffer[i+1] != 0x03:
                        continue
                    if self._buffer[i+2] != 40:
                        continue
                    frame = bytes(self._buffer[i:i+frame_len])
                    if int.from_bytes(frame[-2:], "little") == _crc16_modbus(frame[:-2]):
                        del self._buffer[:i+frame_len]
                        return frame
                # resync: keep tail
                if len(self._buffer) > 2048:
                    self._buffer = self._buffer[-(frame_len-1):]

            try:
                chunk = await asyncio.wait_for(self._reader.read(256), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if chunk == b"":
                # peer closed; no data will come
                raise ConnectionError("EOF from TCP peer")

            self._buffer += chunk

        _LOGGER.debug("HCUM assemble timeout: buffered_len=%s head=%s",
                      len(self._buffer), self._buffer[:120].hex(" "))
        raise TimeoutError("HCUM frame assemble timeout")

    async def async_update(self):
        # Prevent overlapping polls completely
        if self._update_lock.locked():
            _LOGGER.debug("HCUM poll skipped: previous poll still running")
            return

        async with self._update_lock:
            req = self._build_request()
            last_err: Optional[Exception] = None

            for attempt in range(1, self._retries + 2):
                _LOGGER.debug("HCUM poll attempt %s/%s", attempt, self._retries + 1)
                try:
                    await self._ensure_connected()
                    assert self._writer is not None

                    # send
                    _LOGGER.debug("HCUM TX: %s", req.hex(" "))
                    self._writer.write(req)
                    await asyncio.wait_for(self._writer.drain(), timeout=self._timeout)

                    if self._wait_ms:
                        await asyncio.sleep(self._wait_ms / 1000.0)

                    frame = await self._read_frame()

                    payload = frame[3:43]
                    regs = [int.from_bytes(payload[i:i+2], "big") for i in range(0, 40, 2)]

                    wh = (regs[0] << 16) | regs[1]
                    power_w = (regs[16] << 16) | regs[17]
                    voltage = float(struct.unpack(">f", payload[8:12])[0])
                    current = float(struct.unpack(">f", payload[20:24])[0])

                    kwh = round(wh / 1000.0, 1)
                    denom = voltage * current
                    pf = round(max(-1.0, min(1.0, (power_w / denom) if denom else 0.0)), 3)

                    values = {
                        f"{self._name}_rec_dev_record": kwh,
                        f"{self._name}_act_dev_record": kwh,
                        f"{self._name}_act_dev_power": int(power_w),
                        f"{self._name}_dev_voltage": round(voltage, 1),
                        f"{self._name}_dev_current": round(current, 3),
                        f"{self._name}_dev_factor": pf,
                    }
                    _LOGGER.debug("HCUM parsed: %s", values)

                    for s in self._sensors:
                        key = f"{self._name}_{s.sensor_type}"
                        if key in values:
                            s.set_state(values[key])
                    return

                except Exception as e:
                    last_err = e
                    _LOGGER.warning("HCUM update 실패 (%s/%s): %s", attempt, self._retries + 1, e)
                    # reset connection and buffer on any error
                    await self._close_connection()
                    self._buffer = bytearray()
                    await asyncio.sleep(min(0.3 * attempt, 1.5))

            _LOGGER.error("HCUM 최종 실패: %s", last_err)


class HcumSensor(SensorEntity):
    def __init__(self, prefix: str, sensor_type: str, unit, state_class: str, device_class):
        self.sensor_type = sensor_type
        self._name = f"{prefix}_{sensor_type}"
        self._state = None
        self._unit = unit
        self._state_class = state_class
        self._device_class = device_class

    @property
    def name(self):
        return self._name

    @property
    def unique_id(self):
        return self._name

    @property
    def state(self):
        return self._state

    @property
    def unit_of_measurement(self):
        return self._unit

    @property
    def state_class(self):
        return self._state_class

    @property
    def device_class(self):
        return self._device_class

    def set_state(self, value):
        self._state = value
        if getattr(self, "hass", None) is None:
            return
        self.async_write_ha_state()


class PstecUsageSensor(SensorEntity):
    def __init__(self, hass, entry, usage_type: str, record_type: str):
        self.hass = hass
        self._device = entry.data["name"].lower().replace(" ", "_")
        self._usage_type = usage_type
        self._record_type = record_type
        self._meter_day = int(entry.data.get("meter_reading_day", 25))
        self._file = hass.config.path("em", f"{self._device}_tday_energy.json")
        self._baseline = None
        self._state = None

        if usage_type == "lmon_record":
            self._name = f"{self._device}_{record_type}_{usage_type}"
        else:
            self._name = f"{self._device}_{record_type}_{usage_type}_total"

    @property
    def name(self):
        return self._name

    @property
    def unique_id(self):
        return self._name

    @property
    def state(self):
        return self._state

    @property
    def unit_of_measurement(self):
        return "kWh"

    def update_from_file(self):
        records: List[Dict[str, Any]] = []
        if os.path.exists(self._file):
            try:
                with open(self._file, "r", encoding="utf-8") as f:
                    records = json.load(f)
                if not isinstance(records, list):
                    records = []
            except Exception:
                records = []

        def to_float(x, default=0.0):
            try:
                return float(x)
            except Exception:
                return default

        now = datetime.datetime.now()

        if self._usage_type == "tmon":
            meter_records = [r for r in records if r.get("date")]
            meter_records = [
                r for r in meter_records
                if datetime.datetime.strptime(r["date"], "%Y-%m-%d").day == self._meter_day
            ]
            if meter_records:
                latest = max(meter_records, key=lambda r: r["date"])
                rec = to_float(latest.get("rec_dev_record", 0))
                ret = to_float(latest.get("ret_dev_record", 0))
                self._baseline = (rec - ret) if self._record_type == "act" else (rec if self._record_type == "rec" else ret)
            else:
                self._baseline = 0.0
            return

        if self._usage_type in ("tday", "lday"):
            target = now if self._usage_type == "tday" else (now - datetime.timedelta(days=1))
            target_date = target.strftime("%Y-%m-%d")
            rec = next((r for r in records if r.get("date") == target_date), None)

            # If baseline record is missing or zero, fall back to latest non-zero record
            # at or before the target date.
            if rec is None or to_float(rec.get("rec_dev_record", 0), 0.0) <= 0.0:
                cand = []
                try:
                    td = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()
                except Exception:
                    td = None
                for r in records:
                    d = r.get("date")
                    if not d or td is None:
                        continue
                    try:
                        dd = datetime.datetime.strptime(d, "%Y-%m-%d").date()
                    except Exception:
                        continue
                    if dd <= td and to_float(r.get("rec_dev_record", 0), 0.0) > 0.0:
                        cand.append(r)
                rec = max(cand, key=lambda r: r["date"]) if cand else None

            if rec is None:
                self._baseline = 0.0
            else:
                rec_v = to_float(rec.get("rec_dev_record", 0))
                ret_v = to_float(rec.get("ret_dev_record", 0))
                if self._record_type == "act":
                    self._baseline = rec_v - ret_v
                elif self._record_type == "rec":
                    self._baseline = rec_v
                else:
                    self._baseline = ret_v
            return

        if self._usage_type in ("lmon", "lmon_record"):
            meter_records = [r for r in records if r.get("date")]
            meter_records = [
                r for r in meter_records
                if datetime.datetime.strptime(r["date"], "%Y-%m-%d").day == self._meter_day
            ]
            if not meter_records:
                self._baseline = 0.0
                self._state = 0.0
                return

            # Sort ascending by date so we can pick latest and previous meter readings.
            meter_records.sort(key=lambda r: r["date"])
            latest = meter_records[-1]
            prev = meter_records[-2] if len(meter_records) >= 2 else None

            def _value(rec: Dict[str, Any]) -> float:
                rec_v = to_float(rec.get("rec_dev_record", 0))
                ret_v = to_float(rec.get("ret_dev_record", 0))
                if self._record_type == "act":
                    return rec_v - ret_v
                if self._record_type == "rec":
                    return rec_v
                return ret_v

            latest_v = _value(latest)
            self._baseline = latest_v  # keep for reference

            if self._usage_type == "lmon_record":
                # "지난 검침일 때의 수치" (cumulative at last meter reading)
                self._state = round(latest_v, 3)
                return

            # self._usage_type == "lmon": "지난 검침일 - 그 이전 검침일" (usage of last metering cycle)
            if prev is None:
                self._state = 0.0
                return
            prev_v = _value(prev)
            self._state = round(max(0.0, latest_v - prev_v), 3)
            return

        self._baseline = 0.0

    async def async_update(self):
        def to_float_state(entity_id: str, default=0.0):
            st = self.hass.states.get(entity_id)
            if st is None:
                return default
            v = st.state
            if v in (None, "unknown", "unavailable", "N/A"):
                return default
            try:
                return float(v)
            except Exception:
                return default

        # lmon_record: last meter reading cumulative
        # lmon: last metering cycle usage (difference between last and previous meter readings)
        if self._usage_type in ("lmon", "lmon_record"):
            if self._baseline is None or self._state is None:
                self.update_from_file()
            # update_from_file already computed the correct state for these types.
            return

        rec_now = to_float_state(f"sensor.{self._device}_rec_dev_record", 0.0)
        ret_now = 0.0
        act_now = to_float_state(f"sensor.{self._device}_act_dev_record", rec_now - ret_now)

        if self._baseline is None:
            self.update_from_file()
        baseline = float(self._baseline or 0.0)

        if self._record_type == "rec":
            self._state = round(max(0.0, rec_now - baseline), 3)
        elif self._record_type == "ret":
            self._state = 0.0
        elif self._record_type == "act":
            self._state = round(max(0.0, act_now - baseline), 3)
        elif self._record_type == "fct":
            # Forecast usage for the *metering cycle* (from last meter reading day to next meter reading day).
            # If we're very close to the meter reading moment (<= 18 hours since cycle start),
            # show last month's actual usage instead of a noisy projection.
            act_used = to_float_state(f"sensor.{self._device}_act_tmon_total", 0.0)
            lmon_act = to_float_state(f"sensor.{self._device}_act_lmon_total", 0.0)

            now = datetime.datetime.now()

            # Determine last/next meter reading datetime (00:00 at the meter day).
            meter_day = int(self._meter_day)

            def _safe_day(y: int, m: int, d: int) -> int:
                dim = calendar.monthrange(y, m)[1]
                return min(max(1, d), dim)

            if now.day >= meter_day:
                last_y, last_m = now.year, now.month
            else:
                # previous month
                if now.month == 1:
                    last_y, last_m = now.year - 1, 12
                else:
                    last_y, last_m = now.year, now.month - 1

            last_dt = datetime.datetime(last_y, last_m, _safe_day(last_y, last_m, meter_day), 0, 0, 0)

            # next meter reading is one month after last_dt on meter_day
            if last_m == 12:
                next_y, next_m = last_y + 1, 1
            else:
                next_y, next_m = last_y, last_m + 1
            next_dt = datetime.datetime(next_y, next_m, _safe_day(next_y, next_m, meter_day), 0, 0, 0)

            elapsed_hours = (now - last_dt).total_seconds() / 3600.0
            total_days = max(1.0, (next_dt - last_dt).total_seconds() / 86400.0)
            elapsed_days = max(0.001, (now - last_dt).total_seconds() / 86400.0)

            if elapsed_hours <= 18.0:
                # Right after meter reading: report last month's actual usage.
                self._state = round(lmon_act, 1)
            else:
                # Project current-cycle usage to full cycle length.
                self._state = round((act_used / elapsed_days) * total_days, 1)
        else:
            self._state = 0.0

        self.async_write_ha_state()


async def async_setup_entry(hass, entry, async_add_entities):
    hub = PstecHcumHub(hass, entry)
    core_sensors = hub.get_sensors()
    async_add_entities(core_sensors)

    ret_stub = HcumSensor(entry.data["name"].lower().replace(" ", "_"), "ret_dev_record", "kWh", "total_increasing", "energy")
    async_add_entities([ret_stub])
    ret_stub.set_state(0.0)

    usage_sensors = [
        PstecUsageSensor(hass, entry, "tday", "act"),
        PstecUsageSensor(hass, entry, "tday", "rec"),
        PstecUsageSensor(hass, entry, "lday", "act"),
        PstecUsageSensor(hass, entry, "tmon", "act"),
        PstecUsageSensor(hass, entry, "tmon", "rec"),
        PstecUsageSensor(hass, entry, "tmon", "fct"),
        PstecUsageSensor(hass, entry, "lmon", "act"),
        PstecUsageSensor(hass, entry, "lmon_record", "rec"),
        PstecUsageSensor(hass, entry, "lmon_record", "act"),
    ]
    async_add_entities(usage_sensors)

    # Initialize usage sensors once at startup (avoid 'unknown')
    for us in usage_sensors:
        await hass.async_add_executor_job(us.update_from_file)
        await us.async_update()

    # Watch file updates
    file_dir = os.path.dirname(usage_sensors[0]._file)
    os.makedirs(file_dir, exist_ok=True)
    event_handler = JSONFileEventHandler(usage_sensors, hass)
    observer = Observer()
    observer.schedule(event_handler, file_dir, recursive=False)
    observer.start()

    def stop_observer(event):
        observer.stop()
        observer.join()

    hass.bus.async_listen_once("homeassistant_stop", stop_observer)

    # first poll
    hass.async_create_task(hub.async_update())

    async def _fix_today_zero_record():
        await asyncio.sleep(3)
        device = entry.data["name"].lower().replace(" ", "_")
        tday_file = hass.config.path("em", f"{device}_tday_energy.json")
        if not os.path.exists(tday_file):
            return
        try:
            records = await hass.async_add_executor_job(_sync_read_json, tday_file)
            if not isinstance(records, list):
                return
        except Exception:
            return
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        cur = hass.states.get(f"sensor.{device}_rec_dev_record")
        try:
            cur_val = float(cur.state) if cur and cur.state not in ("unknown","unavailable","N/A") else None
        except Exception:
            cur_val = None
        if cur_val is None or cur_val <= 0:
            return
        changed = False
        for r in records:
            if r.get("date") == today and float(r.get("rec_dev_record", 0) or 0) <= 0:
                r["rec_dev_record"] = cur_val
                r["ret_dev_record"] = 0.0
                changed = True
        if changed:
            await hass.async_add_executor_job(lambda: _sync_write_json_atomic(tday_file, records, indent=4))
            for us in usage_sensors:
                await hass.async_add_executor_job(us.update_from_file)
                await us.async_update()
            _LOGGER.warning("HCUM 일일기록 보정: 오늘(%s) baseline 0.0 -> %.3f", today, cur_val)

    hass.async_create_task(_fix_today_zero_record())

    scan_interval = datetime.timedelta(seconds=int(entry.data.get("scan_interval", 20)))

    async def update(_):
        await hub.async_update()
        ret_stub.set_state(0.0)
        # update derived usage sensors every poll tick
        for us in usage_sensors:
            await us.async_update()

    async_track_time_interval(hass, update, scan_interval)

    async def file_saving_callback(now: datetime.datetime):
        try:
            device = entry.data["name"].lower().replace(" ", "_")
            tday_file = hass.config.path("em", f"{device}_tday_energy.json")
            os.makedirs(os.path.dirname(tday_file), exist_ok=True)

            rec_now = hass.states.get(f"sensor.{device}_rec_dev_record")
            rec_val = None
            if rec_now and rec_now.state not in (None, "unknown", "unavailable", "N/A"):
                try:
                    rec_val = float(rec_now.state)
                except Exception:
                    rec_val = None

            today_str = now.strftime("%Y-%m-%d")
            if rec_val is None or rec_val <= 0.0:
                _LOGGER.warning("HCUM 일일기록 저장 스킵: rec_dev_record 값이 유효하지 않음 (state=%s)", getattr(rec_now, "state", None))
                async def _retry():
                    await asyncio.sleep(300)
                    await file_saving_callback(datetime.datetime.now())
                hass.async_create_task(_retry())
                return

            daily_record = {"date": today_str, "rec_dev_record": rec_val, "ret_dev_record": 0.0}

            records = []
            if os.path.exists(tday_file):
                try:
                    records = await hass.async_add_executor_job(_sync_read_json, tday_file)
                    if not isinstance(records, list):
                        records = []
                except Exception:
                    records = []

            updated = False
            for r in records:
                if r.get("date") == today_str:
                    r.update(daily_record)
                    updated = True
                    break
            if not updated:
                records.append(daily_record)

            cutoff = (now - datetime.timedelta(days=90)).date()
            records = [r for r in records if datetime.datetime.strptime(r["date"], "%Y-%m-%d").date() >= cutoff]

            await hass.async_add_executor_job(lambda: _sync_write_json_atomic(tday_file, records, indent=4))

            for us in usage_sensors:
                await hass.async_add_executor_job(us.update_from_file)
                await us.async_update()

        except Exception as e:
            _LOGGER.error("file_saving_callback 오류: %s", e)

    async_track_time_change(hass, file_saving_callback, hour=0, minute=0, second=0)
