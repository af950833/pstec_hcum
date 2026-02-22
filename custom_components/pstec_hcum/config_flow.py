import asyncio
import logging
import voluptuous as vol

from homeassistant import config_entries

from . import DOMAIN

_LOGGER = logging.getLogger(__name__)

# Hard-coded comm params (kept in code as requested)
DEFAULT_PORT = 8899


class PstecHcumConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    async def async_step_user(self, user_input=None):
        errors = {}
        if user_input is not None:
            host = user_input["host"]
            port = int(user_input.get("port", DEFAULT_PORT))

            # connectivity test (quick)
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=3.0,
                )
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                _LOGGER.error("HCUM 연결 실패: %s", e)
                errors["base"] = "cannot_connect"
            else:
                # Name input is allowed to be blank; generate a stable internal name from host/port.
                raw_name = (user_input.get("name") or "").strip()
                internal_name = raw_name if raw_name else f"hcum_{host.replace('.', '_')}_{port}"
                title = raw_name if raw_name else f"HCUM ({host}:{port})"

                data = {
                    "name": internal_name,
                    "host": host,
                    "port": port,
                    "meter_reading_day": int(user_input.get("meter_reading_day", 25)),
                    "scan_interval": int(user_input.get("scan_interval", 10)),
                }
                return self.async_create_entry(title=title, data=data)

        data_schema = vol.Schema(
            {
                # Like the original pstec style: allow blank name field
                vol.Optional("name", default=""): str,
                vol.Required("host"): str,
                vol.Required("port", default=DEFAULT_PORT): int,
                # plain integer inputs (no slider selectors)
                vol.Required("scan_interval", default=10): int,
                vol.Required("meter_reading_day", default=25): int,
            }
        )
        return self.async_show_form(step_id="user", data_schema=data_schema, errors=errors)
