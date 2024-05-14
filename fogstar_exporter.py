import asyncio
import logging
import click
from bleak import BleakClient, BleakGATTCharacteristic
from bleak.exc import BleakError
from prometheus_client import start_http_server, Gauge, Counter

last_check = Gauge("fogstar_last_data", "Timestamp of last datapoint", ["address"])
voltage = Gauge("fogstar_voltage", "Voltage", ["address"])
current = Gauge("fogstar_current", "Current", ["address"])
capacity = Gauge("fogstar_capacity_ah", "Capacity in Ah", ["address", "type"])
temperature = Gauge("fogstar_temperature_c", "Temperature", ["address", "id"])
cycles = Gauge("fogstar_cycles", "Battery cycles", ["address"])

errors = Counter("fogstar_errors", "BLE Fetch errors")

last_fetch = Gauge("fogstar_last_fetch", "Last fetch")

INFORMATION_SERVICE = "0000ff00-0000-1000-8000-00805f9b34fb"
READ_CHARACTERISTIC = "0000ff01-0000-1000-8000-00805f9b34fb"
WRITE_CHARACTERISTIC = "0000ff02-0000-1000-8000-00805f9b34fb"


log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

response = bytearray()


def convert(
    data: bytearray,
    signed=True,
    min_val=None,
    max_val=None,
    scale=1,
    offset=0,
):
    result = (int.from_bytes(data, byteorder="big", signed=signed) + offset) * scale
    if result < min_val:
        raise ValueError(f"Value {result} < {min_val}")
    if result > max_val:
        raise ValueError(f"Value {result} > {max_val}")
    return result


class FogstarExporter:

    def __init__(self, address):
        self.address = address

    def handle_response(self, data: bytearray):
        data = data[4:]
        voltage.labels(self.address).set(
            convert(data[0:2], min_val=0, max_val=20, scale=0.01)
        )
        current.labels(self.address).set(
            convert(data[2:4], min_val=-200, max_val=200, scale=0.01)
        )
        capacity.labels(self.address, "remaining").set(
            convert(data[4:6], min_val=0, max_val=200, scale=0.01)
        )
        capacity.labels(self.address, "full").set(
            convert(data[6:8], min_val=0, max_val=200, scale=0.01)
        )
        cycles.labels(self.address).set(convert(data[8:10], min_val=0, max_val=100000))

        for i in range(int.from_bytes(data[22:23], "big")):
            temperature.labels(self.address, str(i)).set(
                convert(
                    data[23 + i * 2 : i * 2 + 25],
                    min_val=-30,
                    max_val=100,
                    signed=False,
                    scale=0.1,
                    offset=-2731,
                )
            )

        last_fetch.set_to_current_time()

    def notification_handler(
        self, characteristic: BleakGATTCharacteristic, data: bytearray
    ):
        global response
        response += data
        if response.endswith(b"w"):
            try:
                self.handle_response(response)
            except Exception:
                log.exception("Error parsing response: %r", response)
            response = bytearray()

    async def poll(self, address):
        async with BleakClient(address) as client:
            log.info("Connected")
            await client.start_notify(READ_CHARACTERISTIC, self.notification_handler)
            await asyncio.sleep(3)
            while True:
                await client.write_gatt_char(
                    WRITE_CHARACTERISTIC,
                    bytes([0xDD, 0xA5, 0x03, 0x00, 0xFF, 0xFD, 0x77]),
                )
                await asyncio.sleep(10)

    async def main(self, address):
        backoff = 1
        while True:
            try:
                await self.poll(address)
                backoff = 1
            except BleakError as e:
                errors.inc()
                log.warning(f"Bleak error {e}, retrying in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)


@click.command()
@click.option("--address", help="BLE address of battery")
def run(address):
    start_http_server(9103)
    exporter = FogstarExporter(address)
    asyncio.run(exporter.main(address))


if __name__ == "__main__":
    run()
