from flask import Flask, request, jsonify
from datetime import datetime, timezone
import paho.mqtt.publish as publish
import threading
import random
import base64
import time
import json

# HTTP Server Configuration
HTTP_SERVER_PORT = 9010
DATA_SENDING_INTERVAL = 60  # seconds

LORAWAN_FPORT = 85

MQTT_SERVER = "localhost"
MQTT_PORT = 1883

# Define apartments & device ids (extend this dict to simulate more)
APARTMENTS = {
    "studio_apartment": {
        "aqi_device_id": "aqi_studio_01",
        "switch_device_id": "switch_studio_01",
        "socket_device_id": "socket_studio_01",
        "curtain_device_id": "curtain_studio_01",
        "peoplecounter_device_id": "vs350_studio_01",
        "doorlock_device_id": "doorlock_studio_01",
        "thermostat_device_id": "thermo_studio_01",
        "watermeter_device_id": "water_studio_01",
        "gasmeter_device_id": "gas_studio_01",
        "scb_device_id": "scb_studio_01",
        "rooms": ["kitchen", "bathroom", "main"],
    },
    "1_bedroom": {
        "aqi_device_id": "aqi_1bed_01",
        "switch_device_id": "switch_1bed_01",
        "socket_device_id": "socket_1bed_01",
        "curtain_device_id": "curtain_1bed_01",
        "peoplecounter_device_id": "vs350_1bed_01",
        "doorlock_device_id": "doorlock_1bed_01",
        "thermostat_device_id": "thermo_1bed_01",
        "watermeter_device_id": "water_1bed_01",
        "gasmeter_device_id": "gas_1bed_01",
        "scb_device_id": "scb_1bed_01",
        "rooms": ["powder_room", "bedroom", "living_room", "dressing_room"],
    }
}

latest_data_lock = threading.Lock()

latest_data = {}
for apt, cfg in APARTMENTS.items():
    wallswitch_by_room = {}
    for room in cfg["rooms"]:
        wallswitch_by_room[room] = {
            "current": random.randint(200, 300),
            "voltage": round(random.uniform(230, 250), 1),
            "active_power": random.randint(0, 100),
            "power_consumption": random.randint(90000, 100000),
            "power_factor": random.randint(50, 100),
            "switch_1": 1,
            "switch_2": 0,
        }
    latest_data[apt] = {
        "aqi": {
            "temp": round(random.uniform(20, 25), 1),
            "humd": random.randint(40, 60),
            "co2": random.randint(350, 800),
            "battery": random.randint(50, 100)
        },
        "wallsocket": {
            "voltage": round(random.uniform(230, 250), 1),
            "current": random.randint(200, 300),
            "active_power": random.randint(0, 100),
            "power_consumption": random.randint(90000, 100000),
            "power_factor": random.randint(50, 100),
            "socket_status": 1
        },
        "curtain": {
            "battery": random.randint(50, 100),
            "curtainstate": random.randint(0, 100)
        },
        "peoplecounter": {
            "total_in": random.randint(0, 50),
            "total_out": random.randint(0, 50),
            "period_in": random.randint(0, 30),
            "period_out": random.randint(0, 30),
            "battery": random.randint(50, 100),
            "temperature": round(random.uniform(20.0, 30.0), 1)
        },
        "scb": {
            "device_type": "SCB-100",
            "breaker_address": 1,
            "breaker_type": "3P4P",
            # switch_state: 1 = CLOSED, 0 = OPEN (integers)
            "switch_state": 1,
            "remote_control_enabled": True,
            "voltage_A": round(random.uniform(220.0, 240.0), 1),
            "voltage_B": round(random.uniform(220.0, 240.0), 1),
            "voltage_C": round(random.uniform(220.0, 240.0), 1),
            "current_A": round(random.uniform(0.0, 20.0), 2),
            "current_B": round(random.uniform(0.0, 20.0), 2),
            "current_C": round(random.uniform(0.0, 20.0), 2),
            "current_N": round(random.uniform(0.0, 5.0), 2),
            "power_A": round(random.uniform(0.0, 2000.0), 1),
            "power_B": round(random.uniform(0.0, 2000.0), 1),
            "power_C": round(random.uniform(0.0, 2000.0), 1),
            "power_total": round(random.uniform(0.0, 6000.0), 1),
            "power_factor_A": round(random.uniform(0.8, 1.0), 2),
            "power_factor_B": round(random.uniform(0.8, 1.0), 2),
            "power_factor_C": round(random.uniform(0.8, 1.0), 2),
            "leakage_current": 0,
            "temperature_device": random.randint(20, 40),
            "temperature_terminal_A": random.randint(20, 40),
            "temperature_terminal_B": random.randint(20, 40),
            "temperature_terminal_C": random.randint(20, 40),
            "temperature_terminal_N": random.randint(20, 40),
            "alarm_short_circuit": False,
            "alarm_over_current": False,
            "alarm_over_voltage": False,
            "alarm_under_voltage": False,
            "alarm_leakage": False,
            "alarm_overload": False,
            "alarm_temperature": False,
            "alarm_fire": False,
            "alarm_surge": False,
            "alarm_phase_loss": False
        },
        "watermeter": {
            "device_type": "WATER_METER",
            "id": f"{apt}_water",
            "volume": round(random.uniform(0.0, 200.0), 2),
            # valve_state: 1 = CLOSED (connection made), 0 = OPEN (cut off) (integers)
            "valve_state": 0,
            "battery": random.randint(20, 100),
            "low_power": False,
            "alarm": False,
            "communication_error": False
        },
        "gasmeter": {
            "device_type": "GAS_METER",
            "id": f"{apt}_gas",
            "volume": round(random.uniform(0.0, 500.0), 2),
            # valve_state: 1 = CLOSED (connection made), 0 = OPEN (cut off) (integers)
            "valve_state": 0,
            "battery": random.randint(20, 100),
            "low_power": False,
            "alarm": False
        },
        "doorlock": {
            "id": f"{apt}_door",
            "battery": random.randint(30, 100),
            "t": datetime.now(timezone.utc).isoformat(),
            "remote_lock": 0,
            "unlock_record": 0,
            "alarm": 0,
            "auto_relock": 0,
            "auto_relock_enabled": True,
            "current_status": 1,
            "normally_open_mode": 0,
            "tamper": 0,
            "reporting_time": 3600,
            "last_access_method": "",
            "last_access_user_id": 0,
            "last_access_timestamp": "",
            "last_manage_action": "",
            "last_manage_user_id": 0
        },
        "thermostat": {
            "temperature": round(random.uniform(20.0, 24.0), 1),
            "humidity": random.randint(30, 50),
            "setpoint_temperature": round(random.uniform(22.0, 26.0), 1),
            "mode": "cool",
            "status": "home",
            "fan_setting": "auto",
            # valve_status: 1 = connection made/open, 0 = closed (integer)
            "valve_status": 0,
            "fan_status": "auto",
            "co2": random.randint(350, 800),
            "power": "on",
            # track last setpoint updates
            "last_setpoint_timestamp": datetime.now(timezone.utc).isoformat()
        },
        "wallswitch": wallswitch_by_room
    }
    # compute initial people count (total_in - total_out) and clamp to >= 0
    latest_data[apt]["peoplecounter"]["count"] = max(0, latest_data[apt]["peoplecounter"]["total_in"] - latest_data[apt]["peoplecounter"]["total_out"])

def publish_aqi(apt_name):
    device_id = APARTMENTS[apt_name]["aqi_device_id"]
    with latest_data_lock:
        aqi = latest_data[apt_name]["aqi"].copy()
    payload = {
        "id": device_id,
        "gid": device_id,
        "temperature": aqi["temp"],
        "humidity": aqi["humd"],
        "co2": aqi["co2"],
        "battery": aqi["battery"],
        "sensor_name": "AQI"
    }
    topic = f"sim/{device_id}/uplink"
    try:
        publish.single(topic, json.dumps(payload), hostname=MQTT_SERVER, port=MQTT_PORT)
    except Exception as e:
        print(f"[MQTT publish error] {e}")

def publish_wallswitch(apt_name, room):
    base_device = APARTMENTS[apt_name]["switch_device_id"]
    device_id = f"{base_device}_{room}"
    with latest_data_lock:
        ws = latest_data[apt_name]["wallswitch"][room].copy()
    payload = {
        "id": device_id,
        "gid": device_id,
        "current": ws["current"],
        "voltage": ws["voltage"],
        "active_power": ws["active_power"],
        "power_consumption": ws["power_consumption"],
        "power_factor": ws["power_factor"],
        "switch_1": ws["switch_1"],
        "switch_2": ws["switch_2"],
        "sensor_name": "Switch"
    }
    topic = f"sim/{device_id}/uplink"
    try:
        publish.single(topic, json.dumps(payload), hostname=MQTT_SERVER, port=MQTT_PORT)
    except Exception as e:
        print(f"[MQTT publish error] {e}")

def publish_wallsocket(apt_name):
    device_id = APARTMENTS[apt_name]["socket_device_id"]
    with latest_data_lock:
        wallsocket = latest_data[apt_name]["wallsocket"].copy()
    payload = {
        "id": device_id,
        "gid": device_id,
        "current": wallsocket["current"],
        "voltage": wallsocket["voltage"],
        "active_power": wallsocket["active_power"],
        "power_consumption": wallsocket["power_consumption"],
        "power_factor": wallsocket["power_factor"],
        "socket_status": wallsocket["socket_status"],
        "sensor_name": "Socket"
    }
    topic = f"sim/{device_id}/uplink"
    try:
        publish.single(topic, json.dumps(payload), hostname=MQTT_SERVER, port=MQTT_PORT)
    except Exception as e:
        print(f"[MQTT publish error] {e}")

def publish_curtain(apt_name):
    device_id = APARTMENTS[apt_name]["curtain_device_id"]
    with latest_data_lock:
        curtain = latest_data[apt_name]["curtain"].copy()
    payload = {
        "id": device_id,
        "gid": device_id,
        "battery": curtain["battery"],
        "curtainstate": curtain["curtainstate"],
        "sensor_name": "CurtainController"
    }
    topic = f"sim/{device_id}/uplink"
    try:
        publish.single(topic, json.dumps(payload), hostname=MQTT_SERVER, port=MQTT_PORT)
    except Exception as e:
        print(f"[MQTT publish error] {e}")

def publish_peoplecounter(apt_name):
    device_id = APARTMENTS[apt_name].get("peoplecounter_device_id")
    if not device_id:
        return
    with latest_data_lock:
        pc = latest_data[apt_name]["peoplecounter"].copy()
    payload = {
        "total_in": pc["total_in"],
        "total_out": pc["total_out"],
        "period_in": pc["period_in"],
        "period_out": pc["period_out"],
        "battery": pc["battery"],
        "temperature": pc["temperature"]
    }
    topic = f"sim/{device_id}/uplink"
    try:
        publish.single(topic, json.dumps(payload), hostname=MQTT_SERVER, port=MQTT_PORT)
    except Exception as e:
        print(f"[MQTT publish error] {e}")

def publish_doorlock(apt_name):
    device_id = APARTMENTS[apt_name].get("doorlock_device_id")
    if not device_id:
        return
    with latest_data_lock:
        dl = latest_data[apt_name]["doorlock"].copy()
    payload = {
        "id": dl.get("id"),
        "t": dl.get("t"),
        "battery": dl.get("battery"),
        "remote_lock": dl.get("remote_lock"),
        "unlock_record": dl.get("unlock_record"),
        "alarm": dl.get("alarm"),
        "auto_relock": dl.get("auto_relock"),
        "normally_open_mode": dl.get("normally_open_mode"),
        "tamper": dl.get("tamper"),
        "reporting_time": dl.get("reporting_time"),
        "last_access_method": dl.get("last_access_method"),
        "last_access_user_id": dl.get("last_access_user_id"),
        "last_access_timestamp": dl.get("last_access_timestamp"),
        "last_manage_action": dl.get("last_manage_action"),
        "last_manage_user_id": dl.get("last_manage_user_id")
    }
    topic = f"sim/{device_id}/uplink"
    try:
        publish.single(topic, json.dumps(payload), hostname=MQTT_SERVER, port=MQTT_PORT)
    except Exception as e:
        print(f"[MQTT publish error] {e}")

def publish_scb(apt_name):
    device_id = APARTMENTS[apt_name].get("scb_device_id")
    if not device_id:
        return
    with latest_data_lock:
        scb = latest_data[apt_name]["scb"].copy()
    payload = scb
    topic = f"sim/{device_id}/uplink"
    try:
        publish.single(topic, json.dumps(payload), hostname=MQTT_SERVER, port=MQTT_PORT)
    except Exception as e:
        print(f"[MQTT publish error] {e}")

def publish_watermeter(apt_name):
    device_id = APARTMENTS[apt_name].get("watermeter_device_id")
    if not device_id:
        return
    with latest_data_lock:
        wm = latest_data[apt_name]["watermeter"].copy()
    payload = wm
    topic = f"sim/{device_id}/uplink"
    try:
        publish.single(topic, json.dumps(payload), hostname=MQTT_SERVER, port=MQTT_PORT)
    except Exception as e:
        print(f"[MQTT publish error] {e}")

def publish_gasmeter(apt_name):
    device_id = APARTMENTS[apt_name].get("gasmeter_device_id")
    if not device_id:
        return
    with latest_data_lock:
        gm = latest_data[apt_name]["gasmeter"].copy()
    payload = gm
    topic = f"sim/{device_id}/uplink"
    try:
        publish.single(topic, json.dumps(payload), hostname=MQTT_SERVER, port=MQTT_PORT)
    except Exception as e:
        print(f"[MQTT publish error] {e}")

def publish_thermostat(apt_name):
    device_id = APARTMENTS[apt_name].get("thermostat_device_id")
    if not device_id:
        return
    with latest_data_lock:
        th = latest_data[apt_name]["thermostat"].copy()
    payload = {
        "temperature": th["temperature"],
        "humidity": th["humidity"],
        "setpoint_temperature": th["setpoint_temperature"],
        "setpoint_timestamp": th.get("last_setpoint_timestamp", ""),
        "mode": th["mode"],
        "status": th["status"],
        "fan_setting": th["fan_setting"],
        "valve_status": th["valve_status"],
        "fan_status": th["fan_status"],
        "co2": th["co2"],
        "power": th["power"]
    }
    topic = f"sim/{device_id}/uplink"
    try:
        publish.single(topic, json.dumps(payload), hostname=MQTT_SERVER, port=MQTT_PORT)
    except Exception as e:
        print(f"[MQTT publish error] {e}")

def aqi_updater():
    while True:
        for apt in APARTMENTS:
            with latest_data_lock:
                latest_data[apt]["aqi"]["temp"] = round(random.uniform(20, 30), 1)
                latest_data[apt]["aqi"]["humd"] = random.randint(40, 80)
                latest_data[apt]["aqi"]["co2"] = random.randint(300, 1000)
                latest_data[apt]["aqi"]["battery"] = random.randint(50, 100)
            publish_aqi(apt)
        time.sleep(DATA_SENDING_INTERVAL)

def wallswitch_updater():
    while True:
        for apt, cfg in APARTMENTS.items():
            for room in cfg["rooms"]:
                with latest_data_lock:
                    latest_data[apt]["wallswitch"][room]["current"] = random.randint(200, 300)
                    latest_data[apt]["wallswitch"][room]["voltage"] = round(random.uniform(230, 250), 1)
                    latest_data[apt]["wallswitch"][room]["active_power"] = random.randint(0, 100)
                    latest_data[apt]["wallswitch"][room]["power_consumption"] = random.randint(90000, 100000)
                    latest_data[apt]["wallswitch"][room]["power_factor"] = random.randint(50, 100)
                publish_wallswitch(apt, room)
        time.sleep(DATA_SENDING_INTERVAL)

def wallsocket_updater():
    while True:
        for apt in APARTMENTS:
            with latest_data_lock:
                latest_data[apt]["wallsocket"]["current"] = random.randint(200, 300)
                latest_data[apt]["wallsocket"]["voltage"] = round(random.uniform(230, 250), 1)
                latest_data[apt]["wallsocket"]["active_power"] = random.randint(0, 100)
                latest_data[apt]["wallsocket"]["power_consumption"] = random.randint(90000, 100000)
                latest_data[apt]["wallsocket"]["power_factor"] = random.randint(50, 100)
            publish_wallsocket(apt)
        time.sleep(DATA_SENDING_INTERVAL)

def curtain_updater():
    while True:
        for apt in APARTMENTS:
            with latest_data_lock:
                latest_data[apt]["curtain"]["battery"] = random.randint(50, 100)
                # curtainstate remains as set unless changed by PUT; keep current value
            publish_curtain(apt)
        time.sleep(DATA_SENDING_INTERVAL)

def peoplecounter_updater():
    while True:
        for apt in APARTMENTS:
            with latest_data_lock:
                # increment totals a bit to simulate accumulation
                latest_data[apt]["peoplecounter"]["total_in"] += random.randint(0, 3)
                latest_data[apt]["peoplecounter"]["total_out"] += random.randint(0, 3)
                latest_data[apt]["peoplecounter"]["period_in"] = random.randint(0, 30)
                latest_data[apt]["peoplecounter"]["period_out"] = random.randint(0, 30)
                latest_data[apt]["peoplecounter"]["battery"] = max(0, latest_data[apt]["peoplecounter"]["battery"] - random.randint(0, 1))
                latest_data[apt]["peoplecounter"]["temperature"] = round(random.uniform(20.0, 30.0), 1)
                # maintain computed count field (in - out) and ensure non-negative
                latest_data[apt]["peoplecounter"]["count"] = max(0, latest_data[apt]["peoplecounter"]["total_in"] - latest_data[apt]["peoplecounter"]["total_out"])
            publish_peoplecounter(apt)
        time.sleep(DATA_SENDING_INTERVAL)

def doorlock_updater():
    while True:
        for apt in APARTMENTS:
            with latest_data_lock:
                dl = latest_data[apt]["doorlock"]
                
                # --- NEW LOGIC START ---
                # 1. Get Configuration
                # normally_open_mode: 0=Disabled, 1=Mode 1 (Permanent), 2=Mode 2 (Delayed)
                norm_open = dl.get('normally_open_mode', 0)
                
                # auto_relock: Time in seconds before door locks
                # honor explicit enable/disable flag: if auto_relock_enabled is False -> no auto-relocK
                relock_enabled = dl.get('auto_relock_enabled', True)
                relock_time = dl.get('auto_relock', 0) if relock_enabled else None
                # default short relock_time only when field is missing and enabled
                if relock_enabled and (not relock_time):
                    relock_time = 5

                # 2. Determine Status
                calculated_status = 1  # Default to 1 = Locked

                # Priority 1: Normally Open Mode forces UNLOCKED
                if norm_open > 0:
                    calculated_status = 0  # 0 = Unlocked
                else:
                    # If last action was a remote control, honor remote_lock intent.
                    last_method = dl.get('last_access_method', '')
                    last_access = dl.get('last_access_timestamp')

                    # Helper to compute elapsed seconds safely
                    def _elapsed_seconds(ts):
                        try:
                            last_ts = datetime.fromisoformat(ts)
                            return (datetime.now(timezone.utc) - last_ts).total_seconds()
                        except Exception:
                            return None

                    # Case: remote control command - explicit lock/unlock
                    if last_method == 'remote':
                        # remote_lock: 1 = remote-unlock request, 0 = remote-lock request
                        if dl.get('remote_lock') == 1:
                            # If relock is disabled, remain unlocked until explicit lock
                            if relock_time is None:
                                calculated_status = 0
                            else:
                                # if within relock window, consider unlocked; otherwise re-lock
                                elapsed = _elapsed_seconds(last_access) if last_access else None
                                if elapsed is None or elapsed < relock_time:
                                    calculated_status = 0
                                else:
                                    calculated_status = 1
                        else:
                            # explicit remote lock -> locked immediately
                            calculated_status = 1
                    else:
                        # Non-remote access (manual, card, password) uses transient unlock window
                        if last_access:
                            # if relock disabled: once unlocked by access event, remain unlocked
                            if relock_time is None:
                                calculated_status = 0
                            else:
                                elapsed = _elapsed_seconds(last_access)
                                if elapsed is not None and elapsed < relock_time:
                                    calculated_status = 0

                # 3. Store the status
                # 1 = Locked, 0 = Unlocked (Standard convention for door sensors)
                dl["current_status"] = calculated_status
                # --- NEW LOGIC END ---

                # Existing updates (timestamp, slight battery decay)
                dl["t"] = datetime.now(timezone.utc).isoformat()
                dl["battery"] = max(0, dl["battery"] - random.randint(0, 1))
                
            publish_doorlock(apt)
        time.sleep(DATA_SENDING_INTERVAL)

def scb_updater():
    while True:
        for apt in APARTMENTS:
            with latest_data_lock:
                scb = latest_data[apt]["scb"]
                # simulate small fluctuations
                scb["voltage_A"] = round(scb["voltage_A"] + random.uniform(-1.0, 1.0), 1)
                scb["voltage_B"] = round(scb["voltage_B"] + random.uniform(-1.0, 1.0), 1)
                scb["voltage_C"] = round(scb["voltage_C"] + random.uniform(-1.0, 1.0), 1)
                scb["current_A"] = round(max(0.0, scb["current_A"] + random.uniform(-0.5, 0.5)), 2)
                scb["current_B"] = round(max(0.0, scb["current_B"] + random.uniform(-0.5, 0.5)), 2)
                scb["current_C"] = round(max(0.0, scb["current_C"] + random.uniform(-0.5, 0.5)), 2)
                scb["power_A"] = round(scb["current_A"] * scb["voltage_A"], 1)
                scb["power_B"] = round(scb["current_B"] * scb["voltage_B"], 1)
                scb["power_C"] = round(scb["current_C"] * scb["voltage_C"], 1)
                scb["power_total"] = round(scb["power_A"] + scb["power_B"] + scb["power_C"], 1)
                scb["temperature_device"] = random.randint(20, 40)
            publish_scb(apt)
        time.sleep(DATA_SENDING_INTERVAL)

def watermeter_updater():
    while True:
        for apt in APARTMENTS:
            with latest_data_lock:
                latest_data[apt]["watermeter"]["volume"] = round(latest_data[apt]["watermeter"]["volume"] + random.uniform(0.0, 2.0), 2)
                # battery slowly decays
                latest_data[apt]["watermeter"]["battery"] = max(0, latest_data[apt]["watermeter"]["battery"] - random.randint(0, 1))
            publish_watermeter(apt)
        time.sleep(DATA_SENDING_INTERVAL)

def gasmeter_updater():
    while True:
        for apt in APARTMENTS:
            with latest_data_lock:
                latest_data[apt]["gasmeter"]["volume"] = round(latest_data[apt]["gasmeter"]["volume"] + random.uniform(0.0, 5.0), 2)
                latest_data[apt]["gasmeter"]["battery"] = max(0, latest_data[apt]["gasmeter"]["battery"] - random.randint(0, 1))
            publish_gasmeter(apt)
        time.sleep(DATA_SENDING_INTERVAL)

def thermostat_updater():
    while True:
        for apt in APARTMENTS:
            with latest_data_lock:
                th = latest_data[apt]["thermostat"]
                # small random walk around temperature and humidity
                th["temperature"] = round(th["temperature"] + random.uniform(-0.3, 0.3), 1)
                th["humidity"] = max(0, min(100, th["humidity"] + random.randint(-1, 1)))
                th["co2"] = max(200, th["co2"] + random.randint(-5, 5))
                # fan_status follows fan_setting
                th["fan_status"] = th["fan_setting"]
            publish_thermostat(apt)
        time.sleep(DATA_SENDING_INTERVAL)

threading.Thread(target=aqi_updater, daemon=True).start()
threading.Thread(target=wallswitch_updater, daemon=True).start()
threading.Thread(target=wallsocket_updater, daemon=True).start()
threading.Thread(target=curtain_updater, daemon=True).start()
threading.Thread(target=peoplecounter_updater, daemon=True).start()
threading.Thread(target=doorlock_updater, daemon=True).start()
threading.Thread(target=scb_updater, daemon=True).start()
threading.Thread(target=watermeter_updater, daemon=True).start()
threading.Thread(target=gasmeter_updater, daemon=True).start()
threading.Thread(target=thermostat_updater, daemon=True).start()

app = Flask(__name__)

@app.route("/<apartment>/items/<item_name>/state", methods=["GET"])
def get_item_state(apartment, item_name):
    if apartment not in latest_data:
        return "Apartment not found", 404

    # AQI mappings (apartment-level)
    if item_name.endswith("_temp"):
        return str(latest_data[apartment]["aqi"]["temp"])
    if item_name.endswith("_humd"):
        return str(latest_data[apartment]["aqi"]["humd"])
    if item_name.endswith("_co2"):
        return str(latest_data[apartment]["aqi"]["co2"])
    if item_name.endswith("_battery"):
        return str(latest_data[apartment]["aqi"]["battery"])

    # Wall Socket mappings (apartment-level)
    if item_name.endswith("_socket_current"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallsocket"]["current"])
    if item_name.endswith("_socket_voltage"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallsocket"]["voltage"])
    if item_name.endswith("_socket_active_power"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallsocket"]["active_power"])
    if item_name.endswith("_socket_power_consumption"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallsocket"]["power_consumption"])
    if item_name.endswith("_socket_power_factor"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallsocket"]["power_factor"])
    if item_name.endswith("_socket_status"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallsocket"]["socket_status"])
        
    # Curtain mappings (apartment-level)
    if item_name.endswith("_curtainstate"):
        with latest_data_lock:
            return str(latest_data[apartment]["curtain"]["curtainstate"])
    if item_name.endswith("_curtain_battery"):
        with latest_data_lock:
            return str(latest_data[apartment]["curtain"]["battery"])

    # PeopleCounter mappings (apartment-level)
    if "PeopleCounter" in item_name and item_name.endswith("_total_in"):
        with latest_data_lock:
            return str(latest_data[apartment]["peoplecounter"]["total_in"])
    if "PeopleCounter" in item_name and item_name.endswith("_total_out"):
        with latest_data_lock:
            return str(latest_data[apartment]["peoplecounter"]["total_out"])
    if "PeopleCounter" in item_name and item_name.endswith("_period_in"):
        with latest_data_lock:
            return str(latest_data[apartment]["peoplecounter"]["period_in"])
    if "PeopleCounter" in item_name and item_name.endswith("_period_out"):
        with latest_data_lock:
            return str(latest_data[apartment]["peoplecounter"]["period_out"])
    if "PeopleCounter" in item_name and item_name.endswith("_battery"):
        with latest_data_lock:
            return str(latest_data[apartment]["peoplecounter"]["battery"])
    if "PeopleCounter" in item_name and item_name.endswith("_temperature"):
        with latest_data_lock:
            return str(latest_data[apartment]["peoplecounter"]["temperature"])
     # People count (total_in - total_out)
    if "PeopleCounter" in item_name and item_name.endswith("_count"):
        with latest_data_lock:
            return str(latest_data[apartment]["peoplecounter"].get("count", latest_data[apartment]["peoplecounter"]["total_in"] - latest_data[apartment]["peoplecounter"]["total_out"]))
    
    # Door Lock mappings (apartment-level) removed in favor of generic DoorLock_* mapping below.
        
    # Circuit Breaker mappings (apartment-level)
    if "CircuitBreaker" in item_name and item_name.endswith("_device_type"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["device_type"])
    if "CircuitBreaker" in item_name and item_name.endswith("_breaker_address"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["breaker_address"])
    if "CircuitBreaker" in item_name and item_name.endswith("_breaker_type"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["breaker_type"])
    if "CircuitBreaker" in item_name and item_name.endswith("_switch_state"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["switch_state"])
    if "CircuitBreaker" in item_name and item_name.endswith("_remote_control_enabled"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["remote_control_enabled"])
    if "CircuitBreaker" in item_name and item_name.endswith("_voltage_A"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["voltage_A"])
    if "CircuitBreaker" in item_name and item_name.endswith("_voltage_B"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["voltage_B"])
    if "CircuitBreaker" in item_name and item_name.endswith("_voltage_C"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["voltage_C"])
    if "CircuitBreaker" in item_name and item_name.endswith("_current_A"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["current_A"])
    if "CircuitBreaker" in item_name and item_name.endswith("_current_B"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["current_B"])
    if "CircuitBreaker" in item_name and item_name.endswith("_current_C"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["current_C"])
    if "CircuitBreaker" in item_name and item_name.endswith("_power_total"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["power_total"])
    if "CircuitBreaker" in item_name and item_name.endswith("_leakage_current"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["leakage_current"])
    if "CircuitBreaker" in item_name and item_name.endswith("_temperature_device"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["temperature_device"])
    if "CircuitBreaker" in item_name and item_name.endswith("_alarm_overload"):
        with latest_data_lock:
            return str(latest_data[apartment]["scb"]["alarm_overload"])
    # Thermostat mappings (apartment-level)
    # check setpoint BEFORE generic temperature because item names like
    # Thermostat_setpoint_temperature also end with '_temperature'
    if "Thermostat" in item_name and item_name.endswith("_setpoint_temperature"):
        with latest_data_lock:
            return str(latest_data[apartment]["thermostat"]["setpoint_temperature"])
    if "Thermostat" in item_name and item_name.endswith("_temperature"):
        with latest_data_lock:
            return str(latest_data[apartment]["thermostat"]["temperature"])
    if "Thermostat" in item_name and item_name.endswith("_humidity"):
        with latest_data_lock:
            return str(latest_data[apartment]["thermostat"]["humidity"])
    if "Thermostat" in item_name and item_name.endswith("_mode"):
        with latest_data_lock:
            return str(latest_data[apartment]["thermostat"]["mode"])
    if "Thermostat" in item_name and item_name.endswith("_status"):
        with latest_data_lock:
            return str(latest_data[apartment]["thermostat"]["status"])
    if "Thermostat" in item_name and item_name.endswith("_fan_setting"):
        with latest_data_lock:
            return str(latest_data[apartment]["thermostat"]["fan_setting"])
    if "Thermostat" in item_name and item_name.endswith("_valve_status"):
        with latest_data_lock:
            return str(latest_data[apartment]["thermostat"]["valve_status"])
    if "Thermostat" in item_name and item_name.endswith("_fan_status"):
        with latest_data_lock:
            return str(latest_data[apartment]["thermostat"]["fan_status"])
    if "Thermostat" in item_name and item_name.endswith("_co2"):
        with latest_data_lock:
            return str(latest_data[apartment]["thermostat"]["co2"])
    if "Thermostat" in item_name and item_name.endswith("_power"):
        with latest_data_lock:
            return str(latest_data[apartment]["thermostat"]["power"])
    # Water Meter mappings (apartment-level)
    if "WaterMeter" in item_name and item_name.endswith("_volume"):
        with latest_data_lock:
            return str(latest_data[apartment]["watermeter"]["volume"])
    if "WaterMeter" in item_name and item_name.endswith("_valve_state"):
        with latest_data_lock:
            return str(latest_data[apartment]["watermeter"]["valve_state"])
    if "WaterMeter" in item_name and item_name.endswith("_battery"):
        with latest_data_lock:
            return str(latest_data[apartment]["watermeter"]["battery"])
    if "WaterMeter" in item_name and item_name.endswith("_low_power"):
        with latest_data_lock:
            return str(latest_data[apartment]["watermeter"]["low_power"])
    if "WaterMeter" in item_name and item_name.endswith("_alarm"):
        with latest_data_lock:
            return str(latest_data[apartment]["watermeter"]["alarm"])
    if "WaterMeter" in item_name and item_name.endswith("_communication_error"):
        with latest_data_lock:
            return str(latest_data[apartment]["watermeter"]["communication_error"])
    # Gas Meter mappings (apartment-level)
    if "GasMeter" in item_name and item_name.endswith("_volume"):
        with latest_data_lock:
            return str(latest_data[apartment]["gasmeter"]["volume"])
    if "GasMeter" in item_name and item_name.endswith("_valve_state"):
        with latest_data_lock:
            return str(latest_data[apartment]["gasmeter"]["valve_state"])
    if "GasMeter" in item_name and item_name.endswith("_battery"):
        with latest_data_lock:
            return str(latest_data[apartment]["gasmeter"]["battery"])
    if "GasMeter" in item_name and item_name.endswith("_low_power"):
        with latest_data_lock:
            return str(latest_data[apartment]["gasmeter"]["low_power"])
    if "GasMeter" in item_name and item_name.endswith("_alarm"):
        with latest_data_lock:
            return str(latest_data[apartment]["gasmeter"]["alarm"])

    if "Switch" in item_name:
        return "This switch item requires a room segment in the URL. Use /<apartment>/items/<item_name>/<room>/state", 400

    # Generic mapping for DoorLock: allow GETs for any existing key under latest_data[apartment]['doorlock']
    if "DoorLock" in item_name and "_door_" in item_name:
        # extract the key portion after 'DoorLock_' then strip 'door_' if present
        field = item_name.split('DoorLock_', 1)[-1]
        if field.startswith('door_'):
            field = field[len('door_'):]
        with latest_data_lock:
            val = latest_data[apartment]['doorlock'].get(field)
            if val is not None:
                return str(val)

    return "Item not found", 404

@app.route("/<apartment>/items/<item_name>/<room>/state", methods=["GET"])
def get_wallswitch_item_state(apartment, item_name, room):
    if apartment not in latest_data:
        return "Apartment not found", 404
    if room not in APARTMENTS[apartment]["rooms"]:
        return "Room not found", 404

    if item_name.endswith("_current"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallswitch"][room]["current"])
    if item_name.endswith("_voltage"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallswitch"][room]["voltage"])
    if item_name.endswith("_active_power"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallswitch"][room]["active_power"])
    if item_name.endswith("_power_consumption"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallswitch"][room]["power_consumption"])
    if item_name.endswith("_power_factor"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallswitch"][room]["power_factor"])
    if item_name.endswith("_switch_1"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallswitch"][room]["switch_1"])
    if item_name.endswith("_switch_2"):
        with latest_data_lock:
            return str(latest_data[apartment]["wallswitch"][room]["switch_2"])

    return "Item not found", 404

@app.route('/<apartment>/items/Update_Apartment_smart_Switch/<room>/state', methods=['PUT'])
def change_wallswitch(apartment, room):
    if apartment not in latest_data:
        return jsonify({'error': 'Apartment not found'}), 404
    if room not in APARTMENTS[apartment]["rooms"]:
        return jsonify({'error': 'Room not found'}), 404

    switch_states = {}
    for param, value in request.args.items():
        if param.startswith('switch_'):
            try:
                switch_num = int(param.split('_')[1])
            except Exception:
                continue
            state = value.lower() in ('true', '1', 'on')
            switch_states[switch_num] = state
            with latest_data_lock:
                latest_data[apartment]["wallswitch"][room][f"switch_{switch_num}"] = 1 if state else 0

    switch_control = 0
    for switch_num, state in switch_states.items():
        if 1 <= switch_num <= 4:
            switch_control |= 1 << (switch_num + 3)
            if state:
                switch_control |= 1 << (switch_num - 1)

    command_bytes = bytes([8, switch_control, 255])
    command = base64.b64encode(command_bytes).decode()

    device_id = f"{APARTMENTS[apartment]['switch_device_id']}_{room}"
    message = json.dumps({"confirmed": True, "fport": 85, "data": command})
    topic = f'milesight/downlink/{device_id}'

    try:
        publish.single(topic, message, hostname=MQTT_SERVER, port=MQTT_PORT)

        threading.Timer(1.0, publish_wallswitch, args=(apartment, room)).start()
        
        with latest_data_lock:
            new_state = latest_data[apartment]["wallswitch"][room].copy()
        return jsonify({
            'status': f"Switch updated, command {command} published to {topic}",
            'new_state': new_state
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/<apartment>/items/Update_Apartment_smart_Socket/state', methods=['PUT'])
def change_wallsocket(apartment):
    if apartment not in latest_data:
        return jsonify({'error': 'Apartment not found'}), 404
    
    if 'socket_status' not in request.args:
        return jsonify({'error': 'socket_status parameter is required'}), 400
    socket_status = request.args.get('socket_status').lower() in ('true', '1', 'on')
    with latest_data_lock:
        latest_data[apartment]["wallsocket"]["socket_status"] = 1 if socket_status else 0

    if socket_status == True:
        command = "CAEA/w=="
    else:
        command = "CAAA/w=="

    device_id = f"{APARTMENTS[apartment]['socket_device_id']}"
    message = json.dumps({"confirmed": True, "fport": 85, "data": command})
    topic = f'milesight/downlink/{device_id}'

    try:
        publish.single(topic, message, hostname=MQTT_SERVER, port=MQTT_PORT)

        threading.Timer(1.0, publish_wallsocket, args=(apartment,)).start()
        
        with latest_data_lock:
            new_state = latest_data[apartment]["wallsocket"].copy()
        return jsonify({
            'status': f"Socket updated, command {command} published to {topic}",
            'new_state': new_state
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/<apartment>/items/Update_Apartment_smart_Curtain/state', methods=['PUT'])
def change_curtain(apartment):
    if apartment not in latest_data:
        return jsonify({'error': 'Apartment not found'}), 404

    if 'curtainstate' not in request.args:
        return jsonify({'error': 'curtainstate parameter is required (0-100)'}), 400
    try:
        pos = int(request.args.get('curtainstate'))
    except Exception:
        return jsonify({'error': 'curtainstate must be integer 0-100'}), 400
    if pos < 0 or pos > 100:
        return jsonify({'error': 'curtainstate must be in range 0-100'}), 400

    with latest_data_lock:
        latest_data[apartment]["curtain"]["curtainstate"] = pos

    # Build downlink command: [9, position, 255]
    command_bytes = bytes([9, pos, 255])
    command = base64.b64encode(command_bytes).decode()

    device_id = f"{APARTMENTS[apartment]['curtain_device_id']}"
    message = json.dumps({"confirmed": True, "fport": 85, "data": command})
    topic = f'milesight/downlink/{device_id}'

    try:
        publish.single(topic, message, hostname=MQTT_SERVER, port=MQTT_PORT)

        # delayed publish of the updated curtain packet for this apartment
        threading.Timer(1.0, publish_curtain, args=(apartment,)).start()

        with latest_data_lock:
            new_state = latest_data[apartment]["curtain"].copy()
        return jsonify({
            'status': f"Curtain updated to {pos}, command {command} published to {topic}",
            'new_state': new_state
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/<apartment>/items/Update_Apartment_smart_DoorLock/state', methods=['PUT'])
def change_doorlock(apartment):
    """Unified doorlock control/update endpoint (PUT).
    Supported query params (action):
    - action=remote_control&state=unlock|lock
    - action=manage_password&user_id=<int>&password=<6-digit>
    - action=manage_card&user_id=<int>&card_key_hex=<10 hex chars>
    - action=access_event&access_method=<password|card|remote>&user_id=<int>&timestamp=<iso>
    Note: This keeps a single consistent URL for control and simulation updates.
    """
    if apartment not in latest_data:
        return jsonify({'error': 'Apartment not found'}), 404

    action = request.args.get('action')
    if not action:
        return jsonify({'error': 'action parameter is required'}), 400
    action = action.lower()

    device_id = APARTMENTS[apartment].get('doorlock_device_id')
    if not device_id:
        return jsonify({'error': 'doorlock not configured for apartment'}), 404

    try:
        with latest_data_lock:
            dl = latest_data[apartment]['doorlock']

        if action == 'remote_control':
            state = request.args.get('state')
            if not state or state.lower() not in ('unlock', 'lock', "0", "1"):
                return jsonify({'error': 'state must be "unlock" or "lock"'}), 400
            state = state.lower()
            # Command 0x36: 36 01 01 (unlock) or 36 01 00 (lock)
            cmd_hex = '360101' if state == 'unlock' or state == "0" else '360100'
            cmd_bytes = bytes.fromhex(cmd_hex)
            # Update internal state to reflect lock/unlock
            with latest_data_lock:
                    dl['remote_lock'] = 1 if state == 'unlock' or state == "0" else 0
                    # reflect immediate current status for API GETs
                    dl['current_status'] = 0 if state == 'unlock' or state == "0" else 1
                    if state == 'unlock':
                        dl['unlock_record'] = dl.get('unlock_record', 0) + 1
                    dl['t'] = datetime.now(timezone.utc).isoformat()
                    dl['last_access_method'] = 'remote'
                    dl['last_access_user_id'] = 0
                    dl['last_access_timestamp'] = dl['t']

        elif action == 'manage_password':
            # require user_id and password
            user_id = request.args.get('user_id')
            password = request.args.get('password')
            if not user_id or not password:
                return jsonify({'error': 'user_id and password are required'}), 400
            try:
                uid = int(user_id)
            except Exception:
                return jsonify({'error': 'invalid user_id'}), 400
            if not (isinstance(password, str) and len(password) == 6 and password.isdigit()):
                return jsonify({'error': 'password must be 6 digits'}), 400
            header = bytes.fromhex('4E0900')
            uid_b = uid.to_bytes(1, 'big')
            len_b = bytes([6])
            pwd_b = bytes([int(d) for d in password])
            cmd_bytes = header + uid_b + len_b + pwd_b
            # record management action so it appears in next uplink
            with latest_data_lock:
                dl['t'] = datetime.now(timezone.utc).isoformat()
                dl['last_manage_action'] = 'manage_password'
                dl['last_manage_user_id'] = uid

        elif action == 'manage_card':
            user_id = request.args.get('user_id')
            card_key_hex = request.args.get('card_key_hex')
            if not user_id or not card_key_hex:
                return jsonify({'error': 'user_id and card_key_hex are required'}), 400
            try:
                uid = int(user_id)
            except Exception:
                return jsonify({'error': 'invalid user_id'}), 400
            if not (isinstance(card_key_hex, str) and len(card_key_hex) == 10):
                return jsonify({'error': 'card_key_hex must be 10 hex chars'}), 400
            try:
                card_b = bytes.fromhex(card_key_hex)
            except Exception:
                return jsonify({'error': 'card_key_hex invalid hex'}), 400
            header = bytes.fromhex('4D0700')
            uid_b = uid.to_bytes(1, 'big')
            cmd_bytes = header + uid_b + card_b
            # record management action so it appears in next uplink
            with latest_data_lock:
                dl['t'] = datetime.now(timezone.utc).isoformat()
                dl['last_manage_action'] = 'manage_card'
                dl['last_manage_user_id'] = uid

        elif action == 'access_event':
            # Simulates device reporting a user access (unlock via password/card/remote)
            method = request.args.get('access_method') or request.args.get('method')
            user_id = request.args.get('user_id')
            ts = request.args.get('timestamp') or datetime.now(timezone.utc).isoformat()
            if not method or not user_id:
                return jsonify({'error': 'access_method and user_id are required'}), 400
            method = method.lower()
            try:
                uid = int(user_id)
            except Exception:
                return jsonify({'error': 'invalid user_id'}), 400
            with latest_data_lock:
                dl['last_access_method'] = method
                dl['last_access_user_id'] = uid
                dl['last_access_timestamp'] = ts
                dl['t'] = datetime.now(timezone.utc).isoformat()
                # Increment unlock counter on any access event (successful unlock)
                dl['unlock_record'] = dl.get('unlock_record', 0) + 1
            # no downlink for access_event; we just simulate device reporting
            threading.Timer(1.0, publish_doorlock, args=(apartment,)).start()
            with latest_data_lock:
                new_state = latest_data[apartment]['doorlock'].copy()
            return jsonify({'status': 'access_event recorded', 'new_state': new_state}), 200

        elif action == 'set_auto_relock':
            # set_auto_relock?enabled=true|false&timeout=<seconds>
            enabled = request.args.get('enabled')
            timeout = request.args.get('timeout')
            with latest_data_lock:
                if enabled is not None:
                    en = enabled.lower() in ('1', 'true', 'yes', 'on')
                    dl['auto_relock_enabled'] = en
                if timeout is not None:
                    try:
                        to = int(timeout)
                        dl['auto_relock'] = to
                    except Exception:
                        return jsonify({'error': 'invalid timeout'}), 400
                dl['t'] = datetime.now(timezone.utc).isoformat()
            threading.Timer(1.0, publish_doorlock, args=(apartment,)).start()
            with latest_data_lock:
                new_state = latest_data[apartment]['doorlock'].copy()
            return jsonify({'status': 'auto_relock updated', 'new_state': new_state}), 200

        else:
            return jsonify({'error': 'unsupported action'}), 400

        # If we reach here we have cmd_bytes to send downlink
        command_b64 = base64.b64encode(cmd_bytes).decode()
        message = json.dumps({"confirmed": True, "fport": LORAWAN_FPORT, "data": command_b64})
        topic = f'milesight/downlink/{device_id}'
        publish.single(topic, message, hostname=MQTT_SERVER, port=MQTT_PORT)
        threading.Timer(1.0, publish_doorlock, args=(apartment,)).start()
        with latest_data_lock:
            new_state = latest_data[apartment]['doorlock'].copy()
        return jsonify({'status': 'command sent', 'downlink': command_b64, 'new_state': new_state}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/<apartment>/items/Update_Apartment_smart_CircuitBreaker/state', methods=['PUT'])
def change_scb(apartment):
    """Control Circuit Breaker: use param `action=on|off` to change switch_state."""
    if apartment not in latest_data:
        return jsonify({'error': 'Apartment not found'}), 404

    if 'action' not in request.args:
        return jsonify({'error': 'action parameter is required (on|off or 1|0)'}), 400
    action = request.args.get('action').lower()
    # normalize numeric values to on/off
    if action in ('1', 'true'):
        action = 'on'
    elif action in ('0', 'false'):
        action = 'off'
    if action not in ('on', 'off'):
        return jsonify({'error': 'action must be "on" or "off" (or 1/0)'}), 400

    with latest_data_lock:
        scb = latest_data[apartment]['scb']
        # store as integer: 1 = connection made (CLOSED/on), 0 = connection broken (OPEN/off)
        scb['switch_state'] = 1 if action == 'on' else 0

    # Use provided example raw commands
    if action == 'off':
        cmd_bytes = bytes.fromhex('AA030101003DCC55')
    else:
        cmd_bytes = bytes.fromhex('AA03010101FC0C55')
    command = base64.b64encode(cmd_bytes).decode()

    device_id = f"{APARTMENTS[apartment]['scb_device_id']}"
    message = json.dumps({"confirmed": True, "fport": 85, "data": command})
    topic = f'milesight/downlink/{device_id}'
    try:
        publish.single(topic, message, hostname=MQTT_SERVER, port=MQTT_PORT)
        threading.Timer(1.0, publish_scb, args=(apartment,)).start()
        with latest_data_lock:
            new_state = latest_data[apartment]['scb'].copy()
        return jsonify({'status': f'scb {action} command sent', 'new_state': new_state}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/<apartment>/items/Update_Apartment_smart_WaterMeter/state', methods=['PUT'])
def change_watermeter(apartment):
    if apartment not in latest_data:
        return jsonify({'error': 'Apartment not found'}), 404

    # Accept 'action' (open/close) or 'valve' (on/off/1/0)
    action = request.args.get('action') or request.args.get('valve')
    if not action:
        return jsonify({'error': 'action or valve parameter required (open/close or on/off)'}), 400
    action = action.lower()
    # Interpret 'open', 'off', '0', 'false' as valve OPEN (cut off)
    open_values = ('open', 'off', '0', 'false')
    # Interpret 'close', 'on', '1', 'true' as valve CLOSED (connection made)
    close_values = ('close', 'on', '1', 'true')
    if action in open_values:
        state_str = 'OPEN'
        state_int = 0
    elif action in close_values:
        state_str = 'CLOSED'
        state_int = 1
    else:
        return jsonify({'error': 'invalid action'}), 400

    with latest_data_lock:
        latest_data[apartment]['watermeter']['valve_state'] = state_int

    # command encoding uses 1 for OPEN, 0 for CLOSED per device spec
    cmd_byte = 1 if state_str == 'OPEN' else 0
    command_bytes = bytes([12, cmd_byte, 255])
    command = base64.b64encode(command_bytes).decode()
    device_id = f"{APARTMENTS[apartment]['watermeter_device_id']}"
    message = json.dumps({"confirmed": True, "fport": 85, "data": command})
    topic = f'milesight/downlink/{device_id}'
    try:
        publish.single(topic, message, hostname=MQTT_SERVER, port=MQTT_PORT)
        threading.Timer(1.0, publish_watermeter, args=(apartment,)).start()
        with latest_data_lock:
            new_state = latest_data[apartment]['watermeter'].copy()
        return jsonify({'status': f'watermeter valve {state_str}', 'new_state': new_state}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/<apartment>/items/Update_Apartment_smart_GasMeter/state', methods=['PUT'])
def change_gasmeter(apartment):
    if apartment not in latest_data:
        return jsonify({'error': 'Apartment not found'}), 404

    action = request.args.get('action') or request.args.get('valve')
    if not action:
        return jsonify({'error': 'action or valve parameter required (open/close or on/off)'}), 400
    action = action.lower()
    # Interpret 'open', 'off', '0', 'false' as valve OPEN (cut off)
    if action in ('open', 'off', '0', 'false'):
        state_str = 'OPEN'
        state_int = 0
    elif action in ('close', 'on', '1', 'true'):
        state_str = 'CLOSED'
        state_int = 1
    else:
        return jsonify({'error': 'invalid action'}), 400

    with latest_data_lock:
        latest_data[apartment]['gasmeter']['valve_state'] = state_int

    cmd_byte = 1 if state_str == 'OPEN' else 0
    command_bytes = bytes([13, cmd_byte, 255])
    command = base64.b64encode(command_bytes).decode()
    device_id = f"{APARTMENTS[apartment]['gasmeter_device_id']}"
    message = json.dumps({"confirmed": True, "fport": 85, "data": command})
    topic = f'milesight/downlink/{device_id}'
    try:
        publish.single(topic, message, hostname=MQTT_SERVER, port=MQTT_PORT)
        threading.Timer(1.0, publish_gasmeter, args=(apartment,)).start()
        with latest_data_lock:
            new_state = latest_data[apartment]['gasmeter'].copy()
        return jsonify({'status': f'gasmeter valve {state_str}', 'new_state': new_state}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/<apartment>/items/Update_Apartment_smart_Thermostat/state', methods=['PUT'])
def change_thermostat(apartment):
    """Control thermostat via query params:
    - power=on|off
    - fan=high|medium|low|auto
    - mode=cool|heat|auto|vent|dehumidify|off
    - setpoint=<float>
    Multiple params can be provided together; each will publish corresponding downlink.
    """
    if apartment not in latest_data:
        return jsonify({'error': 'Apartment not found'}), 404

    updates = {}
    # power
    if 'power' in request.args:
        p = request.args.get('power').lower()
        if p in ('on', '1', 'true'):
            updates['power'] = 'on'
        elif p in ('off', '0', 'false'):
            updates['power'] = 'off'
        else:
            return jsonify({'error': 'invalid power value'}), 400

    # fan
    if 'fan' in request.args:
        f = request.args.get('fan').lower()
        if f in ('high', 'medium', 'low', 'auto'):
            updates['fan_setting'] = f
        else:
            return jsonify({'error': 'invalid fan value'}), 400

    # mode
    if 'mode' in request.args:
        m = request.args.get('mode').lower()
        if m in ('cool', 'heat', 'auto', 'vent', 'dehumidify', 'off'):
            updates['mode'] = m
        else:
            return jsonify({'error': 'invalid mode value'}), 400

    # setpoint
    if 'setpoint' in request.args:
        try:
            sp = float(request.args.get('setpoint'))
        except Exception:
            return jsonify({'error': 'invalid setpoint value'}), 400
        updates['setpoint_temperature'] = round(sp, 1)

    if not updates:
        return jsonify({'error': 'no valid control parameters provided'}), 400

    device_id = APARTMENTS[apartment].get('thermostat_device_id')
    if not device_id:
        return jsonify({'error': 'thermostat not configured for apartment'}), 404

    # Apply updates locally and build downlink commands per change
    commands = []
    with latest_data_lock:
        th = latest_data[apartment]['thermostat']
        # power command
        if 'power' in updates:
            th['power'] = updates['power']
            cmd = bytes([0x00, 0x01, 0x01]) if th['power'] == 'on' else bytes([0x00, 0x01, 0x00])
            commands.append(cmd)
        # fan command
        if 'fan_setting' in updates:
            th['fan_setting'] = updates['fan_setting']
            mapping = {'high': 0x00, 'medium': 0x01, 'low': 0x02, 'auto': 0x03}
            cmd = bytes([0x01, 0x01, mapping[th['fan_setting']]])
            commands.append(cmd)
        # mode command
        if 'mode' in updates:
            th['mode'] = updates['mode']
            mapping = {'off':0x00,'cool':0x01,'heat':0x02,'vent':0x03,'dehumidify':0x04,'auto':0x05}
            cmd = bytes([0x02, 0x01, mapping[th['mode']]])
            commands.append(cmd)
        # setpoint command
        if 'setpoint_temperature' in updates:
            th['setpoint_temperature'] = updates['setpoint_temperature']
            # record timestamp so publishes/GETs can verify latest setpoint
            th['last_setpoint_timestamp'] = datetime.now(timezone.utc).isoformat()
            val = int(round(th['setpoint_temperature'] * 10))
            high = (val >> 8) & 0xFF
            low = val & 0xFF
            cmd = bytes([0x03, 0x02, high, low])
            commands.append(cmd)

    # Small verification: ensure setpoint persisted in latest_data after applying updates
    if 'setpoint_temperature' in updates:
        expected = updates['setpoint_temperature']
        # quick retry loop (very short) to guard against a race
        retried = 0
        while retried < 3:
            with latest_data_lock:
                current = latest_data[apartment]['thermostat']['setpoint_temperature']
            if current == expected:
                break
            # re-write under lock as a defensive attempt
            with latest_data_lock:
                latest_data[apartment]['thermostat']['setpoint_temperature'] = expected
                latest_data[apartment]['thermostat']['last_setpoint_timestamp'] = datetime.now(timezone.utc).isoformat()
            retried += 1

    # publish each command as a downlink base64 payload
    sent = []
    try:
        for cmd in commands:
            command_b64 = base64.b64encode(cmd).decode()
            message = json.dumps({"confirmed": True, "fport": 85, "data": command_b64})
            topic = f'milesight/downlink/{device_id}'
            publish.single(topic, message, hostname=MQTT_SERVER, port=MQTT_PORT)
            sent.append(cmd.hex().upper())

        # schedule an updated thermostat publish after 1s
        threading.Timer(1.0, publish_thermostat, args=(apartment,)).start()
        with latest_data_lock:
            new_state = latest_data[apartment]['thermostat'].copy()
        return jsonify({'status': 'commands sent', 'commands': sent, 'new_state': new_state}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=HTTP_SERVER_PORT, debug=False, use_reloader=False)