#!/usr/bin/env python
"""
Script for controlling Hoymiles microinverters by modbus commands

Inputs: Power values from the victron system 
(Grid in, PV in, AC out, Chargepower, system state)
-------------------------------------------------------------------------

ToDo:

"""

import os
from socket import socket
from stat import FILE_ATTRIBUTE_DIRECTORY
import string
import sys
import time
import logging
import logging.handlers
import argparse
from xmlrpc.client import boolean
from gi.repository import GLib as gobject
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTT_ERR_SUCCESS
from paho.mqtt.client import MQTT_ERR_NO_CONN
from paho.mqtt.client import MQTT_ERR_QUEUE_SIZE

from collections import OrderedDict

# --------------------------------------------------------------------------- #
# import the various server implementations
# --------------------------------------------------------------------------- #
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
# from pymodbus.client.sync import ModbusUdpClient as ModbusClient
# from pymodbus.client.sync import ModbusSerialClient as ModbusClient

# --------------------------------------------------------------------------- #
# configure the client logging
# --------------------------------------------------------------------------- #
# create logger
log = logging.getLogger('hm_mb_control')
log.setLevel(logging.INFO)
# create file handler which logs even debug messages
fh = logging.handlers.TimedRotatingFileHandler('hm_mb_control.log','D', 1, 5)
fh.setLevel(logging.INFO)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(threadName)s - '
                                '%(levelname)s - %(module)s:%(lineno)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
log.addHandler(fh)
log.addHandler(ch)

# parse arguments
parser = argparse.ArgumentParser(description = 'Victron modbus control test')
parser.add_argument('--version', action='version', version='%(prog)s v')
parser.add_argument('--debug', action="store_true", help='enable debug logging')
# parser.add_argument('--power', default=100, type=int, help='set the output of all MIs to xx percent')
# parser.add_argument('--max', action="store_true", help='set the output of all MIs to 100 percent')
# parser.add_argument('--min', action="store_true", help='set the output of all MIs to 2 percent')
# parser.add_argument('--on', action="store_true", help='switch all MIs ON')
# parser.add_argument('--off', action="store_true", help='switch all MIs OFF')
# parser.add_argument('--mqtt', action="store_true", help= 'enable mqtt data output')
requiredArguments = parser.add_argument_group('required arguments')
args = parser.parse_args()

if args.debug: # switch to debug level
    log.setLevel(logging.DEBUG)
    ch.setLevel(logging.DEBUG)
    fh.setLevel(logging.DEBUG)

# setup modbus unit infos
VIC_UNIT = 100  	# unit ID for the Victron system
HM_UNIT = 0x00		# unit ID for the Hoymiles system
VEBUS_UNIT = 238    # unit ID for the VE.Bus system
# setup mqtt infos
PORT = 1883
BROKER = "mosquitto.fritz.box"
# voltage levels
V_ABSORPTION = 55.2     # absorption voltage 55.2V
V_FLOAT = 53.6          # float voltage 53.6V

# global variables
soc = 0				    # state of charge of the battery
gridTotal = 0		    # grid power
acOutTotal = 0		    # power on AC output
acPVTotal = 0		    # PV power
charge = 0			    # charge power (multi to battery)
veBusState = 0          # VE.Bus state 
                        # 0=Off;1=Low Power;2=Fault;3=Bulk;4=Absorption;5=Float;6=Storage;
                        # 7=Equalize;8=Passthru;9=Inverting;10=Power assist;
                        # 11=Power supply;252=Bulk protection
batteryVoltage = 0.0    # battery voltage
setPercentage = 100     # setpoint (in %) for PV inverter power (calculated by control algo)
isControlling = False   # control loop is running?
autoControl = True      # automatic HM control on/off (via mqtt remote control)
setpoint = 0            # calculated setpoint in W for control algo
setpointPrev = 0        # setpoint in W from last control algo run
setDeltas = [0, 0, 0, 0, 0, 0]
                        # delta PV-power setpoint to delivered PV-power for the last 6 runs
powerSetpoint = 100     # power setpoint for manual control (via mqtt remote control)

# The callback for when the mqtt client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    log.info("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # client.subscribe("$SYS/#")
    client.subscribe("HM-Control/AutoControl")
    client.subscribe("HM-Control/Perc-Power")


# The callback for when a PUBLISH message is received from the mqtt server.
def on_message(client, userdata, msg):
    global autoControl
    global powerSetpoint

    log.debug(msg.topic+" "+str(msg.payload))
    if msg.topic == "HM-Control/Perc-Power":
        log.info("Handling change of Power setpoint")
        powerSetpoint = int(msg.payload)
        log.debug(powerSetpoint)
    elif msg.topic == "HM-Control/AutoControl":
        log.info("Handling change of control flag")
        if msg.payload == b'true':      # payload is bitvalue --> convert to boolean
            autoControl = True
        else: 
            autoControl = False
        log.debug(autoControl)


# setup mqtt client
mqttClient = mqtt.Client()
mqttClient.on_connect = on_connect
mqttClient.on_message = on_message

mqttClient.enable_logger(logger=log)
mqttClient.connect(BROKER, PORT)
mqttClient.loop_start()


# ------
# get all the necessary inputs from the victron system
# ------
def get_inputs():
    
    # vic_client = ModbusClient('192.168.178.50', port=502)
    # vic_client.connect()

    global soc
    global gridTotal
    global acOutTotal
    global acPVTotal
    global charge
    global veBusState
    global batteryVoltage
    global setPercentage
    global isControlling

    # get SOC
    rr = vic_client.read_holding_registers(843, 1, unit=VIC_UNIT)   # SOC
    assert(not rr.isError())     # test that we are not an error
    soc = rr.registers[0] 
    # get grid power values
    rr = vic_client.read_holding_registers(820, 3, unit=VIC_UNIT)   # grid power
    assert(not rr.isError())     # test that we are not an error
    decoder = BinaryPayloadDecoder.fromRegisters(rr.registers, byteorder=Endian.Big, wordorder=Endian.Big)
    gridL1 = decoder.decode_16bit_int()
    gridL2 = decoder.decode_16bit_int()
    gridL3 = decoder.decode_16bit_int()
    gridTotal = gridL1 + gridL2 + gridL3   
    # get ACout power values
    rr = vic_client.read_holding_registers(817, 3, unit=VIC_UNIT)   # grid power
    assert(not rr.isError())     # test that we are not an error
    decoder = BinaryPayloadDecoder.fromRegisters(rr.registers, byteorder=Endian.Big, wordorder=Endian.Big)
    acOutL1 = decoder.decode_16bit_uint()
    acOutL2 = decoder.decode_16bit_uint()
    acOutL3 = decoder.decode_16bit_uint()
    acOutTotal = acOutL1 + acOutL2 + acOutL3
    # get PV power on ACout
    rr = vic_client.read_holding_registers(808, 3, unit=VIC_UNIT)   # grid power
    assert(not rr.isError())     # test that we are not an error
    decoder = BinaryPayloadDecoder.fromRegisters(rr.registers, byteorder=Endian.Big, wordorder=Endian.Big)
    acPVL1 = decoder.decode_16bit_uint()
    acPVL2 = decoder.decode_16bit_uint()
    acPVL3 = decoder.decode_16bit_uint()
    acPVTotal = acPVL1 + acPVL2 + acPVL3
    # get charge power
    rr = vic_client.read_holding_registers(866, 1, unit=VIC_UNIT)   # charge power (multi to battery)
    assert(not rr.isError())     # test that we are not an error
    decoder = BinaryPayloadDecoder.fromRegisters(rr.registers, byteorder=Endian.Big, wordorder=Endian.Big)
    charge = decoder.decode_16bit_int()

    # get VE-Bus state
    rr = vic_client.read_holding_registers(31, 1, unit=VEBUS_UNIT)   # VE.Bus state
    assert(not rr.isError())     # test that we are not an error
    decoder = BinaryPayloadDecoder.fromRegisters(rr.registers, byteorder=Endian.Big, wordorder=Endian.Big)
    veBusState = decoder.decode_16bit_uint()
    # get battery voltage
    rr = vic_client.read_holding_registers(26, 1, unit=VEBUS_UNIT)   # battery voltage    assert(not rr.isError())     # test that we are not an error
    decoder = BinaryPayloadDecoder.fromRegisters(rr.registers, byteorder=Endian.Big, wordorder=Endian.Big)
    batteryVoltage = float(decoder.decode_16bit_uint() / 100.0)


 
    # publish info to mqtt broker
    rr = mqttClient.publish('HM-Control/Info/SOC', soc, 2)
    assert(rr.rc == MQTT_ERR_SUCCESS)    # test that we are not an error
    log.debug('+++ mqtt response: ' +  str(rr.rc))
    mqttClient.publish('HM-Control/Info/gridpower', gridTotal)
    mqttClient.publish('HM-Control/Info/acOutpower', acOutTotal)
    mqttClient.publish('HM-Control/Info/PVpowerL1', acPVL1)
    mqttClient.publish('HM-Control/Info/PVpowerL2', acPVL2)
    mqttClient.publish('HM-Control/Info/PVpowerL3', acPVL3)
    mqttClient.publish('HM-Control/Info/PVpower', acPVTotal)
    mqttClient.publish('HM-Control/Info/chargePower', charge)
    mqttClient.publish('HM-Control/Info/VEBusState', veBusState)
    mqttClient.publish('HM-Control/Info/isControlling', isControlling)
    mqttClient.publish('HM-Control/Info/setPercentage', setPercentage)

    log.info("++ SOC: " + str(soc))
    log.info("++ Grid power L1: " + str(gridL1))
    log.info("++ Grid power L2: " + str(gridL2))
    log.info("++ Grid power L3: " + str(gridL3))
    log.info("++ Grid power total: " + str(gridTotal))
    log.info("++ AC out power L1: " + str(acOutL1))
    log.info("++ AC out power L2: " + str(acOutL2))
    log.info("++ AC out power L3: " + str(acOutL3))
    log.info("++ AC out power total: " + str(acOutTotal))
    log.info("++ PV power L1: " + str(acPVL1))
    log.info("++ PV power L2: " + str(acPVL2))
    log.info("++ PV power L3: " + str(acPVL3))
    log.info("++ PV power total: " + str(acPVTotal))
    log.info("++ Battery voltage: " + str(batteryVoltage))
    log.info("++ Charge power: " + str(charge))
    log.info("++ Controller setpoint: " + str(setPercentage))
    log.info("++ Controller status (controlling?): " + str(isControlling))
    


# ---
# SOC is at the top (limit defined in main loop)
# check of grid power and control of PV inverters is necessary
# ---
def autocontrol_pv():
    
    global soc
    global gridTotal
    global acOutTotal
    global acPVTotal
    global charge
    global setPercentage
    global setpoint
    global setpointPrev
    global setDeltas 

    if gridTotal < 0 or gridTotal > 30 or charge < 30:    # grid feedback or too much consumption/discharge
        if charge > 0:                     # we are charging
            chargeSet = 0
        else:
            chargeSet = charge

        shiftDeltas()           # shift the "Delta-Register" one position to the past
        setDeltas[0] = acPVTotal - setpoint    # determine actual delta
        log.info("++ Deltas: " + str(setDeltas))
        corr = getCorr()                   # calculate correction based on past deltas & battery voltage   
        log.info ("++ Correction factor: " + str(corr))

        # setpoint = acOutTotal + chargeSet     # define setpoint of PV-System (+fixed offset if needed?????)
        setpoint = acPVTotal + gridTotal + 20 - chargeSet  # alternate try to define setpoint, 
                                                        # since acOutTotal does not seem to be accurate
        setPercentage = setpoint / 5200 * 100 # setpoint in % relative to max. power
        log.info("++ PV percentage uncorrected: " + str(setPercentage))
        setPercentage = int(setPercentage + corr)
        if setPercentage < 3:
            setPercentage = 2
        if setPercentage > 100:
            setPercentage = 100
        log.info("++ PV power setpoint: " + str(setpoint))
        log.info("++ Write " + str(setPercentage) + "% to 0xc001")
        rq =hm_client.write_register(0xC001, setPercentage, unit=HM_UNIT)
        log.info("++ Response: " + str(rq))

# ---
# calculate correction value for automatic algo based on deltas in 6 past cycles
# and battery voltage level
# ---
def getCorr():

    global setDeltas
    global veBusState
    global batteryVoltage

    v = 0               # value is 0 per default
    sum = 0             # sum of wighted deltas
    voltDelta = 0.0     # voltage delta

    # correction factor based on deltas in 6 past cycles
    for i in range(6):
        sum = sum + setDeltas[i] / 2 ** i

    v = -0.05 * sum

    # additional correction based on battery voltage level
    if veBusState == 4:                 # we are in absorption state
        voltDelta = V_ABSORPTION - batteryVoltage
        c = voltDelta / V_ABSORPTION * 100
    elif veBusState == 5:               # we are in float state
        voltDelta = V_FLOAT - batteryVoltage
        c = voltDelta / V_FLOAT * 100
    else: 
        c = 0.0

    if c < 0.0:             # limit c to positive values
        c = 0.0

    log.info('++++ Voltage Delta: {:1.2f}'.format(voltDelta))
    log.info('++++ Voltage correction factor: {:1.2f}'.format(c))

    v = v + c               # add voltage correction    

    # limit amount of correction
    if v > 5.0:
        v = 5.0
    if v < -5.0:
        v = -5.0

    return v



# ---
# shift the deltas 1 position to the past
# ---
def shiftDeltas():

    global setDeltas

    setDeltas[5] = setDeltas[4]
    setDeltas[4] = setDeltas[3]
    setDeltas[3] = setDeltas[2]
    setDeltas[2] = setDeltas[1]
    setDeltas[1] = setDeltas[0]
    setDeltas[0] = 0


# ---
# manual control of pv power
# ---
def manualcontrol_pv():

    global powerSetpoint
    global setPercentage

    setPercentage = powerSetpoint
    log.info("++ Manual control; write " + str(setPercentage) + "% to 0xc001")
    rq =hm_client.write_register(0xC001, setPercentage, unit=HM_UNIT)
    log.info("++ Response: " + str(rq))

# ---
# reset PV setpoint to 100%
# ---
def fullpower_pv():
    global setPercentage

    log.info("++ Write 100% to 0xc001")
    setPercentage = 100
    rq =hm_client.write_register(0xC001, setPercentage, unit=HM_UNIT)
    log.info("++ Response: " + str(rq))


# ---
# startup sequence to reset everything to "normal" if script was interrupted in an undefined state
# ---
def startupSequence():
    global setPercentage

    log.info("Startup Sequence: --> 100% PV output")
    setPercentage = 100
    rq =hm_client.write_register(0xC001, setPercentage, unit=HM_UNIT)
    log.info("++ Response: " + str(rq))
    

# ---
# main part of the script
# ---
if __name__ == "__main__":

    interval = 60.0
    lastrun = time.time() - 55
    nightTime = time.strptime('22:00', '%H:%M')
    morningTime = time.strptime('04:30', '%H:%M')
    
    vic_client = ModbusClient('192.168.178.50', port=502)
    vic_client.connect()
    hm_client = ModbusClient('192.168.178.92', port=502)
    hm_client.connect()


    while True:
        try:            
            log.info("Startup; wait 10s to initialize communication")
            time.sleep(10)      # wait 20s to give modbus connection time to initiates
            startupSequence()   # make shure after 1st start everything is in order

            while True:
                actualrun = time.time()
                # log.info('actualtime: ' + str(nowTime))
                # log.info('nigttime: ' + str(nightTime))
                # log.info('morningtime: ' + str(morningTime))
                if actualrun - lastrun > interval:
                    nowTime = time.strptime(time.strftime('%H:%M',time.localtime()),'%H:%M')
                    if nightTime > nowTime and morningTime < nowTime:
                        interval = 60.0         # set normal timeinterval
                        log.info("get input variables from Victron")
                        get_inputs()
                        isControlling = False
                        # log.debug("-->SOC: " + str(soc))
                        if autoControl == True:
                            if soc > 98 or veBusState == 4 or veBusState == 5: # SOC high or Absorbtion or Float
                                log.info("SOC is " + str(soc) + "! Control loop started...")
                                isControlling = True
                                autocontrol_pv()
                                interval = 20.0     # shorten timeinterval to 20s during control action
                            elif setPercentage < 100: 
                                log.info("Resetting PV power to 100%")
                                fullpower_pv()      # reset PV to full power
                        else:
                            manualcontrol_pv()
                            interval = 20.0         # shorten timeinterval during manual control phase
                        lastrun = actualrun
                    else:
                        time.sleep(600)
                else:
                    time.sleep(1)

        except:
            log.error('exeption raised waiting for 2 minutes before retrying')
            log.exception(sys.exc_info())
            # raise
            time.sleep(120)
            mqttClient.reconnect()

        finally:
            log.info("finished")

            # close the clients
            vic_client.close()
            mqttClient.loop_stop()
            mqttClient.disconnect()
