#include <string>

#include "Arduino.h"

#include "ESP8266WiFi.h"
#include "ESP8266WiFiMulti.h"   // Include the Wi-Fi-Multi library
#include "WiFiUdp.h"
//#include "ArduinoOTA.h"
#include "NTPClient.h"
#include "DHT.h"
#include "ModbusRTU.h"
#include "SoftwareSerial.h"

#include "utils.h"
#include "constants.h"
#include "secrets.h"
#include "devices.h"

#include "LogSerial.h"
#include "ModbusSoftSerial.h"

/*
ModbusSerialBus modbus_soft_serial = ModbusSerialBus(Bus::SerialBus::Modbus::SLAVE_ID);
*/


//bool connectioWasAlive = true;
uint32_t cyclesSinceNTP = Constants::Time::CYCLES_BETWEEN_NTP;

bool coils[20];

char channelBuffer[2] = {0xFF, 0xFF};
char timestampBuffer[4] = {0xFF, 0xFF, 0xFF, 0xFF};
char channelTimestampBuffer[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
char valueBuffer[10];
char dataValueBuffer[2 * Constants::Dimension::MARIEX_BUFFERSIZE]; //= {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
uint16_t modbusReadResultIndex = 0;
char jsonPreamble[13];
char byteBuffer[Bus::SerialBus::Modbus::REG_COUNT * 6 + 14 + 1];
uint16_t byteBufferPos = 0;
char dataByteBuffer[Bus::SerialBus::Modbus::REG_COUNT * 6 + 14 + 2 + 4 + 1];

uint32_t current_timestamp = Constants::Time::INVALID_TIMESTAMP;

uint16_t channel = Constants::Dimension::MIN_CHANNEL_INDEX;
uint32_t timestamp = Constants::Time::INVALID_TIMESTAMP;
float value = Constants::Value::INVALID_FLOAT_VALUE;
// TODO: Consider solutions for armoring channels array elements. Presently must be >255, to avoid null bytes leading to datagrams being truncated in WiFiUDP.write()
uint16_t channelOffsets[3] = {0, 0, 0}; 
// Set undefined timestamps to 2**32-1 rather than 0, for the same reason as above
uint32_t timestamps[3] = {Constants::Time::INVALID_TIMESTAMP, Constants::Time::INVALID_TIMESTAMP, Constants::Time::INVALID_TIMESTAMP}; 
float values[3] = {Constants::Value::INVALID_FLOAT_VALUE, Constants::Value::INVALID_FLOAT_VALUE, Constants::Value::INVALID_FLOAT_VALUE};

Bus::SerialBus::Hard::Log logger(115200, 2);

SoftwareSerial serial(D2, D1);

ModbusRTU modbus;
uint16_t modbusReadResult[2][Bus::SerialBus::Modbus::MAX_REG_COUNT];

ESP8266WiFiMulti wifiMulti;

WiFiUDP Udp;

DHT dht(D4, DHT22);

NTPClient timeClient(Udp, "pool.ntp.org");


void updateTime()
{
  cyclesSinceNTP++;
  if (cyclesSinceNTP > Constants::Time::CYCLES_BETWEEN_NTP)
  {
    timeClient.update();
    current_timestamp = timeClient.getEpochTime();
    cyclesSinceNTP = 0;
  }

}

void setupTime()
{
  timeClient.begin();
  timeClient.setTimeOffset(0);
  timeClient.update();
  current_timestamp = timeClient.getEpochTime();
}

void monitorWiFi()
{    
  logger.writeln("monitorWiFi entry");
  // Maintain WiFi connection
  if (wifiMulti.run(5000) == WL_CONNECTED) 
  {
    logger.write("WiFi connected: "); logger.writeln(WiFi.SSID());
    //logger.write(" ");
    //logger.writeln(WiFi.localIP());
  } 
  else 
  {
    logger.writeln("WiFi not connected!");
  }
  logger.writeln("monitorWiFi exit");
}

void setupWiFi()
{
  // Don't save WiFi configuration in flash - optional
  WiFi.persistent(false);
  // Set WiFi to station mode
  WiFi.mode(WIFI_STA);
  // Register multi WiFi networks
  logger.write("secrets::wifiNetworksCount: "); logger.writeln(secrets::wifiNetworksCount);
  for (int i = 0; i < secrets::wifiNetworksCount; i++) wifiMulti.addAP(secrets::wifiNetworks[i].ssid() , secrets::wifiNetworks[i].password());

  monitorWiFi();
}

void prepareData()
{
  channelOffsets[0] = 55;
  channelOffsets[1] = 56;
  channelOffsets[2] = 57;

  for (int i = 0; i < 3; i++) timestamps[i] = current_timestamp;

  values[0] = dht.readTemperature();
  values[1] = dht.readHumidity();
  values[2] = Constants::Value::INVALID_FLOAT_VALUE;
}

bool modbusReadCallback(Modbus::ResultCode event, uint16_t transactionId, void* data) 
{ // Callback to monitor errors
  if (event != Modbus::EX_SUCCESS) 
  {
    logger.write("Request result: 0x");
    logger.writeln_hex(event);
  }
  return true;
}

/*
bool modbusWriteCallback(Modbus::ResultCode event, uint16_t transactionId, void* data) 
{
  bool result;
  uint8_t code ;
  //Serial.printf_P(" 0x%02X ", event);
  //if (event == 0x00) {
  code = event;
  if (code != Modbus::EX_SUCCESS) 
  {
    logger.write("Request result: 0x"); logger.writeln_hex(code);
  }
  result = true;
  return result;
}
*/

void prepareModbusJsonPreamble()
{
  char preambleStart[7] = {'"','S','L','A','V','E','-'};
  char preambleSlaveID[3];
  char preambleEnd[3] = {'"',':','['};
  memcpy(jsonPreamble, &preambleStart, 7);
  dtostrf(Bus::SerialBus::Modbus::SLAVE_ID, 3, 0, preambleSlaveID);
  for (int i = 0; i < 3; i++) if (preambleSlaveID[i] == ' ') preambleSlaveID[i] = '0';
  memcpy(jsonPreamble + 7, &preambleSlaveID, 3);
  memcpy(jsonPreamble + 7 + 3, &preambleEnd, 3);
}

void prepareModbusJsonBuffer()
{
  byteBufferPos = 13;
  char separator = ',';
  for (int i = 0; i < Bus::SerialBus::Modbus::REG_COUNT; i++)
  {
    int noOfPositions = Transform::Check::numDigits(modbusReadResult[modbusReadResultIndex][i]);
    dtostrf(modbusReadResult[modbusReadResultIndex][i], noOfPositions, 0, valueBuffer);
    valueBuffer[noOfPositions] = separator;
    memcpy(byteBuffer + byteBufferPos, &valueBuffer, noOfPositions + 1);
    byteBufferPos += noOfPositions + 1;
  }
  byteBuffer[byteBufferPos - 1] = ']';
  byteBuffer[byteBufferPos] = 0x00;
}


void setup()
{
  // PID regulator Red Lion PXU defaults
  //serial.begin(Bus::SerialBus::BAUD_RATE, SWSERIAL_8E1);
  // ABB A44 energy meter
  serial.begin(Bus::SerialBus::Modbus::BAUD_RATE, SWSERIAL_8N1);

  modbus.begin(&serial);
  modbus.master();

  delay(2000);
  dht.begin();
  delay(2000);

  setupWiFi();
  monitorWiFi();

  setupTime();

  pinMode(LED_BUILTIN, OUTPUT);
}

void loop()
{
  /*
  int slave_id = modbus_soft_serial.slaveId();
  logger.write("slave_id: "); logger.writeln(slave_id);
  */
  //Serial.print("cyclesSinceNTP: "); Serial.println(cyclesSinceNTP);

  monitorWiFi();

  for (int i = 0; i < Bus::SerialBus::Modbus::REG_COUNT; i++) modbusReadResult[modbusReadResultIndex][i] = 0;

  if (!modbus.slave()) 
  {    // Check if no transaction in progress
    logger.writeln("Before readHreg");
    modbusReadResultIndex = 0; //cyclesSinceNTP % 2 ;
    if (modbusReadResultIndex) modbus.readHreg(Bus::SerialBus::Modbus::SLAVE_ID, 20480, modbusReadResult[modbusReadResultIndex], 12, modbusReadCallback); // Send Read Hreg from Modbus Server
    else modbus.readHreg(Bus::SerialBus::Modbus::SLAVE_ID, (uint16_t)Bus::SerialBus::Modbus::FIRST_REG, modbusReadResult[modbusReadResultIndex], Bus::SerialBus::Modbus::REG_COUNT, modbusReadCallback); // Send Read Hreg from Modbus Server
    while(modbus.slave()) 
    { // Check if transaction is active
      modbus.task();
      delay(10);
    }
  }
/*
  uint16_t noOfWriteRegisters = (uint16_t)Bus::SerialBus::Modbus::WRITE_REG_COUNT;
  uint16_t writeRegisters[noOfWriteRegisters];
  uint16_t writeValues[noOfWriteRegisters];
  if (!modbus.slave()) 
  {
    writeRegisters[0] = (uint16_t)10;
    writeRegisters[1] = (uint16_t)5;
    writeValues[0] = (uint16_t)200;
    writeValues[1] = (uint16_t)1001;
    noOfWriteRegisters = (uint16_t)1;
    logger.writeln("Before writeHreg");
    int writeResult = modbus.writeHreg(Bus::SerialBus::Modbus::SLAVE_ID, writeRegisters[0], writeValues, noOfWriteRegisters, modbusWriteCallback);
    logger.write("writeResult: ");
    logger.writeln(writeResult);
    while(modbus.slave()) 
    {
      modbus.task();
      delay(10);
    }
  }
*/

  updateTime();

  prepareData();

  for (int i = 0; i < 3; i++)
  {
    channel = Constants::Dimension::MIN_CHANNEL_INDEX + channelOffsets[i];
    timestamp = timestamps[i];
    value = values[i];

    memcpy(channelBuffer, &channel, 2);
    memcpy(channelTimestampBuffer, &channelBuffer, 2);

    if (timestamp <= Constants::Time::INSTALL_TIMESTAMP || timestamp == 0) timestamp = Constants::Time::INVALID_TIMESTAMP;
    memcpy(timestampBuffer, &timestamp, 4);
    memcpy(channelTimestampBuffer + 2, &timestampBuffer, 4);

    if (Transform::CheckIf::isValidFloat(value))
    {
      memcpy(dataValueBuffer, &channelTimestampBuffer, 2 + 4);

      dtostrf(value, 9, 2, valueBuffer);

      logger.write("valueBuffer: "); logger.writeln(valueBuffer);
      memcpy(dataValueBuffer + 2 + 4, &valueBuffer, 10);

      dataValueBuffer[Constants::Dimension::MARIEX_BUFFERSIZE] = 0x00; // Null terminate
      logger.write("dataValueBuffer: "); logger.writeln(dataValueBuffer);
      memcpy(dataByteBuffer, &dataValueBuffer, Constants::Dimension::MARIEX_BUFFERSIZE + 1);
    }
    else
    {
      memcpy(dataByteBuffer, &channelTimestampBuffer, 2 + 4);

      logger.write("modbusReadResult["); logger.write(modbusReadResultIndex); logger.write("]: ");
      for (int i = 0; i < Bus::SerialBus::Modbus::REG_COUNT; i++) { logger.write(modbusReadResult[modbusReadResultIndex][i]); logger.write(' '); }
      logger.linefeed();

      prepareModbusJsonPreamble();

      memcpy(byteBuffer, &jsonPreamble, 13);

      prepareModbusJsonBuffer();

      logger.write("byteBuffer: "); logger.writeln(byteBuffer); 

      memcpy(dataByteBuffer + 2 + 4, &byteBuffer, byteBufferPos + 2 + 4);
    }

    logger.write("dataByteBuffer: "); logger.writeln(dataByteBuffer); 

    digitalWrite(LED_BUILTIN, LOW);

    Udp.beginPacket(secrets::ip_1, secrets::channelPorts[i]);
    Udp.write(dataByteBuffer); //dataValueBuffer);
    Udp.endPacket();
    Udp.beginPacket(secrets::ip_2, secrets::channelPorts[i]);
    Udp.write(dataByteBuffer); //dataValueBuffer);
    Udp.endPacket();
    digitalWrite(LED_BUILTIN, HIGH);

  }

  int delayMs = Constants::Time::CYCLE_TIME * 1000;
  delay(delayMs);
  current_timestamp += Constants::Time::CYCLE_TIME;
  for (int i = 0; i < 3; i++) timestamps[i] = current_timestamp;
}
