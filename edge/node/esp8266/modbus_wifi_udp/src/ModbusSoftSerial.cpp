/*
#include <Arduino.h>
#include <SoftwareSerial.h>
*/

#include "ModbusRTU.h"
#include "SoftwareSerial.h"

#include "ModbusSoftSerial.h"


ModbusSoftSerial::ModbusSoftSerial(int a_slave_id) : m_slave_id(a_slave_id)
{
}

int ModbusSoftSerial::slaveId() { return this->m_slave_id; }


bool errorCallback(Modbus::ResultCode event, uint16_t transaction_id, void* data) 
{ // Callback to monitor errors
  if (event != Modbus::EX_SUCCESS) 
  {
    //Serial.print("Request result: 0x");
    //Serial.print(event, HEX);
  }
  return true;
}
/*
#define SLAVE_ID 1
#define FIRST_REG 23296 // 0x5B00 ABB A44 instantaneous values starting address
#define REG_COUNT 28

bool coils[20];

char valueBuffer[10];
char byteBuffer[REG_COUNT * 6 + 14 + 1];

SoftwareSerial S(D2, D1);

ModbusRTU mb;

  S.begin(9600, SWSERIAL_8N1);
  mb.begin(&S);
  mb.master();

  uint16_t res[REG_COUNT];

  if (!mb.slave()) 
  {    // Check if no transaction in progress
    mb.readHreg(SLAVE_ID, FIRST_REG, res, REG_COUNT, cb); // Send Read Hreg from Modbus Server
    while(mb.slave()) 
    { // Check if transaction is active
      mb.task();
      delay(10);
    }
  }

  char separator = ',';
  char preamble[13] = {'"','S','L','A','V','E','-','0','0','1','"',':','['};
  memcpy(byteBuffer, &preamble, 13);
  int bufferPos = 13;
  for (int i = 0; i < REG_COUNT; i++)
  {
    int noOfPositions = 1;
    if (res[i]>9) noOfPositions = 2;
    if (res[i]>99) noOfPositions = 3;
    if (res[i]>999) noOfPositions = 4;
    if (res[i]>9999) noOfPositions = 5;
    dtostrf(res[i], noOfPositions, 0, valueBuffer);
    valueBuffer[noOfPositions] = separator;
    memcpy(byteBuffer + bufferPos, &valueBuffer, noOfPositions + 1);
    bufferPos += noOfPositions + 1;
  }
  byteBuffer[bufferPos - 1] = ']';
  byteBuffer[bufferPos] = 0x00;
*/