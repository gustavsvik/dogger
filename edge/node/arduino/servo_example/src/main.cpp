#include <Arduino.h>

/* Börvärdet växlar mellan två lägen ( +-SET_ANGLE_SPAN ) med tidsintervall SET_OPERATE_PERIOD */

float SET_ANGLE = 0.0;
float SET_ANGLE_SPAN = 90.0;
float SET_ANGULAR_SPEED = 0.0;
float SET_OPERATE_PERIOD = 5.0;
float SET_P_VALUE = 1.0;


/* Ett fast läge ( SET_ANGLE ) som börvärde */
/*
float SET_ANGLE = 0.0;
float SET_ANGLE_SPAN = 0.0;
float SET_ANGULAR_SPEED = 0.0;
float SET_OPERATE_PERIOD = 0.0;
float SET_P_VALUE = 10.0;
*/

int PERIOD = 50;
float HYSTERESIS = 0.0;
float MAX_ANGLE = 180.0;

float delta = 0.0;
float timeCounter = 0.0;

void setup() 
{
  Serial.begin(115200);
  pinMode(6, OUTPUT);
  pinMode(7, OUTPUT);
  if (SET_ANGULAR_SPEED>0.05) delta = SET_ANGULAR_SPEED/(1000/PERIOD);
  if (SET_OPERATE_PERIOD>0.05) SET_ANGLE = SET_ANGLE_SPAN;
}

void loop()
{
  if (SET_ANGLE_SPAN > 0.05) SET_ANGLE += delta;
  int potVoltage = analogRead(1) - analogRead(0);
  Serial.println(SET_ANGLE);
  float potAngle = (512.0-(float)potVoltage)/512.0 * MAX_ANGLE;
  Serial.println(potAngle);
  if (potAngle<SET_ANGLE+HYSTERESIS)
  {
    digitalWrite(7, HIGH);
  }
  else if (potAngle>SET_ANGLE-HYSTERESIS)
  {
    digitalWrite(7, LOW);
  }
  int errorSignal = (int)(SET_P_VALUE*PERIOD*abs(potAngle-SET_ANGLE)/MAX_ANGLE);
  if (SET_P_VALUE < 0.05) errorSignal = PERIOD;
  if (errorSignal > PERIOD) errorSignal = PERIOD;
  digitalWrite(6, HIGH);
  delay(errorSignal);
  digitalWrite(6, LOW);
  delay(PERIOD-errorSignal);

  if (SET_ANGULAR_SPEED>0.05)
  {
    if (SET_ANGLE>SET_ANGLE_SPAN || SET_ANGLE<-SET_ANGLE_SPAN) delta = -delta;
  }
  else if (SET_OPERATE_PERIOD > 0.05)
  {
    timeCounter += ((float)PERIOD)/1000;
    if (timeCounter >= SET_OPERATE_PERIOD)
    {
      timeCounter = 0.0;
      SET_ANGLE = -SET_ANGLE;
    }
  }
}