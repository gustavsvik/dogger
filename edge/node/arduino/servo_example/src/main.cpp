int SERVO_SWITCH_PIN = 6;
int SERVO_DIRECTION_PIN = 7;

int STEPPER_PIN[4] = {13, 12, 11, 10};

float SERVO_GEAR_RATIO = 1.0f/100.0f;
float MAX_ANGLE = 100.0;

int STEPPER_STEPS = 32;
float STEPPER_GEAR_RATIO = 1.0f/64.0f;

/* Börvärdet växlar mellan två lägen ( +-SET_ANGLE_SPAN ) med tidsintervall SET_OPERATE_PERIOD */

int PERIOD = 5;

float SET_ANGLE = 0.0;
float SET_ANGLE_SPAN = 90.0;
float SET_ANGULAR_SPEED = 0.0;
float SET_OPERATE_PERIOD = 1.0;

float SET_P_VALUE = 10.0;
float HYSTERESIS = 0.0;

int STEPPER_STEPS_PER_CYCLE = 4;

/* Ett fast läge ( SET_ANGLE ) som börvärde */
/*
float SET_ANGLE = 0.0;
float SET_ANGLE_SPAN = 0.0;
float SET_ANGULAR_SPEED = 0.0;
float SET_OPERATE_PERIOD = 0.0;
*/

float delta = 0.0;
float timeCounter = 0.0;
unsigned long int noOfCycles = 0;

unsigned long int potTimestamp = 0;
unsigned long int lastPotTimestamp = 0;
float potAngle = SET_ANGLE;
float lastPotAngle = SET_ANGLE;
unsigned long int lastNoOfCycles = 0;

unsigned long int noOfStepperSteps = 0;
float stepperAngle = 0;


void printSerial(float value, bool separate, int noOfCycles, int cyclesInterval)
{
  if (noOfCycles % cyclesInterval == 0)
  {
    if (separate)
    {
      Serial.print(value);
      Serial.print(",");
    }
    else
    {
      Serial.println(value);
    }
  }
}

int stepperSequenceValue(int pole, long int step, bool reverse)
{
  //int stepperSwitchingSequence[4] = {0,0,0,0};
  int sequenceIndex = step % 4;
  if (reverse) sequenceIndex = 4-1 - sequenceIndex;
  if (sequenceIndex == pole) return 1;
  return 0;
}


void setup() 
{
  Serial.begin(115200);

  if (SET_ANGULAR_SPEED > 0.05) delta = SET_ANGULAR_SPEED/(1000/PERIOD);
  if (SET_OPERATE_PERIOD > 0.05) SET_ANGLE = SET_ANGLE_SPAN;

  pinMode(SERVO_SWITCH_PIN, OUTPUT);
  digitalWrite(SERVO_SWITCH_PIN, LOW);
  pinMode(SERVO_DIRECTION_PIN, OUTPUT);
  digitalWrite(SERVO_DIRECTION_PIN, LOW);

  for (int i = 0; i < 4; i++)
  {
    pinMode(STEPPER_PIN[i], OUTPUT);
    digitalWrite(STEPPER_PIN[i], LOW);
  }
}


void loop()
{
  noOfCycles++;

  if (SET_ANGLE_SPAN > 0.05) SET_ANGLE += delta;
  printSerial(SET_ANGLE, true, noOfCycles, 10);

  lastPotAngle = potAngle;
  lastPotTimestamp = potTimestamp;

  int potVoltage = analogRead(1) - analogRead(0);
  potAngle = (512.0-(float)potVoltage)/512.0 * MAX_ANGLE;

  potTimestamp = millis();
  printSerial(potAngle, true, noOfCycles, 10);

  if (potAngle < SET_ANGLE+HYSTERESIS)
  {
    digitalWrite(SERVO_DIRECTION_PIN, HIGH);
  }
  else if (potAngle > SET_ANGLE-HYSTERESIS)
  {
    digitalWrite(SERVO_DIRECTION_PIN, LOW);
  }

  int errorSignal = (int)(SET_P_VALUE * PERIOD * abs(potAngle-SET_ANGLE)/MAX_ANGLE);
  if (SET_P_VALUE < 0.05) errorSignal = PERIOD;
  if (errorSignal > PERIOD) errorSignal = PERIOD;

  float servoMotorSpeed = (float)60/360 * (potAngle-lastPotAngle) / (((float)(potTimestamp-lastPotTimestamp))/1000.0f) / SERVO_GEAR_RATIO;
  printSerial(servoMotorSpeed/100.0f, true, noOfCycles, 10);

  int sign = 0.0;
  if (SET_ANGLE < 0.0 && stepperAngle > SET_ANGLE) sign = -1.0;
  if (SET_ANGLE > 0.0 && stepperAngle < SET_ANGLE) sign = 1.0;
  if (SET_ANGLE < 0.0 && stepperAngle <= SET_ANGLE) sign = 0.0;
  if (SET_ANGLE > 0.0 && stepperAngle >= SET_ANGLE) sign = 0.0;

  stepperAngle += (float)360 / (float)STEPPER_STEPS * STEPPER_GEAR_RATIO * sign ; //SET_ANGLE/abs(SET_ANGLE);
  printSerial(stepperAngle, true, noOfCycles, 10);

  float stepperMotorSpeed = (float)60 * (float)(noOfCycles-lastNoOfCycles) * sign / (float)STEPPER_STEPS / (((float)(potTimestamp-lastPotTimestamp))/1000.0f) ;
  printSerial(stepperMotorSpeed/100.0f, false, noOfCycles, 10);
  lastNoOfCycles = noOfCycles;

  for (int pole = 0; pole < 4; pole++)
  {
    digitalWrite( STEPPER_PIN[pole], stepperSequenceValue(pole, noOfCycles, SET_ANGLE<0.0) );
  }

  digitalWrite(SERVO_SWITCH_PIN, HIGH);
  delay(errorSignal);
  digitalWrite(SERVO_SWITCH_PIN, LOW);
  delay(PERIOD-errorSignal);

  if (SET_ANGULAR_SPEED > 0.05)
  {
    if (SET_ANGLE>SET_ANGLE_SPAN || SET_ANGLE<-SET_ANGLE_SPAN) delta = -delta;
  }
  else if (SET_OPERATE_PERIOD > 0.05)
  {
    timeCounter += ((float)PERIOD) / 1000;
    if (timeCounter >= SET_OPERATE_PERIOD)
    {
      timeCounter = 0.0;
      SET_ANGLE = -SET_ANGLE;
    }
  }
}
