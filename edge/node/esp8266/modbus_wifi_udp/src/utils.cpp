#include <stdlib.h>

#include "constants.h"
#include "utils.h"


int Transform::Check::numDigits(int an_int)
{
    int digits = 0;
    if (an_int <= 0) digits = 1; // remove this line if '-' counts as a digit
    while (an_int) 
    {
        an_int /= 10;
        digits++;
    }
    return digits;
}

bool Transform::CheckIf::isValidFloat(float a_float)
{
  float delta = 0.1;
  if ( abs(a_float - Constants::Value::INVALID_FLOAT_VALUE) < delta ) return false;
  return true;
}