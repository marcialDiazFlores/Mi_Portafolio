#include <Servo.h>

int pos = 0;

int pin_sensor_pir = 2;
int pin_servo = 8;
int pin_led = 13;

Servo miServo;

void vueltasServo(int vueltas) {
  
  for (int vuelta = 0; vuelta < vueltas; vuelta++){
    
    for (pos = 0; pos <= 180; pos += 1) {
      miServo.write(pos);
      delay(10);
    }

    for (pos = 180; pos >= 0; pos -= 1) {
      miServo.write(pos);
      delay(10);
    }
  
  }
}

void setup() {
  pinMode(pin_sensor_pir, INPUT);
  pinMode(pin_led, OUTPUT);
  miServo.attach(pin_servo);
  miServo.write(0);
}

void loop() {
  
  if (digitalRead(pin_sensor_pir) == HIGH) {
    digitalWrite(pin_led, HIGH);
    vueltasServo(3);
  }
  
  else {
    digitalWrite(pin_led, LOW);    
  }
}