#include <Ethernet.h>
#include <SPI.h>
#include <ArduinoJson.h>

byte mac[] = {0xFF, 0x53, 0x3A, 0xD2, 0xFF, 0x65};
IPAddress ip(192, 168, 1, 18);  // Coloca la dirección IP deseada

int ledPin = 13;  // Pin del LED

EthernetClient client;

void setup() {
  Serial.begin(9600);  // Configurar la velocidad del puerto serie
  pinMode(ledPin, OUTPUT);  // Configurar el pin del LED como salida
  Ethernet.begin(mac, ip);
  Serial.println("Conexión Ethernet establecida");
}

void loop() {
  // Realizar una solicitud GET a Firebase
  if (client.connect("energysaver-b3f73.firebaseio.com", 80)) {
    Serial.println("Conexión a Firebase establecida");
    client.println("GET /ledState.json HTTP/1.1");
    client.println("Host: energysaver-b3f73.firebaseio.com");
    client.println("Connection: close");
    client.println();
  } else {
    Serial.println("Error de conexión a Firebase");
    return;
  }

  // Esperar hasta que haya datos disponibles
  while (!client.available()) {
    delay(1);
  }

  // Leer la respuesta JSON
  const size_t bufferSize = JSON_OBJECT_SIZE(1) + 20;
  DynamicJsonDocument jsonBuffer(bufferSize);

  // Parsear la respuesta JSON
  DeserializationError error = deserializeJson(jsonBuffer, client);

  if (error) {
    Serial.println("Error al analizar JSON");
    return;
  }

  // Obtener el valor del nodo "Ampolleta"
  bool ledState = jsonBuffer["Dispositivo"]["Ampolleta"];

  // Imprimir el estado actual del LED
  Serial.print("Estado actual del LED: ");
  Serial.println(ledState ? "Encendido" : "Apagado");

  // Apagar o encender el LED según el estado en Firebase
  digitalWrite(ledPin, ledState ? HIGH : LOW);

  // Esperar antes de la siguiente lectura
  delay(1000);
}
