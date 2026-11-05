# Protocolo de Seguridad de Redes, Cifrado TLS y Certificados Digitales

## 1. Requisitos de Transporte Seguro
- **TLS 1.3**: Obligatorio en todos los puntos de entrada externos expuestos a través de Traefik.
- **TLS 1.2**: Permitido únicamente para clientes heredados autorizados utilizando cipher suites robustas (ej. `ECDHE-ECDSA-AES256-GCM-SHA384`, `ECDHE-RSA-AES256-GCM-SHA384`). Se prohíbe terminantemente el uso de TLS 1.0, TLS 1.1 y SSLv3.

## 2. Autenticación Mutua (mTLS) Este-Oeste
- La comunicación interna entre microservicios (por ejemplo, llamadas gRPC o HTTP internas entre `test-fastapi`, `test-fastify` y la base de datos) debe usar certificados x509 mutuos expedidos por la Autoridad Certificadora (CA) interna.

## 3. Renovación y Rotación de Certificados
- Certificados públicos gestionados mediante protocolo ACME (Let's Encrypt o proveedor institucional) con renovación programada cada 60 días.
- Configuración de alertas en OpenTelemetry cuando el certificado tenga menos de 20 días de vigencia remanente.

## 4. Políticas de Red y Aislamiento de Puertos
- Puerto 80: Exclusivo para redirección permanente HTTP 301 hacia HTTPS en el puerto 443.
- Las redes Docker internas (`app-observability-net`) no deben exponer puertos directos al exterior; todo acceso se canaliza a través del reverse proxy Traefik con middleware de rate limiting e in-flight requests.

