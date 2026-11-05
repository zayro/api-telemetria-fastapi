# Protocolo de Gestión y Respuesta ante Incidentes de Seguridad

## 1. Objetivo y Alcance
Este protocolo establece las acciones de respuesta obligatorias ante eventos de seguridad, sospechas de intrusión, exfiltración de información y vulneración de credenciales en la infraestructura de microservicios y bases de datos.

## 2. Clasificación de Severidad
- **P1 - Crítico**: Compromiso confirmado de bases de datos productivas, claves maestras o tokens de administración expuestos públicamente. Tiempo de respuesta máximo: 15 minutos.
- **P2 - Alto**: Falla de autenticación masiva, sospecha de evasión de políticas JWT o denegación de servicio distribuida (DDoS). Tiempo de respuesta máximo: 30 minutos.
- **P3 - Medio**: Anomalías en logs de observabilidad o intentos fallidos de inyección de código sin éxito aparente. Tiempo de respuesta: 2 horas.
- **P4 - Menor**: Advertencias de linters de seguridad o certificados próximos a expirar sin afectación operativa. Tiempo de respuesta: 24 horas.

## 3. Procedimiento de Contención Inmediata
1. **Aislamiento**: Aislar el pod o contenedor comprometido desconectándolo de la red de observabilidad (`app-observability-net`) mediante políticas de red.
2. **Revocación de Credenciales**: Revocar inmediatamente los tokens OAuth 2.0 y sesiones activas en Redis asociadas al usuario o servicio vulnerado.
3. **Rotación de Secretos**: Generar y aplicar de inmediato nuevas credenciales de conexión para PostgreSQL, Redis y RabbitMQ en el gestor de secretos.
4. **Preservación Forense**: Extraer volcados de memoria y asegurar los logs de OpenTelemetry en almacenamiento inmutable antes de reiniciar cualquier contenedor.

## 4. Erradicación y Recuperación
- Desplegar una versión limpia de la imagen Docker validada con firma digital.
- Ejecutar análisis estático y pruebas de regresión.
- Monitorear métricas de error 5xx y latencia durante las siguientes 72 horas.

## 5. Cierre y Lecciones Aprendidas
El equipo de respuesta debe redactar un informe post-mortem dentro de las 48 horas posteriores al incidente, documentando la causa raíz, el impacto y las medidas preventivas adoptadas.

