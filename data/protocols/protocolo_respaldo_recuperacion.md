# Protocolo de Respaldo, Continuidad de Negocio y Disaster Recovery

## 1. Métricas de Continuidad
- **RPO (Recovery Point Objective)**: Menor o igual a 1 hora. Representa la cantidad máxima admisible de datos perdidos ante un fallo imprevisto.
- **RTO (Recovery Time Objective)**: Menor o igual a 4 horas. Representa el lapso máximo aceptable para restablecer los servicios y la operatividad total.

## 2. Estrategia de Copias de Seguridad de Base de Datos
- **PostgreSQL**:
  - Activación obligatoria de WAL (Write-Ahead Logging) archiving continuo hacia almacenamiento de objetos redundante (MinIO / S3).
  - Snapshot completo diario a las 02:00 UTC con retención de 30 días para respaldos diarios y 12 meses para respaldos mensuales.
  - Cifrado en reposo mediante AES-256.
- **Redis**:
  - Persistencia configurada mediante combinación de RDB diario y append-only file (AOF) cada segundo para evitar pérdida de datos en caché crítica.

## 3. Pruebas Periódicas de Restauración
- Cada trimestre se realiza un simulacro de recuperación en frío en un ambiente aislado (Staging/DR).
- La prueba concluye exitosamente cuando se validan la integridad referencial de las tablas `test_items` y `test_events` y los tiempos de recuperación se mantienen dentro del RTO convenido.

## 4. Procedimiento de Conmutación por Falla (Failover)
En caso de falla total en el centro de datos o zona de disponibilidad primaria:
1. Declarar formalmente el estado de desastre por el líder de infraestructura.
2. Promover la réplica de lectura de base de datos a maestro en la región secundaria.
3. Actualizar los registros DNS y certificados TLS para redirigir el tráfico hacia el clúster secundario.
4. Validar el estado mediante las sondas `/health/ready` de los microservicios.

