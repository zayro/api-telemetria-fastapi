# Protocolo de Despliegue Continuo (CI/CD), Versionado Semántico y Rollback

## 1. Versionado y Etiquetado
- Toda versión liberada a producción debe seguir el estándar de Semantic Versioning (vMAJOR.MINOR.PATCH).
- Las imágenes Docker deben etiquetarse con el hash de commit Git y el número de versión formal (evitar uso de `:latest` en ambientes productivos).

## 2. Puertas de Calidad en el Pipeline (Quality Gates)
Antes de construir la imagen final:
1. **Linter y Formato**: Verificación estricta de estilo de código sin errores.
2. **Pruebas Unitarias**: Cobertura mínima de pruebas unitarias sobre casos de uso sin fallos permitidos.
3. **Escaneo de Vulnerabilidades**: SAST y escaneo de dependencias en `requirements.txt`.

## 3. Estrategia de Despliegue Canary Progresivo
1. **Fase 1 (10%)**: Se enruta el 10% del tráfico al nuevo contenedor durante 10 minutos. Se monitorean activamente las sondas `/health/live` y `/health/ready`.
2. **Fase 2 (50%)**: Si la tasa de error 5xx es 0% y la latencia se mantiene estable, se incrementa al 50%.
3. **Fase 3 (100%)**: Finalización de la migración y drenado de conexiones de la versión previa.

## 4. Política de Rollback Automático
Se ejecutará una reversión inmediata e incondicional a la versión previa si se presenta cualquiera de las siguientes condiciones en los primeros 10 minutos:
- Tasa de respuestas HTTP 5xx superior al 1% del volumen total.
- Latencia percentil 95 (p95) superior a 2000 milisegundos.
- Falla consecutiva de 3 chequeos de salud `/health/ready`.

