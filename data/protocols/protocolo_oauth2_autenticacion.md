# Protocolo de Autenticación, Autorización y Manejo de Tokens OAuth 2.0 y OIDC

## 1. Principio General
Todos los endpoints protegidos bajo la ruta `/api/test` exigen validación de tokens JWT mediante el middleware de Traefik y propagación de headers contextuales (`x-auth-user`, `x-auth-email`, `x-auth-roles`).

## 2. Tipos de Tokens y Vigencias
- **Access Token (JWT)**:
  - Formato: JSON Web Token firmado con algoritmo asimétrico RS256 o EdDSA.
  - Vigencia máxima: 15 minutos (900 segundos).
  - Claims obligatorios: `iss` (emisor), `sub` (identificador único), `aud` (audiencia), `exp` (expiración), `iat` (emisión), y `roles` (lista de permisos).
- **Refresh Token**:
  - Almacenamiento: Seguro y cifrado en base de datos.
  - Esquema de Rotación: Rotación estricta de un solo uso (*single-use refresh token rotation*). Cada uso genera un nuevo par de tokens e invalida el anterior.
  - Vigencia máxima absoluta: 7 días calendario.

## 3. Revocación de Tokens (RFC 7009)
- En caso de cierre de sesión explícito o sospecha de compromiso, el cliente debe invocar el endpoint de revocación `/oauth/revoke`.
- El token revocado se almacena en la lista de bloqueo distribuida en Redis con un TTL igual al tiempo remanente de su expiración original.
- Las solicitudes posteriores que presenten dicho token serán rechazadas con código HTTP 401 Unauthorized.

## 4. Roles y Control de Acceso Basado en Roles (RBAC)
Los roles permitidos en el header `x-auth-roles` incluyen:
- `admin`: Permiso total de lectura, escritura y administración de eventos.
- `operator`: Permiso para publicar eventos y consultar métricas.
- `viewer`: Solo lectura de ítems y eventos.

